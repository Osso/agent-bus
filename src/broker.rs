use std::collections::{HashMap, HashSet};

use thiserror::Error;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::message::{BusMessage, Frame, Target};
use crate::wire::{self, WireError};

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wire error: {0}")]
    Wire(#[from] WireError),
}

enum Event {
    Register {
        name: String,
        sender: mpsc::Sender<Frame>,
        reply_tx: mpsc::Sender<Result<(), String>>,
    },
    Message {
        from: String,
        message: BusMessage,
    },
    Subscribe {
        name: String,
        topic: String,
    },
    Unsubscribe {
        name: String,
        topic: String,
    },
    Disconnected {
        name: String,
    },
}

pub struct Broker {
    clients: HashMap<String, mpsc::Sender<Frame>>,
    topics: HashMap<String, HashSet<String>>,
}

impl Broker {
    pub fn new() -> Self {
        Self { clients: HashMap::new(), topics: HashMap::new() }
    }

    pub async fn run(mut self, listener: UnixListener) -> Result<(), BrokerError> {
        let (event_tx, mut event_rx) = mpsc::channel::<Event>(256);
        tokio::spawn(accept_loop(listener, event_tx));
        while let Some(event) = event_rx.recv().await {
            self.handle_event(event).await;
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: Event) {
        match event {
            Event::Register { name, sender, reply_tx } => {
                self.handle_register(name, sender, reply_tx).await;
            }
            Event::Message { from, message } => {
                self.handle_message(from, message).await;
            }
            Event::Subscribe { name, topic } => {
                self.topics.entry(topic).or_default().insert(name);
            }
            Event::Unsubscribe { name, topic } => {
                if let Some(subs) = self.topics.get_mut(&topic) {
                    subs.remove(&name);
                }
            }
            Event::Disconnected { name } => {
                self.handle_disconnect(name).await;
            }
        }
    }

    async fn handle_register(
        &mut self,
        name: String,
        sender: mpsc::Sender<Frame>,
        reply_tx: mpsc::Sender<Result<(), String>>,
    ) {
        if self.clients.contains_key(&name) {
            warn!("registration rejected: name '{}' already taken", name);
            let _ = reply_tx.send(Err(format!("name '{}' already taken", name))).await;
            return;
        }
        info!("client registered: {}", name);
        self.clients.insert(name.clone(), sender);
        let _ = reply_tx.send(Ok(())).await;
        self.broadcast_peer_event(Frame::PeerConnected { name: name.clone() }, &name).await;
    }

    async fn handle_message(&mut self, from: String, message: BusMessage) {
        debug!("message from '{}' kind='{}' to={:?}", from, message.kind, message.to);
        match message.to.clone() {
            Target::Named(target) => self.route_named(from, target, message).await,
            Target::Topic(topic) => self.route_topic(from, topic, message).await,
            Target::Broadcast => self.route_broadcast(from, message).await,
        }
    }

    async fn route_named(&mut self, from: String, target: String, message: BusMessage) {
        if let Some(sender) = self.clients.get(&target) {
            let _ = sender.send(Frame::Message(message)).await;
        } else {
            warn!("no client named '{}', dropping message from '{}'", target, from);
            if let Some(sender) = self.clients.get(&from) {
                let _ = sender
                    .send(Frame::Error { message: format!("no client named '{}'", target) })
                    .await;
            }
        }
    }

    async fn route_topic(&mut self, from: String, topic: String, message: BusMessage) {
        let subscribers: Vec<String> = self
            .topics
            .get(&topic)
            .map(|s| s.iter().filter(|n| *n != &from).cloned().collect())
            .unwrap_or_default();
        for name in subscribers {
            if let Some(sender) = self.clients.get(&name) {
                let _ = sender.send(Frame::Message(message.clone())).await;
            }
        }
    }

    async fn route_broadcast(&mut self, from: String, message: BusMessage) {
        let targets: Vec<String> =
            self.clients.keys().filter(|n| *n != &from).cloned().collect();
        for name in targets {
            if let Some(sender) = self.clients.get(&name) {
                let _ = sender.send(Frame::Message(message.clone())).await;
            }
        }
    }

    async fn handle_disconnect(&mut self, name: String) {
        info!("client disconnected: {}", name);
        self.clients.remove(&name);
        for subs in self.topics.values_mut() {
            subs.remove(&name);
        }
        // Client is already removed, so except="" is equivalent; be explicit for clarity.
        self.broadcast_peer_event(Frame::PeerDisconnected { name: name.clone() }, &name).await;
    }

    async fn broadcast_peer_event(&self, frame: Frame, except: &str) {
        for (name, sender) in &self.clients {
            if name != except {
                let _ = sender.send(frame.clone()).await;
            }
        }
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

async fn accept_loop(listener: UnixListener, event_tx: mpsc::Sender<Event>) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                info!("new connection accepted");
                let tx = event_tx.clone();
                tokio::spawn(handle_connection(stream, tx));
            }
            Err(e) => {
                warn!("accept error: {}", e);
            }
        }
    }
}

async fn handle_connection(stream: tokio::net::UnixStream, event_tx: mpsc::Sender<Event>) {
    let (mut read_half, write_half) = stream.into_split();
    let (out_tx, out_rx) = mpsc::channel::<Frame>(64);
    tokio::spawn(write_task(write_half, out_rx));

    let name = match perform_registration(&mut read_half, out_tx.clone(), &event_tx).await {
        Some(n) => n,
        None => return,
    };

    run_read_loop(read_half, out_tx, event_tx, name).await;
}

async fn perform_registration(
    read_half: &mut tokio::net::unix::OwnedReadHalf,
    out_tx: mpsc::Sender<Frame>,
    event_tx: &mpsc::Sender<Event>,
) -> Option<String> {
    let frame = match wire::read_frame(read_half).await {
        Ok(f) => f,
        Err(e) => {
            debug!("read error during registration: {}", e);
            return None;
        }
    };

    let name = extract_register_name(frame, &out_tx).await?;
    send_register_event(name, out_tx, event_tx).await
}

async fn extract_register_name(frame: Frame, out_tx: &mpsc::Sender<Frame>) -> Option<String> {
    match frame {
        Frame::Register { name } => Some(name),
        _ => {
            let _ = out_tx
                .send(Frame::Error { message: "expected Register frame".into() })
                .await;
            None
        }
    }
}

async fn send_register_event(
    name: String,
    out_tx: mpsc::Sender<Frame>,
    event_tx: &mpsc::Sender<Event>,
) -> Option<String> {
    let (reply_tx, mut reply_rx) = mpsc::channel(1);
    let _ = event_tx
        .send(Event::Register { name: name.clone(), sender: out_tx.clone(), reply_tx })
        .await;

    match reply_rx.recv().await {
        Some(Ok(())) => {
            let _ = out_tx.send(Frame::Registered).await;
            Some(name)
        }
        Some(Err(msg)) => {
            let _ = out_tx.send(Frame::Error { message: msg }).await;
            None
        }
        None => None,
    }
}

async fn run_read_loop(
    mut read_half: tokio::net::unix::OwnedReadHalf,
    out_tx: mpsc::Sender<Frame>,
    event_tx: mpsc::Sender<Event>,
    name: String,
) {
    loop {
        let frame = match wire::read_frame(&mut read_half).await {
            Ok(f) => f,
            Err(_) => break,
        };
        if !dispatch_frame(frame, &name, &out_tx, &event_tx).await {
            break;
        }
    }
    let _ = event_tx.send(Event::Disconnected { name }).await;
}

async fn dispatch_frame(
    frame: Frame,
    name: &str,
    out_tx: &mpsc::Sender<Frame>,
    event_tx: &mpsc::Sender<Event>,
) -> bool {
    match frame {
        Frame::Message(msg) => {
            let _ = event_tx
                .send(Event::Message { from: name.to_string(), message: msg })
                .await;
        }
        Frame::Subscribe { topic } => {
            let _ = event_tx
                .send(Event::Subscribe { name: name.to_string(), topic })
                .await;
        }
        Frame::Unsubscribe { topic } => {
            let _ = event_tx
                .send(Event::Unsubscribe { name: name.to_string(), topic })
                .await;
        }
        other => {
            warn!("unexpected frame from '{}': {:?}", name, other);
            let _ = out_tx
                .send(Frame::Error { message: "unexpected frame type".into() })
                .await;
        }
    }
    true
}

async fn write_task(
    mut write_half: tokio::net::unix::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Frame>,
) {
    while let Some(frame) = rx.recv().await {
        if let Err(e) = wire::write_frame(&mut write_half, &frame).await {
            debug!("write error: {}", e);
            break;
        }
    }
}
