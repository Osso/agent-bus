use std::path::Path;

use thiserror::Error;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tracing::debug;
use uuid::Uuid;

use crate::message::{BusMessage, Frame, Target};
use crate::wire::{self, WireError};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wire error: {0}")]
    Wire(#[from] WireError),
    #[error("registration failed: {0}")]
    RegistrationFailed(String),
    #[error("disconnected")]
    Disconnected,
}

pub struct BusClient {
    name: String,
    writer: mpsc::Sender<Frame>,
    reader: mpsc::Receiver<Frame>,
}

impl BusClient {
    /// Connect to the bus and register with the given name.
    pub async fn connect(path: impl AsRef<Path>, name: &str) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path.as_ref()).await?;
        let (mut read_half, write_half) = stream.into_split();

        let (write_tx, write_rx) = mpsc::channel::<Frame>(64);
        let (read_tx, read_rx) = mpsc::channel::<Frame>(64);

        write_tx
            .send(Frame::Register { name: name.to_string() })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        tokio::spawn(client_write_task(write_half, write_rx));

        await_registered(&mut read_half).await?;

        tokio::spawn(client_read_task(read_half, read_tx));

        Ok(Self { name: name.to_string(), writer: write_tx, reader: read_rx })
    }

    /// Send a message to a target.
    pub async fn send(
        &self,
        to: Target,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<(), ClientError> {
        let msg = BusMessage {
            id: Uuid::new_v4(),
            from: self.name.clone(),
            to,
            kind: kind.into(),
            payload,
        };
        self.writer
            .send(Frame::Message(msg))
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    /// Receive the next incoming message/event.
    pub async fn recv(&mut self) -> Option<Frame> {
        self.reader.recv().await
    }

    /// Subscribe to a topic.
    pub async fn subscribe(&self, topic: &str) -> Result<(), ClientError> {
        self.writer
            .send(Frame::Subscribe { topic: topic.to_string() })
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    /// Unsubscribe from a topic.
    pub async fn unsubscribe(&self, topic: &str) -> Result<(), ClientError> {
        self.writer
            .send(Frame::Unsubscribe { topic: topic.to_string() })
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    /// Get this client's registered name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

async fn await_registered(
    read_half: &mut tokio::net::unix::OwnedReadHalf,
) -> Result<(), ClientError> {
    match wire::read_frame(read_half).await.map_err(ClientError::Wire)? {
        Frame::Registered => Ok(()),
        Frame::Error { message } => Err(ClientError::RegistrationFailed(message)),
        other => {
            debug!("unexpected frame during registration: {:?}", other);
            Err(ClientError::RegistrationFailed("unexpected frame".into()))
        }
    }
}

async fn client_write_task(
    mut write_half: tokio::net::unix::OwnedWriteHalf,
    mut rx: mpsc::Receiver<Frame>,
) {
    while let Some(frame) = rx.recv().await {
        if let Err(e) = wire::write_frame(&mut write_half, &frame).await {
            debug!("client write error: {}", e);
            break;
        }
    }
}

async fn client_read_task(
    mut read_half: tokio::net::unix::OwnedReadHalf,
    tx: mpsc::Sender<Frame>,
) {
    loop {
        match wire::read_frame(&mut read_half).await {
            Ok(frame) => {
                if tx.send(frame).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                debug!("client read error: {}", e);
                break;
            }
        }
    }
}
