use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::message::BusMessage;

/// Error type for bus operations
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("name already registered: {0}")]
    NameTaken(String),
    #[error("unknown recipient: {0}")]
    UnknownRecipient(String),
    #[error("bus is shut down")]
    Shutdown,
}

type Registry = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<BusMessage>>>>;

/// Shared message router. Cheap to clone (Arc internally).
#[derive(Clone)]
pub struct Bus {
    registry: Registry,
}

impl Bus {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Deregister an agent by name, closing its receive channel.
    pub fn deregister(&self, name: &str) {
        let mut reg = self.registry.lock().unwrap();
        reg.remove(name);
    }

    /// Register an agent by name, returning its Mailbox.
    pub fn register(&self, name: &str) -> Result<Mailbox, BusError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut reg = self.registry.lock().unwrap();
        if reg.contains_key(name) {
            return Err(BusError::NameTaken(name.to_string()));
        }
        reg.insert(name.to_string(), tx);
        Ok(Mailbox {
            name: name.to_string(),
            registry: self.registry.clone(),
            rx,
        })
    }
}

impl Default for Bus {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-agent handle for sending and receiving messages.
#[derive(Debug)]
pub struct Mailbox {
    name: String,
    registry: Registry,
    rx: mpsc::UnboundedReceiver<BusMessage>,
}

impl Mailbox {
    /// Send a message to a named recipient.
    pub fn send(&self, to: &str, kind: &str, payload: serde_json::Value) -> Result<Uuid, BusError> {
        let id = Uuid::new_v4();
        let msg = BusMessage {
            id,
            from: self.name.clone(),
            to: to.to_string(),
            kind: kind.to_string(),
            payload,
        };
        let reg = self.registry.lock().unwrap();
        let tx = reg
            .get(to)
            .ok_or_else(|| BusError::UnknownRecipient(to.to_string()))?;
        tx.send(msg).map_err(|_| BusError::Shutdown)?;
        Ok(id)
    }

    /// Receive the next message. Returns None if the bus is dropped.
    pub async fn recv(&mut self) -> Option<BusMessage> {
        self.rx.recv().await
    }

    /// Try to receive a message without blocking.
    pub fn try_recv(&mut self) -> Option<BusMessage> {
        self.rx.try_recv().ok()
    }

    /// Returns this mailbox's registered name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        let mut reg = self.registry.lock().unwrap();
        reg.remove(&self.name);
    }
}
