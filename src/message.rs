use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Routing target for a message
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum Target {
    /// Send to a specific named client
    Named(String),
    /// Send to all subscribers of a topic
    Topic(String),
    /// Send to all connected clients (except sender)
    Broadcast,
}

/// Application-level message sent between agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusMessage {
    pub id: Uuid,
    pub from: String,
    pub to: Target,
    pub kind: String,
    pub payload: serde_json::Value,
}

/// Wire protocol frames between client and broker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame")]
pub enum Frame {
    // Client → Broker
    Register { name: String },
    Subscribe { topic: String },
    Unsubscribe { topic: String },

    // Bidirectional
    Message(BusMessage),

    // Broker → Client
    Registered,
    Error { message: String },
    PeerConnected { name: String },
    PeerDisconnected { name: String },
}
