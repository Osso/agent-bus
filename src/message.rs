use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Application-level message sent between agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusMessage {
    pub id: Uuid,
    pub from: String,
    pub to: String,
    pub kind: String,
    pub payload: serde_json::Value,
}
