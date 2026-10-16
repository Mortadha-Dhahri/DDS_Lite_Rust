use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlMessage {
    Nack {
        topic: String,
        missing_sequences: Vec<u64>,
    },
    Ack {
        topic: String,
        sequence_number: u64,
    },
    Heartbeat {
        participant_id: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub message: ControlMessage,
}

pub fn encode_control_message(message: &ControlMessage) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(&ControlEnvelope {
        message: message.clone(),
    })
}

pub fn decode_control_message(bytes: &[u8]) -> Result<ControlMessage, bincode::Error> {
    let envelope: ControlEnvelope = bincode::deserialize(bytes)?;

    Ok(envelope.message)
}

#[cfg(test)]
#[path = "../../unit_tests/serialization/control_message_roundtrip.rs"]
mod control_message_tests;
