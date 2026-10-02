use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ControlMessage {
    Nack {
        topic: String,
        missing_sequences: Vec<u64>,
    },

    Ack {
        topic: String,
        sequence_number: u64,
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
mod tests {
    use super::{ControlMessage, decode_control_message, encode_control_message};

    #[test]
    fn nack_round_trip() {
        let message = ControlMessage::Nack {
            topic: "vehicle/state".to_string(),
            missing_sequences: vec![3, 4, 5],
        };

        let encoded = encode_control_message(&message).unwrap();

        let decoded = decode_control_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn empty_nack_round_trip() {
        let message = ControlMessage::Nack {
            topic: "vehicle/state".to_string(),
            missing_sequences: Vec::new(),
        };

        let encoded = encode_control_message(&message).unwrap();

        let decoded = decode_control_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }
    #[test]
    fn ack_round_trip() {
        let message = ControlMessage::Ack {
            topic: "vehicle/state".to_string(),
            sequence_number: 42,
        };

        let encoded = encode_control_message(&message).unwrap();

        let decoded = decode_control_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }
}
