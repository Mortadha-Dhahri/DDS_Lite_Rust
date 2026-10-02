use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlMessage {
    Nack {
        topic: String,
        missing_sequences: Vec<u64>,
    },
}

pub fn encode_control_message(
    message: &ControlMessage,
) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(message)
}

pub fn decode_control_message(
    bytes: &[u8],
) -> Result<ControlMessage, bincode::Error> {
    bincode::deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_control_message,
        encode_control_message,
        ControlMessage,
    };

    #[test]
    fn nack_round_trip() {
        let message = ControlMessage::Nack {
            topic: "vehicle/state".to_string(),
            missing_sequences: vec![3, 4, 5],
        };

        let encoded =
            encode_control_message(&message).unwrap();

        let decoded =
            decode_control_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn empty_nack_round_trip() {
        let message = ControlMessage::Nack {
            topic: "vehicle/state".to_string(),
            missing_sequences: Vec::new(),
        };

        let encoded =
            encode_control_message(&message).unwrap();

        let decoded =
            decode_control_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }
}