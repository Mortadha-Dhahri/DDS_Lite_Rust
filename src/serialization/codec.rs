use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct WireMessage {
    pub sequence_number: u64,
    pub topic: String,
    pub type_name: String,
    pub payload: Vec<u8>,
}

pub fn serialize_payload<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(value)
}

pub fn deserialize_payload<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode::Error> {
    bincode::deserialize(bytes)
}

pub fn encode_message(message: &WireMessage) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(message)
}

pub fn decode_message(bytes: &[u8]) -> Result<WireMessage, bincode::Error> {
    bincode::deserialize(bytes)
}