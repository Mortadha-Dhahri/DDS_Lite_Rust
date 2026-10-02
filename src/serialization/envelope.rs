use serde::{Deserialize, Serialize};

use super::codec::WireMessage;
use super::control::ControlMessage;

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    Data(WireMessage),
    Control(ControlMessage),
}

pub fn encode_network_message(
    message: &NetworkMessage,
) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(message)
}

pub fn decode_network_message(
    bytes: &[u8],
) -> Result<NetworkMessage, bincode::Error> {
    bincode::deserialize(bytes)
}