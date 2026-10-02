/*

Conceptually :

Participant → Server

REGISTER_PARTICIPANT
    participant_id
    address

REGISTER_ENDPOINT
    participant_id
    topic
    kind

LOOKUP
    topic
    kind

*/

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use super::EndpointKind;

#[derive(Debug, Serialize, Deserialize)]
pub enum DiscoveryRequest {
    RegisterParticipant {
        participant_id: u64,
        address: SocketAddr,
    },

    RegisterEndpoint {
        participant_id: u64,
        topic: String,
        kind: EndpointKind,
    },

    Lookup {
        topic: String,
        kind: EndpointKind,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DiscoveryResponse {
    Registered,

    Endpoints(Vec<DiscoveredEndpoint>),

    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoveredEndpoint {
    pub participant_id: u64,
    pub topic: String,
    pub address: SocketAddr,
    pub kind: EndpointKind,
}
