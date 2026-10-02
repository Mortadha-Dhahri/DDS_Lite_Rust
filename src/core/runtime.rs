use std::io;
use std::net::SocketAddr;

use crate::{EndpointKind, NetworkDiscovery, Participant, UdpTransport};

pub struct ParticipantRuntime {
    pub participant: Participant,
    pub discovery: NetworkDiscovery,
    pub transport: UdpTransport,
}

impl ParticipantRuntime {
    pub fn new(
        participant_id: u64,
        port: u16,
        kind: EndpointKind,
        topic: &str,
        discovery_server: SocketAddr,
    ) -> io::Result<Self> {
        let participant_address = format!("127.0.0.1:{port}").parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid participant address")
        })?;

        let participant = Participant::new(participant_id, participant_address);

        let transport = UdpTransport::bind(participant_address)?;

        let discovery = NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), discovery_server)?;

        discovery.register_participant(&participant)?;

        discovery.register_endpoint(participant.id(), topic, kind)?;

        Ok(Self {
            participant,
            discovery,
            transport,
        })
    }
}
