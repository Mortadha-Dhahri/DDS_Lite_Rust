use std::io;
use std::net::SocketAddr;

use crate::{
    ControlMessage, EndpointKind, NetworkDiscovery, NetworkMessage, Participant, Transport,
    UdpTransport,
};

pub struct ParticipantRuntime {
    pub participant: Participant,
    pub discovery: NetworkDiscovery,
    pub transport: UdpTransport,
    topic: String,
    kind: EndpointKind,
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
            topic: topic.to_string(),
            kind,
        })
    }

    pub fn send_heartbeat(&self) -> io::Result<()> {
        let heartbeat = ControlMessage::Heartbeat {
            participant_id: self.participant.id(),
        };

        let message = NetworkMessage::Control(heartbeat);

        let bytes = crate::encode_network_message(&message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let peer_kind = match self.kind {
            EndpointKind::Publisher => EndpointKind::Subscriber,
            EndpointKind::Subscriber => EndpointKind::Publisher,
        };

        let endpoints = self.discovery.lookup(&self.topic, peer_kind)?;

        for endpoint in endpoints {
            if endpoint.participant_id == self.participant.id() {
                continue;
            }

            self.transport.send(&bytes, endpoint.address)?;
        }

        Ok(())
    }
}
