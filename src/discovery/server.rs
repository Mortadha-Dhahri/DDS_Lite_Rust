use crate::core::Participant;
use std::io;
use std::net::{SocketAddr, UdpSocket};

use super::{Discovery, DiscoveryRequest, DiscoveryResponse, Endpoint, LocalDiscovery};

pub struct DiscoveryServer {
    socket: UdpSocket,
    discovery: LocalDiscovery,
}

impl DiscoveryServer {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(address)?;

        Ok(Self {
            socket,
            discovery: LocalDiscovery::new(),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    fn handle_request(&mut self, request: DiscoveryRequest) -> DiscoveryResponse {
        match request {
            DiscoveryRequest::RegisterParticipant {
                participant_id,
                address,
            } => {
                let participant = Participant::new(participant_id, address);

                self.discovery.register_participant(participant);

                DiscoveryResponse::Registered
            }

            DiscoveryRequest::RegisterEndpoint {
                participant_id,
                topic,
                kind,
            } => {
                let endpoint = Endpoint {
                    participant_id,
                    topic,
                    kind,
                };

                self.discovery.register_endpoint(endpoint);

                DiscoveryResponse::Registered
            }

            DiscoveryRequest::Lookup { topic, kind } => {
                let endpoints = self.discovery.lookup(&topic, kind);

                let discovered = endpoints
                    .into_iter()
                    .filter_map(|endpoint| {
                        let address = self
                            .discovery
                            .participant_address(endpoint.participant_id)?;

                        Some(super::DiscoveredEndpoint {
                            participant_id: endpoint.participant_id,
                            topic: endpoint.topic,
                            address,
                            kind: endpoint.kind,
                        })
                    })
                    .collect();

                DiscoveryResponse::Endpoints(discovered)
            }
        }
    }
}

impl DiscoveryServer {
    pub fn run(&mut self) -> io::Result<()> {
        let mut buffer = [0u8; 65_535];

        loop {
            let (size, sender) = self.socket.recv_from(&mut buffer)?;

            let request = bincode::deserialize::<DiscoveryRequest>(&buffer[..size])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

            let response = self.handle_request(request);

            let bytes = bincode::serialize(&response)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

            self.socket.send_to(&bytes, sender)?;
        }
    }
}
