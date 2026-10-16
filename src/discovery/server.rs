use crate::core::Participant;
use crate::qos::liveliness::LivelinessTracker;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use super::{Discovery, DiscoveryRequest, DiscoveryResponse, Endpoint, LocalDiscovery};
pub struct DiscoveryServer {
    socket: UdpSocket,
    discovery: LocalDiscovery,
    liveliness: LivelinessTracker,
}

impl DiscoveryServer {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Self::bind_with_timeout(address, Duration::from_secs(10))
    }

    pub fn bind_with_timeout(address: SocketAddr, timeout: Duration) -> io::Result<Self> {
        let socket = UdpSocket::bind(address)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
        Ok(Self {
            socket,
            discovery: LocalDiscovery::new(),
            liveliness: LivelinessTracker::new(timeout),
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
                self.liveliness.observe(participant_id);

                DiscoveryResponse::Registered
            }

            DiscoveryRequest::Heartbeat { participant_id } => {
                if self.discovery.participant_address(participant_id).is_some() {
                    self.liveliness.observe(participant_id);
                    DiscoveryResponse::Registered
                } else {
                    DiscoveryResponse::Error(format!("unknown participant: {participant_id}"))
                }
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
    fn expire_participants(&mut self) {
        let expired = self.liveliness.expired_participants();

        for participant_id in expired {
            self.discovery.remove_participant(participant_id);
            self.liveliness.forget(participant_id);
        }
    }
}

impl DiscoveryServer {
    pub fn run(&mut self) -> io::Result<()> {
        let mut buffer = [0u8; 65_535];

        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((size, sender)) => {
                    self.expire_participants();

                    let request = bincode::deserialize::<DiscoveryRequest>(&buffer[..size])
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

                    let response = self.handle_request(request);

                    let bytes = bincode::serialize(&response)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

                    self.socket.send_to(&bytes, sender)?;
                }
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut =>
                {
                    self.expire_participants();
                }
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
#[path = "../../unit_tests/discovery/server_lifecycle.rs"]
mod discovery_lifecycle_tests;
