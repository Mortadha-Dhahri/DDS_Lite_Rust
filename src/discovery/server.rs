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
mod tests {
    use super::*;
    use crate::discovery::EndpointKind;
    use std::thread;
    use std::time::Duration;

    fn test_server(timeout: Duration) -> DiscoveryServer {
        DiscoveryServer::bind_with_timeout("127.0.0.1:0".parse().unwrap(), timeout).unwrap()
    }

    fn register_participant(server: &mut DiscoveryServer, participant_id: u64, port: u16) {
        let response = server.handle_request(DiscoveryRequest::RegisterParticipant {
            participant_id,
            address: format!("127.0.0.1:{port}").parse().unwrap(),
        });

        assert!(matches!(response, DiscoveryResponse::Registered));
    }

    fn register_endpoint(
        server: &mut DiscoveryServer,
        participant_id: u64,
        topic: &str,
        kind: EndpointKind,
    ) {
        let response = server.handle_request(DiscoveryRequest::RegisterEndpoint {
            participant_id,
            topic: topic.to_string(),
            kind,
        });

        assert!(matches!(response, DiscoveryResponse::Registered));
    }

    #[test]
    fn heartbeat_keeps_participant_alive() {
        let mut server = test_server(Duration::from_millis(100));

        register_participant(&mut server, 1, 7001);

        thread::sleep(Duration::from_millis(60));

        let response = server.handle_request(DiscoveryRequest::Heartbeat { participant_id: 1 });

        assert!(matches!(response, DiscoveryResponse::Registered));

        thread::sleep(Duration::from_millis(60));

        server.expire_participants();

        assert!(server.discovery.participant_address(1).is_some());
    }

    #[test]
    fn expired_participant_and_endpoints_are_removed() {
        let mut server = test_server(Duration::from_millis(20));

        register_participant(&mut server, 1, 7001);
        register_participant(&mut server, 2, 7002);

        register_endpoint(&mut server, 1, "vehicle/state", EndpointKind::Publisher);

        register_endpoint(&mut server, 2, "vehicle/state", EndpointKind::Subscriber);

        thread::sleep(Duration::from_millis(40));

        server.expire_participants();

        assert!(server.discovery.participant_address(1).is_none());
        assert!(server.discovery.participant_address(2).is_none());

        assert!(
            server
                .discovery
                .lookup("vehicle/state", EndpointKind::Publisher)
                .is_empty()
        );

        assert!(
            server
                .discovery
                .lookup("vehicle/state", EndpointKind::Subscriber)
                .is_empty()
        );
    }

    #[test]
    fn heartbeat_from_unknown_participant_is_rejected() {
        let mut server = test_server(Duration::from_millis(100));

        let response = server.handle_request(DiscoveryRequest::Heartbeat { participant_id: 99 });

        assert!(matches!(response, DiscoveryResponse::Error(_)));
    }

    #[test]
    fn expiration_removes_only_inactive_participant() {
        let mut server = test_server(Duration::from_millis(100));

        register_participant(&mut server, 1, 7001);
        register_participant(&mut server, 2, 7002);

        register_endpoint(&mut server, 1, "vehicle/state", EndpointKind::Publisher);

        register_endpoint(&mut server, 2, "vehicle/state", EndpointKind::Publisher);

        // Let both participants approach expiration.
        thread::sleep(Duration::from_millis(60));

        // Refresh participant 2 only.
        let response = server.handle_request(DiscoveryRequest::Heartbeat { participant_id: 2 });

        assert!(matches!(response, DiscoveryResponse::Registered));

        // Allow participant 1 to expire.
        thread::sleep(Duration::from_millis(60));

        server.expire_participants();

        // Participant 1 must have expired.
        assert!(server.discovery.participant_address(1).is_none());

        // Participant 2 must remain registered.
        assert!(server.discovery.participant_address(2).is_some());

        // Only participant 2's publisher endpoint should remain.
        let publishers = server
            .discovery
            .lookup("vehicle/state", EndpointKind::Publisher);

        assert_eq!(publishers.len(), 1);
        assert_eq!(publishers[0].participant_id, 2);
    }
}
