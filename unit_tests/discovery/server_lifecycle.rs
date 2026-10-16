//! Unit tests for server lifecycle.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

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
#[test]
    fn malformed_datagram_does_not_stop_discovery_server() {
        use std::net::UdpSocket;

        let mut server = test_server(Duration::from_secs(2));
        let server_address = server.local_addr().unwrap();
        thread::spawn(move || server.run().unwrap());

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();
        client.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        client.send_to(&[0xff, 0x00, 0x01], server_address).unwrap();

        let request = DiscoveryRequest::RegisterParticipant {
            participant_id: 987,
            address: "127.0.0.1:7987".parse().unwrap(),
        };
        let bytes = bincode::serialize(&request).unwrap();
        client.send_to(&bytes, server_address).unwrap();

        let mut buffer = [0u8; 1024];
        let (size, _) = client.recv_from(&mut buffer).unwrap();
        let response: DiscoveryResponse = bincode::deserialize(&buffer[..size]).unwrap();
        assert!(matches!(response, DiscoveryResponse::Registered));
    }

