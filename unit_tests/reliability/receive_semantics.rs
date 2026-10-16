//! Unit tests for receive semantics.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn heartbeat_marks_participant_alive() {
        let topic = Topic::new("vehicle/state", "VehicleState");

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:6000".parse::<SocketAddr>().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();

        let mut subscriber = Subscriber::<Vec<u8>>::new(
            topic,
            discovery,
            transport,
            QosPolicy::new(10, crate::Reliability::BestEffort),
        )
        .unwrap();

        let heartbeat = ControlMessage::Heartbeat { participant_id: 42 };

        subscriber.handle_control_message(heartbeat).unwrap();

        assert_eq!(
            subscriber.liveliness.state(42),
            crate::LivelinessState::Alive
        );
    }
    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestPayload {
        value: u32,
    }

    #[test]
    fn duplicate_reliable_message_is_acknowledged_without_redelivery() {
        let topic = Topic::new("test/duplicate-ack", "TestPayload");

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let mut subscriber = Subscriber::<TestPayload>::new(
            topic.clone(),
            discovery,
            transport,
            QosPolicy::new(10, crate::Reliability::Reliable),
        )
        .unwrap();

        let publisher = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

        publisher
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let subscriber_address = subscriber.local_addr().unwrap();
        let publisher_address = publisher.local_addr().unwrap();

        let payload = crate::serialize_payload(&TestPayload { value: 42 }).unwrap();

        let message = NetworkMessage::Data(crate::WireMessage {
            sequence_number: 1,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload,
        });

        let bytes = crate::encode_network_message(&message).unwrap();

        // First delivery: the subscriber should return the payload and ACK it.
        publisher.send_to(&bytes, subscriber_address).unwrap();

        assert_eq!(
            subscriber.try_receive().unwrap(),
            Some(TestPayload { value: 42 })
        );

        assert_ack_received(&publisher, topic.name(), 1);

        // Simulate retransmission after the first ACK was lost.
        publisher.send_to(&bytes, subscriber_address).unwrap();

        // The duplicate must not be delivered again.
        assert_eq!(subscriber.try_receive().unwrap(), None);

        // But the subscriber must send another ACK.
        assert_ack_received(&publisher, topic.name(), 1);

        // Confirm that the ACKs came from the subscriber.
        assert_ne!(publisher_address, subscriber_address);
    }

    fn assert_ack_received(
        publisher: &std::net::UdpSocket,
        expected_topic: &str,
        expected_sequence: u64,
    ) {
        let mut buffer = [0u8; 65_535];

        let (size, sender) = publisher
            .recv_from(&mut buffer)
            .expect("expected an ACK from the subscriber");

        match crate::decode_network_message(&buffer[..size]).unwrap() {
            NetworkMessage::Control(ControlMessage::Ack {
                topic,
                sequence_number,
            }) => {
                assert_eq!(topic, expected_topic);
                assert_eq!(sequence_number, expected_sequence);
            }
            other => panic!("expected ACK, got {other:?}"),
        }
    }
    #[test]
    fn messages_from_different_publishers_have_independent_sequences() {
        let topic = Topic::new("test/multiple-publishers", "TestPayload");

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let mut subscriber = Subscriber::<TestPayload>::new(
            topic.clone(),
            discovery,
            transport,
            QosPolicy::new(10, crate::Reliability::Reliable),
        )
        .unwrap();

        let publisher_a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let publisher_b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();

        let subscriber_address = subscriber.local_addr().unwrap();

        let make_message = |value| {
            let payload = crate::serialize_payload(&TestPayload { value }).unwrap();

            let message = NetworkMessage::Data(crate::WireMessage {
                sequence_number: 1,
                topic: topic.name().to_string(),
                type_name: topic.type_name().to_string(),
                payload,
            });

            crate::encode_network_message(&message).unwrap()
        };

        // Both publishers independently begin with sequence 1.
        publisher_a
            .send_to(&make_message(10), subscriber_address)
            .unwrap();

        publisher_b
            .send_to(&make_message(20), subscriber_address)
            .unwrap();

        // Both messages should reach the application.
        let first = subscriber.try_receive().unwrap();
        let second = subscriber.try_receive().unwrap();

        assert!(
            first.is_some(),
            "the first publisher's message should be delivered"
        );

        assert!(
            second.is_some(),
            "the second publisher's sequence 1 must not be treated as a duplicate"
        );

        let mut values = vec![first.unwrap().value, second.unwrap().value];

        values.sort_unstable();

        assert_eq!(values, vec![10, 20]);
    }
#[test]
    fn corrupt_payload_does_not_advance_sequence_tracker() {
        let topic = Topic::new("test/corrupt-payload", "TestPayload");
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut subscriber = Subscriber::<TestPayload>::new(
            topic.clone(),
            discovery,
            transport,
            QosPolicy::new(10, crate::Reliability::Reliable),
        )
        .unwrap();
        let sender: SocketAddr = "127.0.0.1:7400".parse().unwrap();

        let result = subscriber
            .handle_data_message(
                crate::WireMessage {
                    sequence_number: 7,
                    topic: topic.name().to_string(),
                    type_name: topic.type_name().to_string(),
                    payload: vec![0xff, 0x00],
                },
                sender,
            )
            .unwrap();

        assert!(result.is_none());
        assert_eq!(
            subscriber.sequence_trackers.get(&sender).map(|tracker| tracker.last_sequence()),
            None,
            "a corrupt payload must not create or advance sequence state"
        );
    }

    #[test]
    fn sequence_one_after_high_water_mark_starts_a_new_stream() {
        let topic = Topic::new("test/publisher-restart", "TestPayload");
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut subscriber = Subscriber::<TestPayload>::new(
            topic.clone(),
            discovery,
            transport,
            QosPolicy::new(10, crate::Reliability::BestEffort),
        )
        .unwrap();
        let sender: SocketAddr = "127.0.0.1:7401".parse().unwrap();
        let payload = crate::serialize_payload(&TestPayload { value: 1 }).unwrap();

        let high = crate::WireMessage {
            sequence_number: 8,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload: payload.clone(),
        };
        assert!(subscriber.handle_data_message(high, sender).unwrap().is_some());

        let restarted = crate::WireMessage {
            sequence_number: 1,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload,
        };
        assert!(
            subscriber.handle_data_message(restarted, sender).unwrap().is_some(),
            "sequence 1 after a higher sequence should be accepted as a restarted stream"
        );
        assert_eq!(
            subscriber.sequence_trackers.get(&sender).unwrap().last_sequence(),
            1
        );
    }

