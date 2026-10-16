//! Unit tests for retry and acknowledgement.
//! These tests are compiled as child modules of the implementation module so they can
//! verify internal invariants without exposing private implementation details.

    use super::*;
    use std::net::UdpSocket;
    use std::time::Duration;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestMessage {
        value: u32,
    }

    fn send_sample(
        transport: &UdpTransport,
        topic: &Topic,
        destination: std::net::SocketAddr,
        sequence_number: u64,
        value: u32,
    ) {
        let payload = serialize_payload(&TestMessage { value }).unwrap();

        let message = NetworkMessage::Data(WireMessage {
            sequence_number,
            topic: topic.name().to_string(),
            type_name: topic.type_name().to_string(),
            payload,
        });

        let bytes = encode_network_message(&message).unwrap();

        transport.send(&bytes, destination).unwrap();
    }
    #[test]
    fn expired_acknowledgement_is_retransmitted() {
        println!("STEP 1: bind receiver");

        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver_address = receiver.local_addr().unwrap();

        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        println!("receiver = {}", receiver_address);

        println!("STEP 2: bind publisher transport");

        let publisher_address = "127.0.0.1:7202".parse().unwrap();

        let transport = UdpTransport::bind(publisher_address).unwrap();

        println!("publisher transport bound");

        println!("STEP 3: create publisher");

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let topic = Topic::new("test/topic", "TestMessage");

        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

        println!("publisher created");

        println!("STEP 4: populate history");

        publisher.history.push(1, TestMessage { value: 42 });

        println!("history populated");

        println!("STEP 5: create expired ACK");

        publisher
            .pending_acks
            .entry(receiver_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now() - Duration::from_secs(1),
                    retransmission_attempts: 0,
                },
            );

        println!("pending ACK populated");

        println!("STEP 6: retransmit expired ACK");

        publisher
            .retransmit_expired_acknowledgements(Duration::from_millis(100))
            .unwrap();

        println!("retransmit returned");

        println!("STEP 7: receive retransmitted packet");

        let mut buffer = vec![0u8; 65_535];

        let (size, sender) = receiver.recv_from(&mut buffer).unwrap();

        println!("received {} bytes from {}", size, sender);

        assert_eq!(sender, publisher_address);

        let network_message = decode_network_message(&buffer[..size]).unwrap();

        match network_message {
            NetworkMessage::Data(message) => {
                assert_eq!(message.sequence_number, 1);
                assert_eq!(message.topic, "test/topic");

                let decoded: TestMessage = deserialize_payload(&message.payload).unwrap();

                assert_eq!(decoded, TestMessage { value: 42 });
            }

            NetworkMessage::Control(_) => {
                panic!("Expected a data message");
            }
        }
    }

    #[test]
    fn best_effort_does_not_track_acknowledgements() {
        std::thread::spawn(|| {
            let mut server = DiscoveryServer::bind("127.0.0.1:6203".parse().unwrap()).unwrap();

            server.run().unwrap();
        });

        std::thread::sleep(Duration::from_millis(100));

        let receiver = UdpSocket::bind("127.0.0.1:7331").unwrap();
        let receiver_address = receiver.local_addr().unwrap();

        let publisher_address = "127.0.0.1:7332".parse().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6203".parse().unwrap(),
        )
        .unwrap();

        let publisher_participant = Participant::new(1, publisher_address);
        let subscriber_participant = Participant::new(2, receiver_address);

        discovery
            .register_participant(&publisher_participant)
            .unwrap();

        discovery
            .register_participant(&subscriber_participant)
            .unwrap();

        let topic = Topic::new("test/qos", "TestMessage");

        discovery
            .register_endpoint(
                publisher_participant.id(),
                topic.name(),
                EndpointKind::Publisher,
            )
            .unwrap();

        discovery
            .register_endpoint(
                subscriber_participant.id(),
                topic.name(),
                EndpointKind::Subscriber,
            )
            .unwrap();

        let transport = UdpTransport::bind(publisher_address).unwrap();

        let qos = QosPolicy::new(10, Reliability::BestEffort);

        let mut publisher =
            Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

        publisher.publish(&TestMessage { value: 42 }).unwrap();

        assert_eq!(publisher.pending_ack_count(), 0);
    }
    #[test]
    fn best_effort_ignores_nack() {
        std::thread::spawn(|| {
            let mut server = DiscoveryServer::bind("127.0.0.1:6201".parse().unwrap()).unwrap();

            server.run().unwrap();
        });

        std::thread::sleep(Duration::from_millis(100));

        let receiver = UdpSocket::bind("127.0.0.1:7301").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();
        let publisher_address = "127.0.0.1:7302".parse().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6201".parse().unwrap(),
        )
        .unwrap();

        let publisher_participant = Participant::new(1, publisher_address);
        let subscriber_participant = Participant::new(2, receiver_address);

        discovery
            .register_participant(&publisher_participant)
            .unwrap();

        discovery
            .register_participant(&subscriber_participant)
            .unwrap();

        let topic = Topic::new("test/nack-qos", "TestMessage");

        discovery
            .register_endpoint(
                publisher_participant.id(),
                topic.name(),
                EndpointKind::Publisher,
            )
            .unwrap();

        discovery
            .register_endpoint(
                subscriber_participant.id(),
                topic.name(),
                EndpointKind::Subscriber,
            )
            .unwrap();

        let transport = UdpTransport::bind(publisher_address).unwrap();

        let qos = QosPolicy::new(10, Reliability::BestEffort);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        // Populate publisher history and send the initial DATA packet.
        publisher.publish(&TestMessage { value: 42 }).unwrap();

        let mut buffer = vec![0u8; 65_535];

        // Consume the original DATA packet.
        receiver.recv_from(&mut buffer).unwrap();

        let nack = ControlMessage::Nack {
            topic: topic.name().to_string(),
            missing_sequences: vec![1],
        };

        publisher
            .handle_control_message(nack, receiver_address)
            .unwrap();

        // BestEffort must ignore the NACK and send no retransmission.
        let result = receiver.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut
            ),
            "BestEffort publisher should not retransmit after NACK, got: {:?}",
            result
        );
    }

    #[test]
    fn late_and_duplicate_acknowledgements_preserve_pending_state() {
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/ack", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        let subscriber = UdpSocket::bind("127.0.0.1:0").unwrap();
        let subscriber_address = subscriber.local_addr().unwrap();

        // Simulate a pending ACK for sequence 2.
        publisher
            .pending_acks
            .entry(subscriber_address)
            .or_default()
            .insert(
                2,
                PendingAck {
                    sent_at: Instant::now(),
                    retransmission_attempts: 0,
                },
            );

        assert_eq!(publisher.pending_ack_count(), 1);

        // A late ACK for sequence 1 must not remove sequence 2.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                subscriber_address,
            )
            .unwrap();

        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "a late ACK must not remove an unrelated pending acknowledgement"
        );

        // The correct ACK clears sequence 2.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 2,
                },
                subscriber_address,
            )
            .unwrap();

        assert_eq!(
            publisher.pending_ack_count(),
            0,
            "the matching ACK should clear the pending acknowledgement"
        );

        // Receiving the same ACK again must be harmless.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 2,
                },
                subscriber_address,
            )
            .unwrap();

        assert_eq!(
            publisher.pending_ack_count(),
            0,
            "a duplicate ACK must not recreate pending state"
        );
    }
    #[test]
    fn nack_for_missing_history_entry_does_not_block_available_retransmission() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        // History depth is 1: adding sequence 2 should evict sequence 1.
        let topic = Topic::new("test/nack-history", "TestMessage");
        let qos = QosPolicy::new(1, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 10 });
        publisher.history.push(2, TestMessage { value: 20 });

        // Confirm the intended history state before testing the NACK.
        assert!(
            publisher.history.find(1).is_none(),
            "sequence 1 should no longer be in history"
        );
        assert!(
            publisher.history.find(2).is_some(),
            "sequence 2 should remain in history"
        );

        // Request both sequences: one unavailable and one available.
        let nack = ControlMessage::Nack {
            topic: topic.name().to_string(),
            missing_sequences: vec![1, 2],
        };

        publisher
            .handle_control_message(nack, receiver_address)
            .expect("handling an unavailable sequence should not fail");

        // Sequence 1 cannot be retransmitted, but sequence 2 should be.
        let mut buffer = [0u8; 65_535];
        let (size, sender) = receiver
            .recv_from(&mut buffer)
            .expect("available sequence 2 should still be retransmitted");

        assert_eq!(sender, publisher.local_addr().unwrap());

        match decode_network_message(&buffer[..size]).unwrap() {
            NetworkMessage::Data(message) => {
                assert_eq!(message.sequence_number, 2);

                let decoded: TestMessage = deserialize_payload(&message.payload).unwrap();
                assert_eq!(decoded, TestMessage { value: 20 });
            }
            NetworkMessage::Control(_) => {
                panic!("Expected a retransmitted data message");
            }
        }
    }
    #[test]
    fn retransmission_stops_after_maximum_attempts() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/retry-limit", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 42 });

        publisher
            .pending_acks
            .entry(receiver_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now() - Duration::from_secs(1),
                    retransmission_attempts: 0,
                },
            );

        assert_eq!(publisher.pending_ack_count(), 1);

        // Each call should retransmit the message while attempts remain.
        for attempt in 1..=MAX_RETRANSMISSIONS {
            publisher
                .retransmit_expired_acknowledgements(Duration::ZERO)
                .unwrap();

            let mut buffer = [0u8; 65_535];

            let (size, _) = receiver
                .recv_from(&mut buffer)
                .expect("expected a retransmission");

            match decode_network_message(&buffer[..size]).unwrap() {
                NetworkMessage::Data(message) => {
                    assert_eq!(message.sequence_number, 1);
                }
                NetworkMessage::Control(_) => {
                    panic!("Expected a retransmitted data message");
                }
            }

            let pending = publisher
                .pending_acks
                .get(&receiver_address)
                .and_then(|entries| entries.get(&1))
                .expect("pending ACK should remain until retries are exhausted");

            assert_eq!(
                pending.retransmission_attempts, attempt,
                "unexpected retransmission attempt count"
            );
        }

        // The next expired check must remove the exhausted entry without sending.
        publisher
            .retransmit_expired_acknowledgements(Duration::ZERO)
            .unwrap();

        assert_eq!(
            publisher.pending_ack_count(),
            0,
            "exhausted pending ACK should be removed"
        );

        // Verify that no fourth retransmission was sent.
        let mut buffer = [0u8; 65_535];

        let result = receiver.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut
            ),
            "publisher sent an unexpected retransmission after reaching the limit"
        );
    }
    #[test]
    fn retransmission_limit_is_independent_per_subscriber() {
        let exhausted_receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let active_receiver = UdpSocket::bind("127.0.0.1:0").unwrap();

        for receiver in [&exhausted_receiver, &active_receiver] {
            receiver
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
        }

        let exhausted_address = exhausted_receiver.local_addr().unwrap();
        let active_address = active_receiver.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/independent-retries", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 42 });

        // Subscriber A has exhausted its retry budget.
        publisher
            .pending_acks
            .entry(exhausted_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now() - Duration::from_secs(1),
                    retransmission_attempts: MAX_RETRANSMISSIONS,
                },
            );

        // Subscriber B still has retries available.
        publisher
            .pending_acks
            .entry(active_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now() - Duration::from_secs(1),
                    retransmission_attempts: 1,
                },
            );

        assert_eq!(publisher.pending_ack_count(), 2);

        publisher
            .retransmit_expired_acknowledgements(Duration::ZERO)
            .unwrap();

        // Subscriber A's exhausted entry must be removed.
        assert!(
            !publisher.pending_acks.contains_key(&exhausted_address),
            "exhausted subscriber should have no pending ACK entries"
        );

        // Subscriber B's message should have been retransmitted.
        let mut buffer = [0u8; 65_535];

        let (size, sender) = active_receiver
            .recv_from(&mut buffer)
            .expect("active subscriber should receive a retransmission");

        assert_eq!(
            sender,
            publisher.transport.local_addr().unwrap(),
            "retransmission should originate from the publisher"
        );

        match decode_network_message(&buffer[..size]).unwrap() {
            NetworkMessage::Data(message) => {
                assert_eq!(message.sequence_number, 1);
                assert_eq!(message.topic, "test/independent-retries");
            }
            NetworkMessage::Control(_) => {
                panic!("expected a retransmitted data message");
            }
        }

        // Subscriber B remains pending, with its retry count incremented.
        let pending = publisher
            .pending_acks
            .get(&active_address)
            .and_then(|entries| entries.get(&1))
            .expect("active subscriber's pending ACK should remain");

        assert_eq!(pending.retransmission_attempts, 2);

        // Subscriber A must not receive a retransmission.
        let mut buffer = [0u8; 65_535];

        let result = exhausted_receiver.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut
            ),
            "exhausted subscriber must not receive another retransmission"
        );

        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "only the active subscriber's pending ACK should remain"
        );
    }
    #[test]
    fn missing_history_removes_only_the_unretransmittable_pending_ack() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/missing-history-retry", "TestMessage");
        let qos = QosPolicy::new(1, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        // History depth is 1, so inserting sequence 2 evicts sequence 1.
        publisher.history.push(1, TestMessage { value: 10 });
        publisher.history.push(2, TestMessage { value: 20 });

        assert!(publisher.history.find(1).is_none());
        assert!(publisher.history.find(2).is_some());

        let expired_at = Instant::now() - Duration::from_secs(1);

        // Both sequences have expired pending ACKs.
        let pending = publisher.pending_acks.entry(receiver_address).or_default();

        pending.insert(
            1,
            PendingAck {
                sent_at: expired_at,
                retransmission_attempts: 0,
            },
        );

        pending.insert(
            2,
            PendingAck {
                sent_at: expired_at,
                retransmission_attempts: 0,
            },
        );

        publisher
            .retransmit_expired_acknowledgements(Duration::ZERO)
            .unwrap();

        let pending = publisher
            .pending_acks
            .get(&receiver_address)
            .expect("available sequence should keep the pending ACK map");

        assert!(
            !pending.contains_key(&1),
            "pending ACK for the evicted sample should be removed"
        );

        assert_eq!(
            pending.get(&2).unwrap().retransmission_attempts,
            1,
            "available sample should still be retransmitted and tracked"
        );

        // Verify that sequence 2 was actually sent.
        let mut buffer = [0u8; 65_535];

        let (size, _) = receiver
            .recv_from(&mut buffer)
            .expect("available sequence 2 should be retransmitted");

        match decode_network_message(&buffer[..size]).unwrap() {
            NetworkMessage::Data(message) => {
                assert_eq!(message.sequence_number, 2);

                let decoded: TestMessage = deserialize_payload(&message.payload).unwrap();

                assert_eq!(decoded, TestMessage { value: 20 });
            }
            NetworkMessage::Control(_) => {
                panic!("expected a retransmitted data message");
            }
        }

        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "only the ACK for the retransmittable sequence should remain"
        );
    }
    #[test]
    fn acknowledgement_from_unknown_subscriber_preserves_pending_state() {
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/ack-isolation", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        let subscriber = UdpSocket::bind("127.0.0.1:0").unwrap();
        let unknown_sender = UdpSocket::bind("127.0.0.1:0").unwrap();

        let subscriber_address = subscriber.local_addr().unwrap();
        let unknown_address = unknown_sender.local_addr().unwrap();

        publisher
            .pending_acks
            .entry(subscriber_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now(),
                    retransmission_attempts: 0,
                },
            );

        // An unrelated sender acknowledges sequence 1.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                unknown_address,
            )
            .unwrap();

        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "an ACK from an unknown subscriber must not clear another subscriber's pending ACK"
        );

        assert!(
            publisher
                .pending_acks
                .get(&subscriber_address)
                .unwrap()
                .contains_key(&1),
            "the original subscriber's pending ACK must remain"
        );
    }
    #[test]
    fn acknowledgement_clears_only_the_matching_subscribers_pending_state() {
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/multi-subscriber-ack", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        let subscriber_a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let subscriber_b = UdpSocket::bind("127.0.0.1:0").unwrap();

        let address_a = subscriber_a.local_addr().unwrap();
        let address_b = subscriber_b.local_addr().unwrap();

        // Both subscribers are waiting for ACK sequence 1.
        for address in [address_a, address_b] {
            publisher.pending_acks.entry(address).or_default().insert(
                1,
                PendingAck {
                    sent_at: Instant::now(),
                    retransmission_attempts: 0,
                },
            );
        }

        assert_eq!(publisher.pending_ack_count(), 2);

        // Subscriber A acknowledges sequence 1.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                address_a,
            )
            .unwrap();

        // Only subscriber A's pending ACK should be removed.
        assert_eq!(publisher.pending_ack_count(), 1);
        assert!(!publisher.pending_acks.contains_key(&address_a));

        assert!(
            publisher
                .pending_acks
                .get(&address_b)
                .unwrap()
                .contains_key(&1),
            "subscriber B's ACK must remain pending"
        );

        // Subscriber B acknowledges the same sequence.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                address_b,
            )
            .unwrap();

        assert_eq!(publisher.pending_ack_count(), 0);
    }
    #[test]
    fn only_subscriber_with_pending_ack_receives_retransmission() {
        let subscriber_a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let subscriber_b = UdpSocket::bind("127.0.0.1:0").unwrap();

        for subscriber in [&subscriber_a, &subscriber_b] {
            subscriber
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
        }

        let address_a = subscriber_a.local_addr().unwrap();
        let address_b = subscriber_b.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/multi-subscriber-retry", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        // Sequence 1 is available for retransmission.
        publisher.history.push(1, TestMessage { value: 42 });

        let expired_at = Instant::now() - Duration::from_secs(1);

        // Both subscribers initially have a pending ACK for sequence 1.
        for address in [address_a, address_b] {
            publisher.pending_acks.entry(address).or_default().insert(
                1,
                PendingAck {
                    sent_at: expired_at,
                    retransmission_attempts: 0,
                },
            );
        }

        assert_eq!(publisher.pending_ack_count(), 2);

        // Subscriber A acknowledges sequence 1.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                address_a,
            )
            .unwrap();

        // Only subscriber B should still have a pending ACK.
        assert_eq!(publisher.pending_ack_count(), 1);
        assert!(!publisher.pending_acks.contains_key(&address_a));
        assert!(publisher.pending_acks.contains_key(&address_b));

        // Process expired ACKs. Only B should receive a retransmission.
        publisher
            .retransmit_expired_acknowledgements(Duration::ZERO)
            .unwrap();

        let mut buffer = [0u8; 65_535];

        // Subscriber B must receive sequence 1 again.
        let (size, sender) = subscriber_b
            .recv_from(&mut buffer)
            .expect("subscriber B should receive the retransmission");

        assert_eq!(sender, publisher.local_addr().unwrap());

        match decode_network_message(&buffer[..size]).unwrap() {
            NetworkMessage::Data(message) => {
                assert_eq!(message.sequence_number, 1);
                assert_eq!(message.topic, "test/multi-subscriber-retry");

                let decoded: TestMessage = deserialize_payload(&message.payload).unwrap();
                assert_eq!(decoded, TestMessage { value: 42 });
            }
            NetworkMessage::Control(_) => {
                panic!("Expected a retransmitted data message");
            }
        }

        // Subscriber A must not receive a retransmission.
        let result = subscriber_a.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.kind() == io::ErrorKind::TimedOut
            ),
            "subscriber A must not receive a retransmission after acknowledging sequence 1"
        );

        // B's retry is tracked; A's completed delivery stays cleared.
        let pending_b = publisher
            .pending_acks
            .get(&address_b)
            .and_then(|pending| pending.get(&1))
            .expect("subscriber B should still have a pending ACK");

        assert_eq!(pending_b.retransmission_attempts, 1);
        assert_eq!(publisher.pending_ack_count(), 1);
        assert!(!publisher.pending_acks.contains_key(&address_a));
    }

    #[test]
    fn reliable_delivery_recovers_a_dropped_sample() {
        let topic = Topic::new("test/packet-loss-recovery", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let publisher_discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let publisher_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let mut publisher = Publisher::<TestMessage>::new(
            topic.clone(),
            publisher_discovery,
            publisher_transport,
            qos.clone(),
        )
        .unwrap();

        let subscriber_discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let subscriber_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let mut subscriber = Subscriber::<TestMessage>::new(
            topic.clone(),
            subscriber_discovery,
            subscriber_transport,
            qos,
        )
        .unwrap();

        // Nonblocking reads let the test control exactly when packets are processed.
        subscriber.set_nonblocking(true).unwrap();
        publisher.set_nonblocking(true).unwrap();

        //let publisher_address = publisher.local_addr().unwrap();
        let subscriber_address = subscriber.local_addr().unwrap();

        // Retain all three samples so sequence 2 can be retransmitted.
        for (sequence, value) in [(1, 10), (2, 20), (3, 30)] {
            publisher.history.push(sequence, TestMessage { value });
        }

        send_sample(&publisher.transport, &topic, subscriber_address, 1, 10);
        send_sample(&publisher.transport, &topic, subscriber_address, 3, 30);

        let received = subscriber
            .try_receive()
            .expect("subscriber should process sequence 1");

        assert_eq!(
            received,
            Some(TestMessage { value: 10 }),
            "subscriber should deliver sequence 1"
        );

        let publisher_address = publisher.local_addr().unwrap();
        println!("Publisher transport: {publisher_address}");
        println!("Subscriber transport: {subscriber_address}");

        let received_control = publisher
            .try_receive_control()
            .expect("publisher should process the ACK");

        println!("Publisher processed control message: {received_control}");

        assert!(
            received_control,
            "publisher should receive the ACK for sequence 1"
        );

        // 2. Simulate packet loss: sequence 2 is never sent.
        // 3. Sequence 3 arrives, exposing the missing sequence.

        send_sample(&publisher.transport, &topic, subscriber_address, 3, 30);

        assert_eq!(
            subscriber.try_receive().unwrap(),
            Some(TestMessage { value: 30 })
        );

        // The subscriber sends NACK(2), followed by ACK(3).
        // The first control packet should be the NACK.
        assert!(
            publisher.try_receive_control().unwrap(),
            "publisher should receive the NACK for sequence 2"
        );

        // Handling the NACK should retransmit sequence 2.
        assert!(
            publisher.try_receive_control().unwrap(),
            "publisher should receive the ACK for sequence 3"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(1);

        let recovered = loop {
            if let Some(message) = subscriber.try_receive().unwrap() {
                break Some(message);
            }

            if std::time::Instant::now() >= deadline {
                break None;
            }

            std::thread::yield_now();
        };

        assert_eq!(
            recovered,
            Some(TestMessage { value: 20 }),
            "subscriber should deliver retransmitted sequence 2"
        );

        // 5. Replay sequence 2 to simulate a duplicate retransmission.

        send_sample(&publisher.transport, &topic, subscriber_address, 2, 20);

        assert_eq!(
            subscriber.try_receive().unwrap(),
            None,
            "a duplicate sample must not be delivered twice"
        );

        // The subscriber must still ACK the duplicate.
        assert!(
            publisher.try_receive_control().unwrap(),
            "publisher should receive an ACK for the duplicate sequence"
        );

        assert_eq!(publisher_address.port() != 0, true);
    }
    #[test]
    fn publishing_and_acknowledgement_clear_pending_state() {
        use std::thread;

        // Start an isolated discovery server on an available port.
        let mut server = DiscoveryServer::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let server_address = server.local_addr().unwrap();

        thread::spawn(move || {
            server.run().unwrap();
        });

        let topic = Topic::new("test/ack-lifecycle", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        // Create the actual UDP transports.
        let publisher_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let subscriber_transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let publisher_address = publisher_transport.local_addr().unwrap();
        let subscriber_address = subscriber_transport.local_addr().unwrap();

        // Create discovery clients.
        let publisher_discovery =
            NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address).unwrap();
        let subscriber_discovery =
            NetworkDiscovery::bind("127.0.0.1:0".parse().unwrap(), server_address).unwrap();

        // Register both participants and their endpoints.
        let publisher_participant = Participant::new(1, publisher_address);
        let subscriber_participant = Participant::new(2, subscriber_address);

        publisher_discovery
            .register_participant(&publisher_participant)
            .unwrap();
        publisher_discovery
            .register_participant(&subscriber_participant)
            .unwrap();

        publisher_discovery
            .register_endpoint(
                publisher_participant.id(),
                topic.name(),
                EndpointKind::Publisher,
            )
            .unwrap();
        publisher_discovery
            .register_endpoint(
                subscriber_participant.id(),
                topic.name(),
                EndpointKind::Subscriber,
            )
            .unwrap();

        let mut publisher = Publisher::<TestMessage>::new(
            topic.clone(),
            publisher_discovery,
            publisher_transport,
            qos.clone(),
        )
        .unwrap();

        let mut subscriber =
            Subscriber::<TestMessage>::new(topic, subscriber_discovery, subscriber_transport, qos)
                .unwrap();

        publisher.set_nonblocking(true).unwrap();
        subscriber.set_nonblocking(true).unwrap();

        // 1. Publish through the real publisher API.
        publisher.publish(&TestMessage { value: 42 }).unwrap();

        // A reliable publication should create one pending ACK.
        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "publication should create a pending ACK"
        );

        // 2. Wait for the subscriber to receive the sample.
        let deadline = Instant::now() + Duration::from_secs(1);

        let received = loop {
            if let Some(message) = subscriber.try_receive().unwrap() {
                break Some(message);
            }

            if Instant::now() >= deadline {
                break None;
            }

            thread::yield_now();
        };

        assert_eq!(
            received,
            Some(TestMessage { value: 42 }),
            "subscriber should receive the published sample"
        );

        // 3. The subscriber has sent an ACK over UDP.
        // Process it through the publisher's real control-message path.
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut ack_processed = false;

        while Instant::now() < deadline {
            if publisher.try_receive_control().unwrap() {
                ack_processed = true;
                break;
            }

            thread::yield_now();
        }

        assert!(
            ack_processed,
            "publisher should process the subscriber's ACK"
        );

        // 4. The ACK must clear the corresponding pending state.
        assert_eq!(
            publisher.pending_ack_count(),
            0,
            "acknowledged publication should no longer be pending"
        );
    }
    #[test]
    fn nack_retransmissions_respect_maximum_attempts() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/nack-retry-limit", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 42 });

        publisher
            .pending_acks
            .entry(receiver_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now(),
                    retransmission_attempts: 0,
                },
            );

        for attempt in 1..=MAX_RETRANSMISSIONS {
            publisher
                .handle_control_message(
                    ControlMessage::Nack {
                        topic: topic.name().to_string(),
                        missing_sequences: vec![1],
                    },
                    receiver_address,
                )
                .unwrap();

            let mut buffer = [0u8; 65_535];
            let (size, _) = receiver
                .recv_from(&mut buffer)
                .expect("expected NACK-triggered retransmission");

            match decode_network_message(&buffer[..size]).unwrap() {
                NetworkMessage::Data(message) => {
                    assert_eq!(message.sequence_number, 1);
                }
                NetworkMessage::Control(_) => {
                    panic!("Expected a retransmitted data message");
                }
            }

            let pending = publisher
                .pending_acks
                .get(&receiver_address)
                .and_then(|entries| entries.get(&1))
                .expect("pending ACK should remain until retries are exhausted");

            assert_eq!(
                pending.retransmission_attempts, attempt,
                "NACK retransmissions must update the retry counter"
            );
        }

        // A NACK must not trigger another retransmission after the limit.
        publisher
            .handle_control_message(
                ControlMessage::Nack {
                    topic: topic.name().to_string(),
                    missing_sequences: vec![1],
                },
                receiver_address,
            )
            .unwrap();

        let mut buffer = [0u8; 65_535];
        let result = receiver.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut
            ),
            "NACK caused an unexpected retransmission after the retry limit"
        );

        assert_eq!(
            publisher.pending_ack_count(),
            1,
            "the retry limit should not itself remove the pending ACK"
        );
    }
    #[test]
    fn nack_cannot_reset_retry_budget_after_pending_ack_expires() {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();
        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let topic = Topic::new("test/retry-budget", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 42 });

        publisher
            .pending_acks
            .entry(receiver_address)
            .or_default()
            .insert(
                1,
                PendingAck {
                    sent_at: Instant::now() - Duration::from_secs(1),
                    retransmission_attempts: MAX_RETRANSMISSIONS,
                },
            );

        // The exhausted ACK entry should be removed without retransmission.
        publisher
            .retransmit_expired_acknowledgements(Duration::from_millis(100))
            .unwrap();

        assert_eq!(publisher.pending_ack_count(), 0);

        // A subsequent NACK must not reset the exhausted retry budget.
        publisher
            .handle_control_message(
                ControlMessage::Nack {
                    topic: topic.name().to_string(),
                    missing_sequences: vec![1],
                },
                receiver_address,
            )
            .unwrap();

        let mut buffer = [0u8; 65_535];
        let result = receiver.recv_from(&mut buffer);

        assert!(
            matches!(
                result,
                Err(ref error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut
            ),
            "NACK unexpectedly retransmitted a sample after the retry budget was exhausted"
        );
    }
    #[test]
    fn acknowledgement_clears_retry_state_after_pending_ack_is_removed() {
        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let transport = UdpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let topic = Topic::new("test/ack-retry-cleanup", "TestMessage");
        let qos = QosPolicy::new(10, Reliability::Reliable);

        let mut publisher =
            Publisher::<TestMessage>::new(topic.clone(), discovery, transport, qos).unwrap();

        let subscriber = UdpSocket::bind("127.0.0.1:0").unwrap();
        let subscriber_address = subscriber.local_addr().unwrap();

        // Simulate an exhausted retry budget whose pending ACK was already removed.
        publisher
            .nack_attempts
            .entry(subscriber_address)
            .or_default()
            .insert(1, MAX_RETRANSMISSIONS);

        // Sequence 1 has no pending ACK.
        assert!(
            publisher
                .pending_acks
                .get(&subscriber_address)
                .and_then(|sequences| sequences.get(&1))
                .is_none(),
            "sequence 1 must not have a pending ACK"
        );

        assert_eq!(
            publisher.nack_attempts[&subscriber_address][&1],
            MAX_RETRANSMISSIONS
        );

        // An ACK from the same subscriber should clear the retry state.
        publisher
            .handle_control_message(
                ControlMessage::Ack {
                    topic: topic.name().to_string(),
                    sequence_number: 1,
                },
                subscriber_address,
            )
            .unwrap();

        assert!(
            !publisher.nack_attempts.contains_key(&subscriber_address),
            "the subscriber's retry-state entry should be removed when empty"
        );

        assert!(
            publisher
                .nack_attempts
                .get(&subscriber_address)
                .and_then(|sequences| sequences.get(&1))
                .is_none(),
            "the acknowledged sequence's retry state must be removed"
        );
    }
