use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::{
    ControlMessage, DiscoveredEndpoint, DiscoveryServer, EndpointKind, History, NetworkDiscovery,
    NetworkMessage, Participant, QosPolicy, Reliability, Topic, Transport, UdpTransport,
    WireMessage, decode_network_message, deserialize_payload, encode_network_message,
    serialize_payload,
};

const MAX_RETRANSMISSIONS: u8 = 3;

#[derive(Debug, Clone, Copy)]
struct PendingAck {
    sent_at: Instant,
    retransmission_attempts: u8,
}

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    qos: QosPolicy,
    history: History<T>,
    next_sequence_number: u64,
    pending_acks: HashMap<SocketAddr, HashMap<u64, PendingAck>>,
    last_subscriber_refresh: Option<Instant>,
    subscriber_refresh_interval: Duration,
    subscribers: Option<Vec<DiscoveredEndpoint>>,
    _marker: PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: serde::Serialize + Clone,
{
    pub fn new(
        topic: Topic,
        discovery: NetworkDiscovery,
        transport: UdpTransport,
        qos: QosPolicy,
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            discovery,
            transport,
            qos: qos.clone(),
            history: History::new(qos.history_depth()),
            next_sequence_number: 1,
            pending_acks: HashMap::new(),
            subscribers: None,
            last_subscriber_refresh: None,
            subscriber_refresh_interval: Duration::from_secs(1),
            _marker: PhantomData,
        })
    }

    pub fn publish(&mut self, data: &T) -> io::Result<()> {
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self.history.push(sequence_number, data.clone());

        if self.subscribers.is_none() {
            // Initial discovery is required before the first publication.
            self.refresh_subscribers()?;
        } else {
            let refresh_due = self
                .last_subscriber_refresh
                .map(|last| last.elapsed() >= self.subscriber_refresh_interval)
                .unwrap_or(true);

            if refresh_due {
                // Keep using the existing cache if this refresh fails.
                let _ = self.refresh_subscribers();
            }
        }

        let subscribers = self.subscribers.as_ref().unwrap();

        let payload = serialize_payload(data)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let message = WireMessage {
            sequence_number,
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message = NetworkMessage::Data(message);

        let bytes = encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        for subscriber in subscribers {
            self.transport.send(&bytes, subscriber.address)?;

            if self.is_reliable() {
                self.pending_acks
                    .entry(subscriber.address)
                    .or_default()
                    .insert(
                        sequence_number,
                        PendingAck {
                            sent_at: Instant::now(),
                            retransmission_attempts: 0,
                        },
                    );
            }
        }

        Ok(())
    }

    pub fn retransmit(&self, sequence_number: u64, destination: SocketAddr) -> io::Result<()> {
        let entry = match self.history.find(sequence_number) {
            Some(entry) => entry,
            None => {
                println!(
                    "Cannot retransmit sequence {}: \
                     sample is no longer in history.",
                    sequence_number
                );

                return Ok(());
            }
        };

        let payload = serialize_payload(entry.data())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let message = WireMessage {
            sequence_number: entry.sequence_number(),
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message = NetworkMessage::Data(message);

        let bytes = encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.transport.send(&bytes, destination)?;

        println!(
            "Retransmitted sequence {} to {}.",
            sequence_number, destination
        );

        Ok(())
    }

    pub fn handle_control_message(
        &mut self,
        message: ControlMessage,
        sender: SocketAddr,
    ) -> io::Result<()> {
        match message {
            ControlMessage::Nack {
                topic,
                missing_sequences,
            } => {
                self.handle_nack(topic, missing_sequences, sender)?;
            }
            ControlMessage::Ack {
                topic,
                sequence_number,
            } => {
                if topic != self.topic.name() {
                    return Ok(());
                }

                println!(
                    "Received ACK from {} for sequence {}.",
                    sender, sequence_number
                );

                if let Some(pending) = self.pending_acks.get_mut(&sender) {
                    pending.remove(&sequence_number);

                    if pending.is_empty() {
                        self.pending_acks.remove(&sender);
                    }
                }
            }
            ControlMessage::Heartbeat { .. } => todo!(),
        }

        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    fn is_reliable(&self) -> bool {
        self.qos.reliability() == Reliability::Reliable
    }

    pub fn receive_control(&mut self) -> io::Result<()> {
        let (bytes, sender) = self.transport.receive()?;

        let message = crate::decode_network_message(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        match message {
            NetworkMessage::Control(control) => {
                self.handle_control_message(control, sender)?;
            }

            NetworkMessage::Data(_) => {
                println!(
                    "Publisher received an unexpected data message \
                    from {}.",
                    sender
                );
            }
        }

        Ok(())
    }

    pub fn pending_ack_count(&self) -> usize {
        self.pending_acks
            .values()
            .map(|pending| pending.len())
            .sum()
    }
    pub fn expired_acknowledgements(&self, timeout: Duration) -> Vec<(SocketAddr, u64)> {
        let now = Instant::now();

        self.pending_acks
            .iter()
            .flat_map(|(subscriber, pending)| {
                pending.iter().filter_map(move |(sequence_number, ack)| {
                    if now.duration_since(ack.sent_at) >= timeout {
                        Some((*subscriber, *sequence_number))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
    pub fn retransmit_expired_acknowledgements(&mut self, timeout: Duration) -> io::Result<()> {
        let expired = self.expired_acknowledgements(timeout);

        for (subscriber, sequence_number) in expired {
            let attempts = self
                .pending_acks
                .get(&subscriber)
                .and_then(|pending| pending.get(&sequence_number))
                .map(|ack| ack.retransmission_attempts);

            let Some(attempts) = attempts else {
                continue;
            };

            if attempts >= MAX_RETRANSMISSIONS {
                self.remove_pending_ack(subscriber, sequence_number);
                continue;
            }

            // The sample may have been evicted from publisher history.
            // Remove its pending ACK instead of retrying indefinitely.
            if self.history.find(sequence_number).is_none() {
                println!(
                    "Dropping pending ACK for sequence {} to {}: sample is no longer in history.",
                    sequence_number, subscriber
                );

                self.remove_pending_ack(subscriber, sequence_number);
                continue;
            }

            self.retransmit(sequence_number, subscriber)?;

            if let Some(ack) = self
                .pending_acks
                .get_mut(&subscriber)
                .and_then(|pending| pending.get_mut(&sequence_number))
            {
                ack.retransmission_attempts += 1;
                ack.sent_at = Instant::now();
            }
        }

        Ok(())
    }
    pub fn process_reliability(&mut self, timeout: Duration) -> io::Result<()> {
        if !self.is_reliable() {
            return Ok(());
        }

        self.retransmit_expired_acknowledgements(timeout)
    }

    fn handle_nack(
        &self,
        topic: String,
        missing_sequences: Vec<u64>,
        sender: SocketAddr,
    ) -> io::Result<()> {
        if topic != self.topic.name() {
            return Ok(());
        }

        if !self.is_reliable() {
            return Ok(());
        }

        println!(
            "Received NACK from {} for sequences: {:?}",
            sender, missing_sequences
        );

        for sequence_number in missing_sequences {
            self.retransmit(sequence_number, sender)?;
        }

        Ok(())
    }
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.transport.set_nonblocking(nonblocking)
    }
    pub fn try_receive_control(&mut self) -> io::Result<bool> {
        match self.transport.try_receive()? {
            Some((bytes, sender)) => {
                let message = decode_network_message(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

                match message {
                    NetworkMessage::Control(control) => {
                        self.handle_control_message(control, sender)?;
                        Ok(true)
                    }
                    NetworkMessage::Data(_) => Ok(false),
                }
            }
            None => Ok(false),
        }
    }
    pub fn reliability_tick(&mut self, timeout: Duration) -> io::Result<()> {
        self.process_reliability(timeout)
    }
    pub fn refresh_subscribers(&mut self) -> io::Result<()> {
        let subscribers = self
            .discovery
            .lookup(self.topic.name(), EndpointKind::Subscriber)?;

        self.subscribers = Some(subscribers);
        self.last_subscriber_refresh = Some(Instant::now());

        Ok(())
    }
    pub fn set_subscriber_refresh_interval(&mut self, interval: Duration) {
        self.subscriber_refresh_interval = interval;
    }
    fn remove_pending_ack(&mut self, subscriber: SocketAddr, sequence_number: u64) {
        if let Some(pending) = self.pending_acks.get_mut(&subscriber) {
            pending.remove(&sequence_number);

            if pending.is_empty() {
                self.pending_acks.remove(&subscriber);
            }
        }
    }
}

/*

commit: Connect the reliability timer

Publisher
    │
    └── reliability_tick()
            │
            └── process_reliability()
                    │
                    └── retransmit_expired_acknowledgements()

ParticipantRuntime
    │
    └── tick()
            │
            └── heartbeat scheduling
*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::time::Duration;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestMessage {
        value: u32,
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
}
