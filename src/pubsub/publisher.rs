use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::qos::reliability;
use crate::{
    ControlMessage, DiscoveryServer, EndpointKind, History, NetworkDiscovery, NetworkMessage,
    Participant, QosPolicy, Reliability, Topic, Transport, UdpTransport, WireMessage,
    decode_network_message, deserialize_payload, encode_network_message, serialize_payload,
};

use std::collections::HashMap;

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    qos: QosPolicy,
    history: History<T>,
    next_sequence_number: u64,
    pending_acks: HashMap<SocketAddr, HashMap<u64, Instant>>,
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
            _marker: std::marker::PhantomData,
        })
    }

    pub fn publish(&mut self, data: &T) -> io::Result<()> {
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self.history.push(sequence_number, data.clone());

        let subscribers = self
            .discovery
            .lookup(self.topic.name(), EndpointKind::Subscriber)?;

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

        println!(
            "Publishing sequence {} to {} subscriber(s).",
            sequence_number,
            subscribers.len()
        );

        for subscriber in subscribers {
            self.transport.send(&bytes, subscriber.address)?;

            if self.is_reliable() {
                self.pending_acks
                    .entry(subscriber.address)
                    .or_default()
                    .insert(sequence_number, Instant::now());
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
                self.handle_nack(topic,missing_sequences,sender)?;
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
                pending
                    .iter()
                    .filter_map(move |(sequence_number, sent_at)| {
                        if now.duration_since(*sent_at) >= timeout {
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
            self.retransmit(sequence_number, subscriber)?;
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
}

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
            .insert(1, Instant::now() - Duration::from_secs(1));

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
}
