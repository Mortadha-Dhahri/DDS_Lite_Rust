use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::{
    ControlMessage, EndpointKind, History, NetworkDiscovery, NetworkMessage, Participant,
    QosPolicy, Reliability, Topic, Transport, UdpTransport, WireMessage, decode_network_message,
    deserialize_payload, encode_network_message, serialize_payload,
};

use std::collections::HashMap;

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    history: History<T>,
    next_sequence_number: u64,
    pending_acks: HashMap<SocketAddr, HashMap<u64, Instant>>,
    _marker: std::marker::PhantomData<T>,
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

            self.pending_acks
                .entry(subscriber.address)
                .or_default()
                .insert(sequence_number, Instant::now());
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
                if topic != self.topic.name() {
                    return Ok(());
                }

                println!(
                    "Received NACK from {} for sequences: {:?}",
                    sender, missing_sequences
                );

                for sequence_number in missing_sequences {
                    self.retransmit(sequence_number, sender)?;
                }
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
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let receiver_address = receiver.local_addr().unwrap();

        let publisher_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let publisher_address = publisher_socket.local_addr().unwrap();
        drop(publisher_socket);

        let discovery = NetworkDiscovery::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:6000".parse().unwrap(),
        )
        .unwrap();

        let topic = Topic::new("test/topic", "TestMessage");

        let qos = QosPolicy::new(10, Reliability::Reliable);

        let transport = UdpTransport::bind(publisher_address).unwrap();

        let mut publisher =
            Publisher::<TestMessage>::new(topic, discovery, transport, qos).unwrap();

        publisher.history.push(1, TestMessage { value: 42 });

        publisher
            .pending_acks
            .entry(receiver_address)
            .or_default()
            .insert(1, Instant::now() - Duration::from_secs(1));

        publisher
            .retransmit_expired_acknowledgements(Duration::from_millis(100))
            .unwrap();

        let mut buffer = vec![0u8; 65_535];

        let (size, sender) = receiver.recv_from(&mut buffer).unwrap();

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
}
