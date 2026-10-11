use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::{
    ControlMessage, LivelinessState, LivelinessTracker, NetworkDiscovery, NetworkMessage,
    QosPolicy, SequenceEvent, SequenceTracker, Topic, Transport, UdpTransport,
    decode_network_message, deserialize_payload,
};

pub struct Subscriber<T> {
    topic: Topic,
    _discovery: NetworkDiscovery,
    transport: UdpTransport,
    sequence_tracker: SequenceTracker,
    liveliness: LivelinessTracker,
    qos: QosPolicy,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Subscriber<T>
where
    T: DeserializeOwned,
{
    pub fn new(
        topic: Topic,
        discovery: NetworkDiscovery,
        transport: UdpTransport,
        qos: QosPolicy,
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            _discovery: discovery,
            transport,
            sequence_tracker: SequenceTracker::new(),
            liveliness: LivelinessTracker::new(qos.liveliness_timeout()),
            qos,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn receive(&mut self) -> io::Result<T> {
        loop {
            let (bytes, sender) = self.transport.receive()?;

            let network_message = match Self::decode_message(&bytes) {
                Ok(message) => message,
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    println!("Ignoring malformed packet from {}: {}", sender, error);
                    continue;
                }
                Err(error) => return Err(error),
            };

            match network_message {
                NetworkMessage::Data(message) => {
                    if let Some(data) = self.handle_data_message(message, sender)? {
                        return Ok(data);
                    }
                }
                NetworkMessage::Control(control) => {
                    self.handle_control_message(control)?;
                }
            }
        }
    }

    fn send_nack(&self, destination: &SocketAddr, missing_sequences: Vec<u64>) -> io::Result<()> {
        if missing_sequences.is_empty() {
            return Ok(());
        }

        let control_message = ControlMessage::Nack {
            topic: self.topic.name().to_string(),
            missing_sequences,
        };

        let network_message = NetworkMessage::Control(control_message);

        let bytes = crate::encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.transport.send(&bytes, *destination)?;

        println!("Sent NACK to {}.", destination);

        Ok(())
    }

    fn send_ack(&self, destination: &SocketAddr, sequence_number: u64) -> io::Result<()> {
        let control_message = ControlMessage::Ack {
            topic: self.topic.name().to_string(),
            sequence_number,
        };

        let network_message = NetworkMessage::Control(control_message);

        let bytes = crate::encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.transport.send(&bytes, *destination)?;

        Ok(())
    }

    pub fn handle_control_message(&mut self, message: ControlMessage) -> io::Result<()> {
        match message {
            ControlMessage::Heartbeat { participant_id } => {
                self.liveliness.observe(participant_id);

                println!("Received heartbeat from participant {}.", participant_id);
            }

            ControlMessage::Ack { .. } | ControlMessage::Nack { .. } => {}
        }

        Ok(())
    }
    pub fn liveliness_state(&self, participant_id: u64) -> LivelinessState {
        self.liveliness.state(participant_id)
    }
    pub fn receive_control(&mut self) -> io::Result<()> {
        let (bytes, _) = self.transport.receive()?;

        let message = decode_network_message(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        if let NetworkMessage::Control(control) = message {
            self.handle_control_message(control)?;
        }

        Ok(())
    }

    fn handle_data_message(
        &mut self,
        message: crate::WireMessage,
        sender: SocketAddr,
    ) -> io::Result<Option<T>> {
        if message.topic != self.topic.name() {
            println!(
                "Ignoring message for topic '{}' from {}",
                message.topic, sender
            );
            return Ok(None);
        }

        if message.type_name != self.topic.type_name() {
            println!(
                "Ignoring message with unsupported type '{}' for topic '{}' from {}",
                message.type_name, message.topic, sender
            );
            return Ok(None);
        }

        let sequence_number = message.sequence_number;

        match self.sequence_tracker.observe(sequence_number) {
            SequenceEvent::First => {}
            SequenceEvent::Contiguous => {}
            SequenceEvent::Gap { missing } => {
                self.send_nack(&sender, missing)?;
            }
            SequenceEvent::Late => {}
            SequenceEvent::Duplicate => {
                println!("Ignoring duplicate sequence: {}", sequence_number);

                if self.qos.reliability() == crate::Reliability::Reliable {
                    self.send_ack(&sender, sequence_number)?;
                }

                return Ok(None);
            }
        }

        let data = deserialize_payload::<T>(&message.payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        if self.qos.reliability() == crate::Reliability::Reliable {
            self.send_ack(&sender, sequence_number)?;
        }

        Ok(Some(data))
    }
    pub fn try_receive(&mut self) -> io::Result<Option<T>> {
        match self.transport.try_receive()? {
            Some((bytes, sender)) => {
                let network_message = match Self::decode_message(&bytes) {
                    Ok(message) => message,
                    Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                        println!("Ignoring malformed packet from {}: {}", sender, error);
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };

                match network_message {
                    NetworkMessage::Data(message) => self.handle_data_message(message, sender),
                    NetworkMessage::Control(control) => {
                        self.handle_control_message(control)?;
                        Ok(None)
                    }
                }
            }
            None => Ok(None),
        }
    }
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.transport.set_nonblocking(nonblocking)
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }
    fn decode_message(bytes: &[u8]) -> io::Result<NetworkMessage> {
        decode_network_message(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

#[cfg(test)]
mod tests {
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
}
