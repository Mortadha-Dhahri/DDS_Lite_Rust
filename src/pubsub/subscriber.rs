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

            let network_message = decode_network_message(&bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

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

        println!(
            "Sent ACK for sequence {} to {}.",
            sequence_number, destination
        );

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

        let sequence_number = message.sequence_number;

        match self.sequence_tracker.observe(sequence_number) {
            SequenceEvent::First => {
                println!("Received first sequence: {}", sequence_number);
            }
            SequenceEvent::Contiguous => {
                println!("Received sequence: {}", sequence_number);
            }
            SequenceEvent::Gap { missing } => {
                println!(
                    "Gap detected at sequence {}. Missing: {:?}",
                    sequence_number, missing
                );
                self.send_nack(&sender, missing)?;
            }
            SequenceEvent::Late => {
                println!("Received late sequence: {}", sequence_number);
            }
            SequenceEvent::Duplicate => {
                println!("Ignoring duplicate sequence: {}", sequence_number);
                return Ok(None);
            }
        }

        let data = deserialize_payload::<T>(&message.payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.send_ack(&sender, sequence_number)?;

        Ok(Some(data))
    }
    pub fn try_receive(&mut self) -> io::Result<Option<T>> {
        match self.transport.try_receive()? {
            Some((bytes, sender)) => {
                let network_message = decode_network_message(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

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
}
