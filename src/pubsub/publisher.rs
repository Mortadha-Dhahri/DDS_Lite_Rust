use std::io;
use std::net::SocketAddr;

use crate::{
    EndpointKind,
    History,
    NetworkDiscovery,
    Topic,
    Transport,
    UdpTransport,
    WireMessage,
    serialize_payload,
    qos::QosPolicy,
    NetworkMessage,
    encode_network_message,
    ControlMessage
};

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    history: History<T>,
    next_sequence_number: u64,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: serde::Serialize + Clone,
{
    pub fn new(
        topic: Topic ,
        discovery: NetworkDiscovery,
        transport: UdpTransport,
        qos: QosPolicy,
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            discovery,
            transport,
            history: History::new(qos.history_depth()),
            next_sequence_number : 1, 
            _marker: std::marker::PhantomData,
        })
    }

    pub fn publish(&mut self, data: &T) -> io::Result<()> {
        
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self.history.push(sequence_number, data.clone());


        let subscribers = self.discovery.lookup(
            self.topic.name(),
            EndpointKind::Subscriber,
        )?;

        let payload = serialize_payload(data)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    error,
                )
            })?;

        let message = WireMessage {
            sequence_number,
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message = NetworkMessage::Data(message);

        let bytes = encode_network_message(&network_message)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    error,
                )
            })?;

        println!(
            "Publishing sequence {} to {} subscriber(s).",
            sequence_number,
            subscribers.len()
        );

        for subscriber in subscribers {
            self.transport
                .send(&bytes, subscriber.address)?;
        }

        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    fn retransmit(
        &self,
        sequence_number: u64,
        destination: SocketAddr,
    ) -> io::Result<()> {
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
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    error,
                )
            })?;

        let message = WireMessage {
            sequence_number: entry.sequence_number(),
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message =
            NetworkMessage::Data(message);

        let bytes = encode_network_message(
            &network_message,
        )
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                error,
            )
        })?;

        self.transport.send(&bytes, destination)?;

        println!(
            "Retransmitted sequence {} to {}.",
            sequence_number,
            destination
        );

        Ok(())
    }
    pub fn handle_control_message(
        &self,
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
                    sender,
                    missing_sequences
                );

                for sequence_number in missing_sequences {
                    self.retransmit(
                        sequence_number,
                        sender,
                    )?;
                }
            }
        }

        Ok(())
    }   
}