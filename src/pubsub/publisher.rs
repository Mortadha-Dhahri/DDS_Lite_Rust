use std::io;
use std::net::SocketAddr;

use crate::{
    EndpointKind, History, NetworkDiscovery, Topic, Transport, UdpTransport, WireMessage, encode_message , serialize_payload,qos::QosPolicy
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

        let bytes = encode_message(&message)
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
}