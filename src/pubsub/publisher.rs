use std::io;
use std::net::SocketAddr;

use crate::{
    encode_message,
    serialize_payload,
    EndpointKind,
    NetworkDiscovery,
    Topic,
    Transport,
    UdpTransport,
    WireMessage,
};

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: serde::Serialize,
{
    pub fn new(
        topic: Topic,
        discovery: NetworkDiscovery,
        transport: UdpTransport,
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            discovery,
            transport,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn publish(&self, data: &T) -> io::Result<()> {
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