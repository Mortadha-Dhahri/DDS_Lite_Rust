use std::io;

use serde::de::DeserializeOwned;

use crate::{
    decode_message,
    deserialize_payload,
    NetworkDiscovery,
    Topic,
    Transport,
    UdpTransport,
};

pub struct Subscriber<T> {
    topic: Topic,
    _discovery: NetworkDiscovery,
    transport: UdpTransport,
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
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            _discovery: discovery,
            transport,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn receive(&self) -> io::Result<T> {
        loop {
            let (bytes, sender) = self.transport.receive()?;

            let message = decode_message(&bytes)
                .map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        error,
                    )
                })?;

            if message.topic != self.topic.name() {
                println!(
                    "Ignoring message for topic '{}' from {}",
                    message.topic,
                    sender
                );

                continue;
            }

            let data = deserialize_payload::<T>(
                &message.payload,
            )
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    error,
                )
            })?;

            return Ok(data);
        }
    }
}