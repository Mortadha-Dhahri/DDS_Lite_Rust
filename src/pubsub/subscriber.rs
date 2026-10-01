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
    last_sequence_number: u64,
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
            last_sequence_number:0,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn receive(&mut self) -> io::Result<T> {
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
            

            if message.sequence_number <= self.last_sequence_number {
                println!(
                    "Ignoring duplicate or out-of-order message: sequence {}",
                    message.sequence_number
                );

                continue;
            }

            self.last_sequence_number = message.sequence_number;

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