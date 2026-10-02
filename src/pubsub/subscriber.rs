use std::io;

use serde::de::DeserializeOwned;

use crate::{
    decode_message,
    deserialize_payload,
    NetworkDiscovery,
    Topic,
    Transport,
    UdpTransport,
    SequenceTracker
};

pub struct Subscriber<T> {
    topic: Topic,
    _discovery: NetworkDiscovery,
    transport: UdpTransport,
    sequence_tracker: SequenceTracker,
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
            sequence_tracker:SequenceTracker::new(),
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
            
            let sequence_number = message.sequence_number;

            self.sequence_tracker.observe(sequence_number);

            if sequence_number < self.sequence_tracker.last_sequence()
                && !self
                    .sequence_tracker
                    .missing_sequences()
                    .any(|missing| *missing == sequence_number)
            {
                println!(
                    "Ignoring duplicate or old message: sequence {}",
                    sequence_number
                );

                continue;
            }

            if self.sequence_tracker.has_missing() {
                println!(
                    "Missing sequence(s): {:?}",
                    self.sequence_tracker
                        .missing_sequences()
                        .copied()
                        .collect::<Vec<_>>()
                );
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