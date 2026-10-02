use std::io;

use serde::de::DeserializeOwned;

use crate::{
    decode_message,
    deserialize_payload,
    NetworkDiscovery,
    SequenceEvent,
    SequenceTracker,
    Topic,
    Transport,
    UdpTransport,
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
            sequence_tracker: SequenceTracker::new(),
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

            match self.sequence_tracker.observe(sequence_number) {
                SequenceEvent::First => {
                    println!(
                        "Received first sequence: {}",
                        sequence_number
                    );
                }

                SequenceEvent::Contiguous => {
                    println!(
                        "Received sequence: {}",
                        sequence_number
                    );
                }

                SequenceEvent::Gap { missing } => {
                    println!(
                        "Gap detected at sequence {}. Missing: {:?}",
                        sequence_number,
                        missing
                    );
                }

                SequenceEvent::Late => {
                    println!(
                        "Received late sequence: {}",
                        sequence_number
                    );
                }

                SequenceEvent::Duplicate => {
                    println!(
                        "Ignoring duplicate sequence: {}",
                        sequence_number
                    );

                    continue;
                }
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