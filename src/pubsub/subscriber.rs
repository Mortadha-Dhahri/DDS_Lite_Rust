use std::io;
use std::net::SocketAddr;

use serde::de::DeserializeOwned;

use crate::{
    decode_network_message,
    deserialize_payload,
    ControlMessage,
    NetworkDiscovery,
    NetworkMessage,
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

            let network_message =
                decode_network_message(&bytes)
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            error,
                        )
                    })?;

            let message = match network_message {
                NetworkMessage::Data(message) => message,

                NetworkMessage::Control(_) => {
                    println!(
                        "Ignoring control message from {}",
                        sender
                    );

                    continue;
                }
            };

            if message.topic != self.topic.name() {
                println!(
                    "Ignoring message for topic '{}' from {}",
                    message.topic,
                    sender
                );

                continue;
            }

            let sequence_number =
                message.sequence_number;

            match self
                .sequence_tracker
                .observe(sequence_number)
            {
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
                        "Gap detected at sequence {}. \
                         Missing: {:?}",
                        sequence_number,
                        missing
                    );

                    self.send_nack(
                        &sender,
                        missing,
                    )?;
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

            let data =
                deserialize_payload::<T>(
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
        fn send_nack(
            &self,
            destination: &SocketAddr,
            missing_sequences: Vec<u64>,
        ) -> io::Result<()> {
            if missing_sequences.is_empty() {
                return Ok(());
            }

            let control_message = ControlMessage::Nack {
                topic: self.topic.name().to_string(),
                missing_sequences,
            };

            let network_message =
                NetworkMessage::Control(control_message);

            let bytes =
                crate::encode_network_message(&network_message)
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            error,
                        )
                    })?;

            self.transport.send(
                &bytes,
                *destination,
            )?;

            println!(
                "Sent NACK to {}.",
                destination
            );

            Ok(())
        }
}