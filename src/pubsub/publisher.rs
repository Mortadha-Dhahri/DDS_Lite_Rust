use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::qos::sequence::MAX_TRACKED_GAP;

use crate::{
    ControlMessage, DiscoveredEndpoint, DiscoveryServer, EndpointKind, History, NetworkDiscovery,
    NetworkMessage, Participant, QosPolicy, Reliability, Subscriber, Topic, Transport,
    UdpTransport, WireMessage, decode_network_message, deserialize_payload, encode_network_message,
    serialize_payload,
};

const MAX_RETRANSMISSIONS: u8 = 3;

#[derive(Debug, Clone, Copy)]
struct PendingAck {
    sent_at: Instant,
    retransmission_attempts: u8,
}

pub struct Publisher<T> {
    topic: Topic,
    discovery: NetworkDiscovery,
    transport: UdpTransport,
    qos: QosPolicy,
    history: History<T>,
    next_sequence_number: u64,
    pending_acks: HashMap<SocketAddr, HashMap<u64, PendingAck>>,
    nack_attempts: HashMap<SocketAddr, HashMap<u64, u8>>,
    last_subscriber_refresh: Option<Instant>,
    subscriber_refresh_interval: Duration,
    subscribers: Option<Vec<DiscoveredEndpoint>>,
    _marker: PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: serde::Serialize + Clone,
{
    pub fn new(
        topic: Topic,
        discovery: NetworkDiscovery,
        transport: UdpTransport,
        qos: QosPolicy,
    ) -> io::Result<Self> {
        Ok(Self {
            topic,
            discovery,
            transport,
            qos: qos.clone(),
            history: History::new(qos.history_depth()),
            next_sequence_number: 1,
            pending_acks: HashMap::new(),
            nack_attempts: HashMap::new(),
            subscribers: None,
            last_subscriber_refresh: None,
            subscriber_refresh_interval: Duration::from_secs(1),
            _marker: PhantomData,
        })
    }

    pub fn publish(&mut self, data: &T) -> io::Result<()> {
        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self.history.push(sequence_number, data.clone());

        if self.subscribers.is_none() {
            // Initial discovery is required before the first publication.
            self.refresh_subscribers()?;
        } else {
            let refresh_due = self
                .last_subscriber_refresh
                .map(|last| last.elapsed() >= self.subscriber_refresh_interval)
                .unwrap_or(true);

            if refresh_due {
                // Keep using the existing cache if this refresh fails.
                let _ = self.refresh_subscribers();
            }
        }

        let subscribers = self.subscribers.as_ref().unwrap();

        let payload = serialize_payload(data)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let message = WireMessage {
            sequence_number,
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message = NetworkMessage::Data(message);

        let bytes = encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let mut first_send_error: Option<io::Error> = None;
        for subscriber in subscribers {
            match self.transport.send(&bytes, subscriber.address) {
                Ok(_) => {
                    if self.is_reliable() {
                        self.pending_acks
                            .entry(subscriber.address)
                            .or_default()
                            .insert(
                                sequence_number,
                                PendingAck {
                                    sent_at: Instant::now(),
                                    retransmission_attempts: 0,
                                },
                            );
                    }
                }
                Err(error) => {
                    // Continue fan-out even if one destination fails. Preserve the
                    // first error and return it after attempting all subscribers.
                    if first_send_error.is_none() {
                        first_send_error = Some(error);
                    }
                }
            }
        }

        if let Some(error) = first_send_error {
            return Err(error);
        }

        Ok(())
    }

    pub fn retransmit(&self, sequence_number: u64, destination: SocketAddr) -> io::Result<()> {
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
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let message = WireMessage {
            sequence_number: entry.sequence_number(),
            topic: self.topic.name().to_string(),
            type_name: self.topic.type_name().to_string(),
            payload,
        };

        let network_message = NetworkMessage::Data(message);

        let bytes = encode_network_message(&network_message)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.transport.send(&bytes, destination)?;

        println!(
            "Retransmitted sequence {} to {}.",
            sequence_number, destination
        );

        Ok(())
    }

    pub fn handle_control_message(
        &mut self,
        message: ControlMessage,
        sender: SocketAddr,
    ) -> io::Result<()> {
        match message {
            ControlMessage::Nack {
                topic,
                missing_sequences,
            } => {
                self.handle_nack(topic, missing_sequences, sender)?;
            }
            ControlMessage::Ack {
                topic,
                sequence_number,
            } => {
                if topic != self.topic.name() {
                    return Ok(());
                }

                println!(
                    "Received ACK from {} for sequence {}.",
                    sender, sequence_number
                );

                if let Some(pending) = self.pending_acks.get_mut(&sender) {
                    pending.remove(&sequence_number);

                    if pending.is_empty() {
                        self.pending_acks.remove(&sender);
                    }
                }
                if let Some(sequences) = self.nack_attempts.get_mut(&sender) {
                    sequences.remove(&sequence_number);

                    if sequences.is_empty() {
                        self.nack_attempts.remove(&sender);
                    }
                }
            }
            ControlMessage::Heartbeat { participant_id } => {
                // A heartbeat is valid traffic on this UDP socket; it must not panic
                // the publisher control loop.
                println!(
                    "Received heartbeat from participant {} at {}.",
                    participant_id, sender
                );
            }
        }

        Ok(())
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    fn is_reliable(&self) -> bool {
        self.qos.reliability() == Reliability::Reliable
    }

    pub fn receive_control(&mut self) -> io::Result<()> {
        let (bytes, sender) = self.transport.receive()?;

        let message = crate::decode_network_message(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        match message {
            NetworkMessage::Control(control) => {
                self.handle_control_message(control, sender)?;
            }

            NetworkMessage::Data(_) => {
                println!(
                    "Publisher received an unexpected data message \
                    from {}.",
                    sender
                );
            }
        }

        Ok(())
    }

    pub fn pending_ack_count(&self) -> usize {
        self.pending_acks
            .values()
            .map(|pending| pending.len())
            .sum()
    }
    pub fn expired_acknowledgements(&self, timeout: Duration) -> Vec<(SocketAddr, u64)> {
        let now = Instant::now();

        self.pending_acks
            .iter()
            .flat_map(|(subscriber, pending)| {
                pending.iter().filter_map(move |(sequence_number, ack)| {
                    if now.duration_since(ack.sent_at) >= timeout {
                        Some((*subscriber, *sequence_number))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
    pub fn retransmit_expired_acknowledgements(&mut self, timeout: Duration) -> io::Result<()> {
        let expired = self.expired_acknowledgements(timeout);

        for (subscriber, sequence_number) in expired {
            let attempts = self
                .pending_acks
                .get(&subscriber)
                .and_then(|pending| pending.get(&sequence_number))
                .map(|ack| ack.retransmission_attempts);

            let Some(attempts) = attempts else {
                continue;
            };

            if attempts >= MAX_RETRANSMISSIONS {
                self.nack_attempts
                    .entry(subscriber)
                    .or_default()
                    .insert(sequence_number, attempts);

                self.remove_pending_ack(subscriber, sequence_number);
                continue;
            }
            // The sample may have been evicted from publisher history.
            // Remove its pending ACK instead of retrying indefinitely.
            if self.history.find(sequence_number).is_none() {
                println!(
                    "Dropping pending ACK for sequence {} to {}: sample is no longer in history.",
                    sequence_number, subscriber
                );

                self.remove_pending_ack(subscriber, sequence_number);
                continue;
            }

            self.retransmit(sequence_number, subscriber)?;

            if let Some(ack) = self
                .pending_acks
                .get_mut(&subscriber)
                .and_then(|pending| pending.get_mut(&sequence_number))
            {
                ack.retransmission_attempts += 1;
                ack.sent_at = Instant::now();
            }
        }

        Ok(())
    }
    pub fn process_reliability(&mut self, timeout: Duration) -> io::Result<()> {
        if !self.is_reliable() {
            return Ok(());
        }

        self.retransmit_expired_acknowledgements(timeout)
    }

    fn handle_nack(
        &mut self,
        topic: String,
        missing_sequences: Vec<u64>,
        sender: SocketAddr,
    ) -> io::Result<()> {
        if topic != self.topic.name() || !self.is_reliable() {
            return Ok(());
        }

        println!(
            "Received NACK from {} for sequences: {:?}",
            sender, missing_sequences
        );

        for sequence_number in missing_sequences.into_iter().take(MAX_TRACKED_GAP as usize) {
            if self.history.find(sequence_number).is_none() {
                continue;
            }

            let pending_attempts = self
                .pending_acks
                .get(&sender)
                .and_then(|pending| pending.get(&sequence_number))
                .map(|ack| ack.retransmission_attempts);

            if let Some(attempts) = pending_attempts {
                if attempts >= MAX_RETRANSMISSIONS {
                    continue;
                }

                self.retransmit(sequence_number, sender)?;

                if let Some(ack) = self
                    .pending_acks
                    .get_mut(&sender)
                    .and_then(|pending| pending.get_mut(&sequence_number))
                {
                    ack.retransmission_attempts += 1;
                    ack.sent_at = Instant::now();
                }
            } else {
                let attempts = self
                    .nack_attempts
                    .get(&sender)
                    .and_then(|sequences| sequences.get(&sequence_number))
                    .copied()
                    .unwrap_or(0);

                if attempts >= MAX_RETRANSMISSIONS {
                    continue;
                }

                self.retransmit(sequence_number, sender)?;

                self.nack_attempts
                    .entry(sender)
                    .or_default()
                    .insert(sequence_number, attempts + 1);
            }
        }

        Ok(())
    }
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.transport.set_nonblocking(nonblocking)
    }
    pub fn try_receive_control(&mut self) -> io::Result<bool> {
        match self.transport.try_receive()? {
            Some((bytes, sender)) => {
                let message = decode_network_message(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

                match message {
                    NetworkMessage::Control(control) => {
                        self.handle_control_message(control, sender)?;
                        Ok(true)
                    }
                    NetworkMessage::Data(_) => Ok(false),
                }
            }
            None => Ok(false),
        }
    }
    pub fn reliability_tick(&mut self, timeout: Duration) -> io::Result<()> {
        self.process_reliability(timeout)
    }
    pub fn refresh_subscribers(&mut self) -> io::Result<()> {
        let subscribers = self
            .discovery
            .lookup(self.topic.name(), EndpointKind::Subscriber)?;

        self.subscribers = Some(subscribers);
        self.last_subscriber_refresh = Some(Instant::now());

        Ok(())
    }
    pub fn set_subscriber_refresh_interval(&mut self, interval: Duration) {
        self.subscriber_refresh_interval = interval;
    }
    fn remove_pending_ack(&mut self, subscriber: SocketAddr, sequence_number: u64) {
        if let Some(pending) = self.pending_acks.get_mut(&subscriber) {
            pending.remove(&sequence_number);

            if pending.is_empty() {
                self.pending_acks.remove(&subscriber);
            }
        }
    }
}

/*

commit: Connect the reliability timer

Publisher
    │
    └── reliability_tick()
            │
            └── process_reliability()
                    │
                    └── retransmit_expired_acknowledgements()

ParticipantRuntime
    │
    └── tick()
            │
            └── heartbeat scheduling
*/

#[cfg(test)]
#[path = "../../unit_tests/reliability/retry_and_acknowledgement.rs"]
mod reliability_tests;
