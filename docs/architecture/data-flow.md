# Data flow: publish and receive

## Normal path

1. Application passes a serializable value to `Publisher<T>::publish`.
2. Publisher serializes it and creates a `WireMessage` containing topic, sequence number, publisher session ID, and payload bytes.
3. The network envelope is encoded and the sample is inserted into bounded history.
4. Discovery lookup identifies subscribers for the topic and endpoint kind.
5. Publisher sends the datagram directly to each discovered subscriber. Reliable mode records pending ACK state.
6. Subscriber receives and decodes `NetworkMessage`, filters topic, and deserializes the payload.
7. Subscriber validates publisher session and observes sequence. A duplicate reliable sample is ACKed but not delivered twice. Gaps may produce a NACK.
8. Decoded value is returned to the application.
9. ACK control traffic clears the matching publisher pending state.

## Failure paths

- **Invalid bytes/payload:** decoding fails; sequence state must not advance and invalid data must not be ACKed.
- **Duplicate:** not delivered twice; reliable duplicate handling ACKs again so retries can stop.
- **Gap:** missing sequences are tracked and requested through bounded NACK state.
- **Missing history:** publisher cannot retransmit an evicted sample; only the unrecoverable pending entry is cleaned up.
- **Send error during fan-out:** attempts continue for other subscribers, then the first send error is returned.
- **New publisher session:** a new session gets independent sequence state; packets from a retired session are ignored.

## Guarantees and non-guarantees

This is not a durable queue. Arbitrary packet loss, network partitions, process crashes, incompatible wire formats, and gaps larger than the NACK limit are not guaranteed to recover.
