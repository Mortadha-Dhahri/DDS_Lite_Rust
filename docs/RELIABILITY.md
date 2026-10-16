# Reliable delivery: expected behavior

This document describes the behavior covered by the current implementation and tests. It is not a claim of full DDS/RTPS conformance.

## Delivery modes

- **Best effort** sends data without maintaining reliable-delivery pending-ACK state.
- **Reliable** delivery records pending acknowledgements for subscribers that receive a sample and uses control messages to coordinate acknowledgement and retransmission.

## Acknowledgements

- ACK state is keyed by subscriber address and sequence number.
- An ACK clears only the matching sequence for the sender that issued it.
- An ACK from an unknown subscriber must not clear another subscriber's pending state.
- ACK cleanup also removes retry state for the acknowledged sequence; empty per-subscriber maps are removed.
- Late or duplicate ACKs should be harmless.

## NACK and retry limits

- A NACK can request retransmission of a sample still available in publisher history.
- A missing history entry cannot be retransmitted; only that unrecoverable pending entry should be removed.
- Retransmission attempts are bounded by `MAX_RETRANSMISSIONS` (currently 3).
- The retry budget must not reset just because the pending-ACK entry expired or was removed.
- Retry state is independent per subscriber, so one subscriber reaching its limit does not consume another subscriber's budget.

## Subscriber sequence handling

- Sequence tracking is scoped by publisher UDP address and publisher session ID.
- Each publisher instance generates a unique session ID and reuses it for all published samples and retransmissions during that instance's lifetime.
- When a subscriber receives a sample from a new session at an address with an active session, it retires the previous session and activates the new one.
- Packets from retired sessions are ignored, preventing delayed packets from an old publisher instance from reactivating that session.
- Each publisher session maintains independent sequence state. A restarted publisher can therefore begin at sequence 1 without inheriting the previous session's high-water mark.
- A duplicate reliable sample is acknowledged but not delivered to the application a second time.
- Missing sequence numbers are tracked so the receiver can identify gaps and recognize when missing samples arrive.

## Defensive behavior

- A publisher accepts heartbeat control messages without panicking.
- The discovery server ignores malformed request datagrams and continues serving later requests.
- A subscriber validates the payload before updating sequence state; a corrupt payload is ignored and is not ACKed.
- Sequence-gap tracking retains at most 128 missing sequence numbers and sends at most that many in one NACK. This bounds allocation and control-datagram size, but a larger gap is only partially requested for retransmission.
- Publisher fan-out attempts every discovered subscriber even when an individual UDP send fails, then returns the first send error.
- Local discovery registration is idempotent for participant IDs and for the participant/topic/kind endpoint tuple.
- A subscriber validates the payload before updating sequence state; a corrupt payload is ignored and is not ACKed.

## Scope and limitations

The current implementation is a learning-focused DDS-inspired middleware. These tests cover selected reliability invariants, not every failure mode or interoperability requirement of DDS/RTPS. In particular, successful unit tests do not prove lossless delivery under arbitrary packet loss, process crashes, or network partitions.
Adding `publisher_session_id` changes the serialized `WireMessage` format. This implementation does not currently provide a wire-protocol version negotiation or backward-compatibility mechanism, so binaries built against different message formats may be unable to communicate correctly. Run compatible versions of the publisher and subscriber together.