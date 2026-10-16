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

- Sequence tracking is scoped per publisher.
- A duplicate reliable sample is acknowledged but not delivered to the application a second time.
- Missing sequence numbers are tracked so the receiver can identify gaps and recognize when missing samples arrive.

## Scope and limitations

The current implementation is a learning-focused DDS-inspired middleware. These tests cover selected reliability invariants, not every failure mode or interoperability requirement of DDS/RTPS. In particular, successful unit tests do not prove lossless delivery under arbitrary packet loss, process crashes, or network partitions.
