# Reliable delivery state machine

## Modes

**Best effort** sends samples without pending-ACK state and does not retransmit due to ACK timeout or NACK. **Reliable** tracks outstanding deliveries and processes ACK/NACK control messages.

## Sender state

Publisher tracks pending acknowledgements and retry state per subscriber/sequence. An ACK must match sender address and sequence. A matching ACK clears pending and retry state; unknown or duplicate ACKs must not affect another subscriber.

A NACK asks for retransmission of samples still in bounded history. Retry attempts are limited by `MAX_RETRANSMISSIONS` (currently 3). Retry counts must not reset merely because a pending-ACK entry was removed. Samples no longer in history cannot be retransmitted.

## Receiver state

Subscriber validates payload bytes before committing sequence progress. Tracking is isolated by publisher UDP address and session ID.

- New contiguous data advances the high-water mark.
- A forward jump identifies missing sequences.
- A late sample can fill a tracked gap.
- A duplicate reliable sample is ACKed but not delivered twice.
- A corrupt payload is ignored and must not advance sequence state.
- A new session retires the old one; delayed data from retired sessions is ignored.

Missing-sequence state and NACK lists are capped at 128 entries. This bounds memory and datagram size, but larger gaps may only be partially requested.

## Control messages

- **ACK:** confirms receipt for a sequence/topic under the custom wire format.
- **NACK:** requests specific missing sequence numbers.
- **Heartbeat:** refreshes peer liveliness.

These are simplified custom messages, not RTPS control submessages.

## Tests

- `unit_tests/reliability/retry_and_acknowledgement.rs`: ACK isolation, late/duplicate ACKs, retry exhaustion, missing history, best-effort, heartbeat, fan-out errors.
- `unit_tests/reliability/receive_semantics.rs`: duplicate suppression, publisher isolation, corrupt payload, heartbeat, session semantics.
- `unit_tests/qos/sequence_tracking.rs`: classification and bounded gap tracking.
- `tests/reliable_delivery.rs` and `tests/stress.rs`: end-to-end UDP recovery.

## Limitations

Reliability here means bounded recovery for selected cases, not guaranteed delivery under arbitrary loss, process crashes, or partitions. The wire format has no version negotiation; peers must use compatible revisions.
