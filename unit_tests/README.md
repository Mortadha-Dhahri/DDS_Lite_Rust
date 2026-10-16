# Unit test suite

Unit tests live here and are grouped by **the behavior they verify**, not by the production source file that contains the test-module declaration.

## Test groups and expected behavior

| Test file | Expected behavior covered |
|---|---|
| `reliability/retry_and_acknowledgement.rs` | Reliable publishers track pending ACKs per subscriber and sequence; ACKs only clear the matching subscriber/sequence state; unknown, late, and duplicate ACKs do not corrupt other state; NACKs trigger eligible retransmissions; retries stop at the configured limit; unavailable history entries do not block other recoverable samples; best-effort mode does not track reliable ACK state. |
| `reliability/receive_semantics.rs` | Subscribers handle control messages, ACK reliable duplicate data without redelivering it, track sequence state independently per publisher, and update participant liveliness on heartbeats. |
| `discovery/endpoint_registry.rs` | Local discovery registers and looks up endpoints by topic and endpoint kind, and participant cleanup does not remove unrelated endpoints. |
| `discovery/server_lifecycle.rs` | The discovery server registers participants/endpoints and removes expired or unregistered participant state without affecting other participants. |
| `qos/history_capacity.rs` | History respects its configured depth and evicts the oldest entries; evicted sequence numbers are no longer available for retransmission. |
| `qos/liveliness_expiry.rs` | Observed participants remain live until their timeout expires, after which expired participant IDs are reported. |
| `qos/sequence_tracking.rs` | Sequence tracking distinguishes new, duplicate, and out-of-order samples and reports gaps until missing sequence numbers arrive. |
| `qos/policy_defaults.rs` | QoS policy accessors expose the configured history depth, reliability mode, and liveliness timeout. |
| `serialization/control_message_roundtrip.rs` | Control messages can be encoded and decoded without changing their variant or fields. |

The end-to-end reliable delivery scenario remains under `tests/reliable_delivery.rs`. It exercises communication across real UDP sockets and the discovery service rather than testing a single module in isolation.

## Run tests

From the repository root:

```bash
# All unit and integration tests
cargo test

# One behavior-focused test module
cargo test --lib retry_and_acknowledgement
cargo test --lib receive_semantics
cargo test --lib sequence_tracking

# One named test, with output visible
cargo test nack_cannot_reset_retry_budget_after_pending_ack_expires -- --nocapture

# Integration tests only
cargo test --test reliable_delivery

# Formatting and lint checks
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Cargo still discovers these as unit tests because each implementation module includes its behavior-focused file using `#[cfg(test)]` and `#[path]`. This preserves access to private state for invariant tests; do not make private fields or methods public solely to make tests compile.

## Adding a test

1. Choose the behavior being verified and place the test in the matching feature group.
2. Keep assertions focused on observable behavior or a clearly stated internal invariant.
3. For network-level behavior involving publisher, subscriber, and discovery interactions, prefer an integration test under `tests/`.
4. Run `cargo fmt --check`, `cargo test`, and Clippy before opening a PR.
5. If a test needs private implementation access, attach it as a child test module from the relevant source module; do not widen production visibility just for tests.
