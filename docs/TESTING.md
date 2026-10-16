# Testing guide

## Test layers

- **Unit tests** under `unit_tests/` verify focused behaviors and invariants: ACK/NACK state transitions, retransmission limits, sequence-gap tracking, liveliness expiry, discovery lifecycle, QoS policy, and control-message encoding.
- **Integration tests** under `tests/` verify multi-component behavior over sockets. The reliable-delivery test uses UDP transport and the discovery service to exercise the publisher/subscriber path.
- **Benchmarks** are configured separately in `Cargo.toml`; they are not part of the ordinary test suite.

## Commands

Run from the repository root:

```bash
cargo test
cargo test --lib
cargo test --test reliable_delivery
cargo test -- --nocapture
cargo test <substring-of-test-name>
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

For example, to check the regression that prevents an exhausted NACK retry budget from restarting after pending-ACK cleanup:

```bash
cargo test nack_cannot_reset_retry_budget_after_pending_ack_expires -- --nocapture
```

To run a feature-focused unit-test module:

```bash
cargo test --lib retry_and_acknowledgement
cargo test --lib receive_semantics
cargo test --lib endpoint_registry
cargo test --lib sequence_tracking
```

## Interpreting failures

- A unit-test failure usually points to a local state invariant or a single component's behavior.
- An integration-test failure can indicate socket setup, discovery registration, timing, or an interaction between components.
- Some network tests use local UDP ports and timing delays. If a test fails intermittently, rerun the exact failing test and inspect its output before changing production logic.
- A green test suite does not establish conformance with the full DDS/RTPS specifications; this repository intentionally implements a simplified DDS-inspired subset.
