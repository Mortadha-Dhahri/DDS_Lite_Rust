# DDS-Lite Rust

A learning-focused, DDS-inspired publish/subscribe middleware implemented in Rust. It explores topic-based data exchange, endpoint discovery, UDP transport, serialization, QoS, bounded history, sequence tracking, acknowledgements, retransmission, and liveliness.

> This is not a complete DDS or RTPS implementation and does not claim standards conformance or interoperability with production DDS implementations.

## Current capabilities

- Typed application payloads serialized with Serde and bincode.
- Topic-based publisher/subscriber API over UDP.
- Participant and endpoint discovery through a UDP discovery server.
- Best-effort and reliable delivery modes.
- Bounded publisher history and sequence-numbered samples.
- ACK/NACK handling and bounded retransmission attempts.
- Sequence-gap detection and duplicate suppression.
- Publisher session IDs to distinguish restarts and reject delayed packets from retired sessions.
- Participant liveliness tracking and discovery expiry.
- Unit, integration, robustness, and stress tests.
- Throughput benchmark target and runnable examples.

See [reliability behavior and limitations](docs/RELIABILITY.md).

## Architecture at a glance

```text
Application payload
       |
       v
 Publisher<T>  ---- DATA over UDP ----> Subscriber<T>
       |                                  |
       +--------- ACK / NACK / HEARTBEAT -+
       |
       +--- NetworkDiscovery <--> DiscoveryServer
                    participant / endpoint registry
```

- **Core** (`src/core/`): topics, participants, message wrappers, runtime composition.
- **Pub/sub** (`src/pubsub/`): typed publishing/receiving and reliability state.
- **Discovery** (`src/discovery/`): local registry, network client/server, protocol.
- **QoS** (`src/qos/`): reliability modes, history, sequence and liveliness tracking.
- **Serialization** (`src/serialization/`): data/control wire formats and codecs.
- **Transport** (`src/transport/`): UDP abstraction and socket implementation.

See [architecture overview](docs/architecture.md) and [architecture documentation index](docs/README.md).

## Requirements

- Stable Rust toolchain (edition 2024) and Cargo.
- No external broker is required; examples use local UDP sockets.

## Build and verify

From the repository root:

```bash
cargo build
cargo test --all-targets
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Focused checks:

```bash
cargo test --test reliable_delivery
cargo test nack_cannot_reset_retry_budget_after_pending_ack_expires -- --nocapture
```

See the [test catalog](docs/TEST_CATALOG.md), [testing guide](docs/TESTING.md), and [unit test guide](unit_tests/README.md). GitHub Actions lives in `.github/workflows/ci.yml`; hosted CI can be blocked by an account/repository billing restriction.

## Run an example

The participant example expects a discovery server at `127.0.0.1:6000`. Start a discovery server using `DiscoveryServer::bind(...)` and its server loop, then run publisher and subscriber in separate terminals:

```bash
cargo run --example participant -- 1 7001 publisher
cargo run --example participant -- 2 7002 subscriber
```

The example publishes a `VehicleState` sample from the publisher role and waits for typed samples on the subscriber role. Other examples include `network_discovery.rs`, `reliable_publisher.rs`, and `ack_timeout.rs`.

The publisher-session ID is part of the serialized wire message. No protocol-version negotiation is implemented; publisher and subscriber binaries should use compatible revisions.

## Repository layout

```text
src/
  core/           Topic, participant, message, runtime
  discovery/      Registry, UDP client/server, protocol
  pubsub/         Publisher and subscriber state machines
  qos/            Reliability, history, sequence, liveliness
  serialization/  Wire messages and codecs
  transport/      Transport trait and UDP implementation
unit_tests/       Behavior-focused unit tests
tests/            Cross-component UDP integration/stress tests
examples/         Runnable demonstrations
benches/          Throughput benchmark
docs/             API, testing, reliability, architecture docs
```

## Documentation

- [API reference](docs/API_REFERENCE.md) — public APIs, component responsibilities, errors, and call flows.
- [Test catalog](docs/TEST_CATALOG.md) — named tests and the invariants they protect.
- [Architecture overview](docs/architecture.md) and [architecture diagrams](docs/README.md).
- [Testing guide](docs/TESTING.md).
- [Reliability behavior and limitations](docs/RELIABILITY.md).
- [Unit test guide](unit_tests/README.md).

## Design constraints

- UDP itself is unreliable; recovery is limited to implemented ACK/NACK, history, and retry mechanisms.
- Bounded history means evicted samples cannot be retransmitted.
- Sequence-gap tracking is capped at 128 missing sequence numbers.
- Retry attempts are bounded and do not imply eventual delivery.
- Discovery/liveliness are simplified timeout-based mechanisms.
- The wire format has no version negotiation or backward-compatibility mechanism.
- Full DDS/RTPS interoperability, security, broad QoS coverage, and ROS 2 integration are outside the current scope.

## License

MIT — see [LICENSE](LICENSE).
