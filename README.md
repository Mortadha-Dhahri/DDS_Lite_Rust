# DDS-Lite Rust

A learning-focused, DDS-inspired publish/subscribe middleware implemented in Rust. The project explores typed topic-based communication over UDP, discovery, QoS, bounded history, sequence tracking, acknowledgements, retransmission, and liveliness.

> **Scope:** This is not a complete DDS or RTPS implementation. It does not claim DDS/RTPS standards conformance or interoperability with production DDS implementations.

## Current status

The project has a working synchronous UDP pub/sub path, a centralized discovery service, best-effort and reliable delivery paths, bounded publisher history, ACK/NACK handling, retransmission limits, sequence-gap tracking, publisher-session tracking, liveliness expiry, and a cooperative nonblocking endpoint runtime. Unit and integration tests exercise these components, including managed publisher/subscriber DATA+ACK exchange.

The implementation remains intentionally small and educational. Important limitations—centralized discovery, explicit polling, a versionless bincode wire format, and bounded reliability behavior—are documented below.

## Quick start

Requires a stable Rust toolchain with Cargo (Rust edition 2024).

Clone and enter the repository:

```bash
git clone https://github.com/Mortadha-Dhahri/DDS_Lite_Rust.git
cd DDS_Lite_Rust
```

### 1. Start the discovery server

In **Terminal 1**, from the repository root:

```bash
cargo run --example discovery_server
```

The server listens on `127.0.0.1:6000` by default. Keep this process running.

### 2. Start the subscriber

In **Terminal 2**, from the same repository:

```bash
cargo run --example participant -- 2 7002 subscriber
```

Wait until the subscriber prints that it is waiting for messages.

### 3. Start the publisher

In **Terminal 3**:

```bash
cargo run --example participant -- 1 7001 publisher
```

The example publishes one `VehicleState` sample on `vehicle/state`. Starting the subscriber first matters because this example publishes immediately after its initial discovery lookup; it does not continuously republish the sample.

All three processes use loopback addresses and are intended for a single-machine demo. The participant example hardcodes the discovery server address and currently binds participant UDP sockets to `127.0.0.1`.

### Build and test

```bash
cargo build --all-targets
cargo test --all-targets
```

For reliability-focused coverage:

```bash
cargo test --test reliable_delivery
cargo test --test endpoint_runtime
```

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

- **Core** (`src/core/`): topics, participants, and runtime composition.
- **Pub/sub** (`src/pubsub/`): typed publishing/receiving and reliability state.
- **Discovery** (`src/discovery/`): local registry, network client/server, and protocol.
- **QoS** (`src/qos/`): reliability modes, history, sequence and liveliness tracking.
- **Serialization** (`src/serialization/`): data/control wire formats and codecs.
- **Transport** (`src/transport/`): UDP abstraction and socket implementation.

## Runtime and threading model

The public API includes blocking receive methods and explicit timer/polling methods. The basic participant example calls methods such as `receive()` or `receive_control()` in its own loop; callers are responsible for scheduling heartbeat and reliability work where applicable.

`EndpointRuntime<T>` is an additive **cooperative polling** API, not a background executor: the application must keep calling `poll_once()` for receive and timer work to progress. It owns the endpoint while polling so there is one receive owner per socket. Polling is nonblocking, but `Publisher::publish()` may perform synchronous discovery I/O, particularly on first publication or when a refresh is due. Avoid placing that call on a latency-sensitive loop without accounting for the possible delay.

There is no Tokio or `mio` runtime integration at present. An asynchronous or readiness-driven runtime could be explored later.

## Deliberate simplifications and limitations

- **Centralized discovery:** participants register and look up endpoints through one UDP `DiscoveryServer`, rather than DDS-style multicast discovery. This keeps the project easy to inspect and test, but makes the discovery server a single point of failure. Existing data-plane endpoints may still have cached peer addresses, but discovery registration and fresh lookups depend on the server.
- **UDP transport:** UDP can lose, reorder, or duplicate datagrams. ACK/NACK and bounded retransmission recover some cases, not every failure mode.
- **Bounded history and retries:** samples evicted from history cannot be retransmitted; retry attempts are capped and do not guarantee eventual delivery.
- **Wire compatibility:** payloads and protocol messages use Serde with bincode 1.x. The current wire format has no protocol-version field, negotiation, or backward-compatibility guarantee. Publisher and subscriber binaries should use compatible revisions. Evaluating a maintained serialization option or bincode 2 is future work; changing the format requires explicit compatibility tests and a migration plan.
- **Polling and latency:** discovery refresh may occur synchronously inside `publish()`, and receive/timer work in the cooperative runtime only progresses while the caller polls.
- **Network addressing:** the participant example binds to loopback by default, so it is a single-host demo without code/configuration changes. It is not ready for multi-host deployment as-is.
- **Security and conformance:** authentication, encryption, comprehensive QoS, protocol negotiation, and full DDS/RTPS interoperability are out of scope.

## Demos

Browse the [demo guide](demo/README.md) for runnable examples covering basic pub/sub, discovery, reliable delivery, missing ACKs, runtime behavior, robustness tests, and the throughput benchmark.

## Documentation

Start at the [documentation index](docs/README.md).

- [Layered architecture](docs/architecture.md)
- [Component architecture](docs/architecture/components.md)
- [Data flow](docs/architecture/data-flow.md)
- [Discovery lifecycle](docs/architecture/discovery.md)
- [Reliable delivery](docs/architecture/reliable-delivery.md)
- [Sequence diagrams](docs/architecture/sequence-diagrams.md)
- [API reference](docs/API_REFERENCE.md)
- [Test catalog](docs/TEST_CATALOG.md)
- [Testing guide](docs/TESTING.md)
- [Reliability behavior and limitations](docs/RELIABILITY.md)
- [Implementation trade-offs and follow-up work](docs/IMPLEMENTATION_TRADEOFFS.md)

## Repository layout

```text
src/
  core/           Topics, participants, runtimes
  discovery/      Registry, UDP client/server, protocol
  pubsub/         Publisher/subscriber state machines
  qos/            Reliability, history, sequence, liveliness
  serialization/  Wire messages and codecs
  transport/      UDP transport
unit_tests/       Behavior-focused unit tests
tests/            Cross-component integration and stress tests
examples/         Runnable demos
demo/             Demo guide and run instructions
benches/          Throughput benchmark
docs/             Architecture, API, testing and reliability docs
```

## License

MIT — see [LICENSE](LICENSE).
