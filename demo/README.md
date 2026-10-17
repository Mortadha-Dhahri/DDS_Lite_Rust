# DDS-Lite Rust demos

This directory is a **demo guide**, not a second implementation of the examples. The runnable Rust programs live in `examples/`; this guide explains what each one demonstrates, how to run it, and what the result does—and does not—prove.

All commands below are intended to run from the repository root on one machine. The current examples use loopback addresses such as `127.0.0.1`. Start the discovery server when a demo uses network discovery, and keep it running until the demo is finished.

> **Scope:** DDS-Lite Rust is a learning-focused DDS-inspired middleware. It is not a standards-conforming DDS/RTPS implementation and does not interoperate directly with production DDS implementations.

## Demo map

| Demo | What it shows | Runnable entry point |
|---|---|---|
| Basic typed publish/subscribe | A `VehicleState` sample on `vehicle/state` | `examples/participant.rs` |
| Network discovery | Register participants/endpoints and look up subscribers by topic | `examples/network_discovery.rs` |
| Reliable delivery | Send five sequenced samples and wait for ACKs | `examples/reliable_publisher.rs` + `examples/reliable_subscriber.rs` |
| Silent subscriber / missing ACKs | A registered UDP endpoint receives datagrams but intentionally sends no ACK | `examples/silent_subscriber.rs` |
| ACK/retransmission probe | Small publisher-side probe around the retransmission API | `examples/ack_timeout.rs` |
| Cooperative endpoint runtime | Managed polling, timer progress, and DATA/ACK behavior | `tests/endpoint_runtime.rs` |
| Reliability and loss-recovery behavior | ACK/NACK, bounded retries, sequence gaps, duplicate handling and history limits | `tests/reliable_delivery.rs`, `tests/sequence_robustness.rs`, and related tests |
| Liveliness and discovery expiry | Heartbeats keep a participant registered; inactive participants expire | `tests/discovery_liveliness.rs`, `tests/liveliness.rs` |
| Stress and transport robustness | Exercise behavior under repeated traffic and malformed/loss scenarios | `tests/stress.rs`, `tests/robustness.rs`, `tests/transport_robustness.rs` |
| Throughput benchmark | Local UDP best-effort sample throughput over 10,000 messages | `benches/throughput.rs` |

## 1. Basic typed publish/subscribe

This is the shortest end-to-end demo. It uses the centralized discovery server and the `participant` example.

**Terminal 1 — discovery server**

```bash
cargo run --example discovery_server
```

**Terminal 2 — subscriber**

```bash
cargo run --example participant -- 2 7002 subscriber
```

Wait for `Subscriber waiting for messages...`.

**Terminal 3 — publisher**

```bash
cargo run --example participant -- 1 7001 publisher
```

The publisher sends one `VehicleState` sample containing speed and steering-angle fields. The subscriber should print the received value.

**Important:** start the subscriber first. The current publisher example publishes once after its discovery lookup; it does not continuously publish or wait for a late subscriber.

## 2. Network discovery

Start the discovery server in Terminal 1:

```bash
cargo run --example discovery_server
```

Then run in Terminal 2:

```bash
cargo run --example network_discovery
```

The program registers two participants, registers a publisher and subscriber endpoint for `vehicle/state`, then looks up subscribers. This isolates the discovery control plane; it does not send application DATA between those endpoints.

## 3. Reliable delivery with ACKs

Start the discovery server:

```bash
cargo run --example discovery_server
```

Start the subscriber first:

```bash
cargo run --example reliable_subscriber
```

Then start the publisher in another terminal:

```bash
cargo run --example reliable_publisher
```

The publisher waits for a discovered subscriber, sends five sequenced `VehicleState` samples, processes ACK/control traffic and reports success only when each sample is acknowledged within its deadline. The subscriber waits for five samples and prints their sequence and speed.

This demonstrates the repository's current reliable-delivery path under a local, controlled run. It does not prove guaranteed delivery under arbitrary packet loss, process failure, or network partitions.

## 4. Silent subscriber: observe missing acknowledgements

This demo deliberately registers a subscriber endpoint but does not create a middleware `Subscriber`, so it receives UDP datagrams without sending DDS-Lite ACKs.

Start the discovery server:

```bash
cargo run --example discovery_server
```

Start the silent endpoint:

```bash
cargo run --example silent_subscriber
```

The silent subscriber binds to `127.0.0.1:7002` and keeps its discovery registration alive. It is intended to be paired with a publisher configured for reliable delivery if you are experimenting with ACK timeouts and retry limits. The existing `participant` example uses best-effort QoS, so it is not the appropriate partner for demonstrating reliable retries.

Stop the silent subscriber with Ctrl+C when finished.

## 5. ACK timeout / retransmission probe

The repository also contains `examples/ack_timeout.rs`:

```bash
cargo run --example discovery_server
```

In another terminal:

```bash
cargo run --example ack_timeout
```

Treat this as a low-level API probe, not as the canonical reliable-delivery demo. Its current publisher is configured with `Reliability::BestEffort`, so it does **not** establish the full reliable ACK/retransmission behavior. Use the reliable publisher/subscriber pair above for the end-to-end reliable path.

## 6. Cooperative endpoint runtime and reliability tests

These are automated demonstrations of behavior rather than interactive terminal programs. Run them from the repository root:

```bash
cargo test --test endpoint_runtime
cargo test --test reliable_delivery
cargo test --test sequence_robustness
cargo test --test discovery_liveliness
cargo test --test liveliness
```

They cover the managed cooperative-polling runtime, DATA/ACK exchange, retry and history behavior, sequence-gap/session handling, and participant liveness/expiry. See the [test catalog](../docs/TEST_CATALOG.md) for the purpose of individual tests.

To run the entire test suite:

```bash
cargo test --all-targets
```

## 7. Throughput benchmark

Run the existing benchmark harness:

```bash
cargo bench --bench throughput
```

The benchmark starts a local discovery server, exchanges 10,000 best-effort messages, and reports elapsed time and received-message throughput. Results depend on the host and runtime conditions; compare runs on the same machine and do not treat a single number as a general performance claim.

## Recommended demo order

1. **Basic pub/sub** — understand the topic and sample path.
2. **Network discovery** — isolate participant and endpoint registration.
3. **Reliable delivery** — observe ACK processing end to end.
4. **Silent subscriber** — explore the effect of missing ACKs.
5. **Runtime and robustness tests** — inspect lifecycle and failure-handling invariants.
6. **Throughput benchmark** — measure the current local best-effort path.

## Related documentation

- [Architecture overview](../docs/architecture.md)
- [Data flow](../docs/architecture/data-flow.md)
- [Discovery lifecycle](../docs/architecture/discovery.md)
- [Reliable delivery design](../docs/architecture/reliable-delivery.md)
- [Reliability behavior and limits](../docs/RELIABILITY.md)
- [Test catalog](../docs/TEST_CATALOG.md)
- [Implementation trade-offs](../docs/IMPLEMENTATION_TRADEOFFS.md)
