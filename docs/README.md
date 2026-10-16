# Architecture documentation

These documents describe the current implementation, not a target architecture or a claim of DDS/RTPS conformance.

- [Layered architecture](architecture.md): modules and responsibilities.
- [Component architecture](architecture/components.md): component boundaries and dependencies.
- [Data flow](architecture/data-flow.md): end-to-end publication and reception.
- [Discovery lifecycle](architecture/discovery.md): registration, lookup, heartbeat, expiry.
- [Reliable delivery](architecture/reliable-delivery.md): ACK/NACK, history, retries, sequence/session state.
- [Sequence diagrams](architecture/sequence-diagrams.md): Mermaid flows for discovery, publish/ACK, gap recovery, heartbeat/expiry, and restart sessions.
- [API reference](API_REFERENCE.md).
- [Test catalog](TEST_CATALOG.md).

Recommended reading order: component architecture → data flow → reliability → sequence diagrams. The implementation is synchronous and UDP-based; explicit polling and `tick()` calls are not an always-running executor.
