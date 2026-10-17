# Implementation trade-offs and follow-up work

This document distinguishes current behavior from possible future improvements. It is not a promise of DDS/RTPS conformance.

## Logging

Library code emits diagnostics through the `log` facade rather than writing directly to stdout/stderr. Applications choose and initialize a logger (for example, `env_logger` or `tracing-log`); without a logger implementation, log records are normally discarded. Runnable examples may print user-facing status and received samples.

## Blocking, polling, and scheduling

The core APIs are synchronous. Blocking receive methods wait for a datagram, and the basic examples explicitly call `receive()`, `receive_control()`, or timer methods. Applications are responsible for scheduling those calls.

`EndpointRuntime<T>` provides cooperative polling. It does not create a background thread or executor: timers and receive handling advance only while the caller invokes `poll_once()`. Each runtime owns its endpoint's receive path, and each poll handles due timer work plus at most one nonblocking receive attempt. This improves integration into a caller-owned loop, but it is not equivalent to an asynchronous runtime. `Publisher::publish()` can still perform synchronous discovery lookup work, so it is not guaranteed to have bounded low latency.

Potential future directions include a readiness-based reactor such as `mio` or an async runtime such as Tokio. Those would be architectural changes, not drop-in claims about the current API.

## Discovery topology

Discovery is client/server: participants register and look up topic endpoints through a single UDP `DiscoveryServer`. It is not DDS-style multicast discovery. This is a deliberate educational simplification that keeps registration and lookup behavior easy to test, at the cost of a single point of failure and a dependency on the server for fresh discovery information.

## Wire format and compatibility

The current implementation uses Serde with bincode 1.x for serialized protocol messages and payloads. The wire format has no explicit version field or negotiation. Adding fields to serialized structures can break communication between binaries built from different revisions.

A future serialization review could compare postcard and bincode 2, but serializer replacement alone does not provide protocol compatibility. Before changing the format, define an envelope/version policy, compatibility tests, and a migration strategy.

## Known implementation costs and configuration limits

- UDP receive helpers allocate a 65,535-byte buffer per receive call. Buffer reuse or a caller-provided buffer could reduce repeated allocation.
- The participant runtime example binds to `127.0.0.1` and uses a loopback discovery server address. Multi-host operation requires configurable bind/advertised addresses and appropriate network configuration.
- Publisher discovery lookup may happen synchronously inside `publish()` on initial publication and at the refresh interval. A separate discovery-refresh path or caller-triggered refresh can keep it off latency-sensitive publishing paths.
- Reliable delivery has bounded history and bounded retries; it does not guarantee delivery through arbitrary loss, crashes, or partitions.
