# Discovery and participant lifecycle

## Registration

1. Participant creates a `NetworkDiscovery` client bound to a local UDP address.
2. Client sends participant ID/address registration to the discovery server.
3. Server records the participant and its last-seen time.
4. Client registers an endpoint with participant ID, topic, and `EndpointKind`.
5. Publisher/subscriber lookup requests endpoints matching topic and peer kind.
6. Publisher can refresh lookup results so endpoints joining later can be discovered.

## Heartbeat and expiry

A participant heartbeat refreshes its server-side last-seen timestamp. The server processes expiry; participants past the configured timeout are removed with their endpoint records. Expiry is per participant. The runtime's `tick()` is explicit and must be called. The subscriber's local liveliness tracker separately records observed peer heartbeats.

## Error behavior

- Malformed discovery datagrams are rejected/ignored without terminating the server.
- Heartbeats from unknown IDs do not implicitly register a participant.
- Unavailable discovery operations return errors.
- Discovery is a control-plane registry; it does not relay application samples.

## Relevant tests

`unit_tests/discovery/endpoint_registry.rs`, `unit_tests/discovery/server_lifecycle.rs`, `tests/subscriber_cache.rs`, `tests/discovery_liveliness.rs`, `tests/liveliness.rs`, and `tests/discover_failure.rs`.
