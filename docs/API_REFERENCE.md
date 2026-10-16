 # API reference

This reference describes the crate's public surface and the main internal interfaces. It reflects the current implementation, a simplified DDS-inspired middleware rather than a complete DDS/RTPS implementation.

## Crate surface

`src/lib.rs` exposes `core`, `discovery`, `pubsub`, `qos`, `serialization`, and `transport`. Common re-exports include `Participant`, `ParticipantRuntime`, `Topic`, `DiscoveryServer`, `EndpointKind`, `NetworkDiscovery`, `Publisher<T>`, `Subscriber<T>`, `QosPolicy`, `Reliability`, `History<T>`, `SequenceTracker`, `LivelinessTracker`, `Transport`, and `UdpTransport`.

Typical flow: define a serializable payload and `Topic`; start a discovery server; create/register a participant and endpoint; construct a publisher or subscriber with a QoS policy; then publish or receive.

## Core — `src/core/`

### `Topic`
`Topic::new(name, type_name)` creates a topic descriptor from values convertible to `String`. Equality/hash include the topic's identifying fields. Constructing a topic does not register it with discovery.

### `Participant`
`Participant::new(id, address)` creates participant metadata. `id()` and `address()` expose its ID and socket address. Registration is performed by `NetworkDiscovery`, not by the descriptor itself.

### `Message<T>`
A local typed wrapper for a topic and value. `Message::new(topic, data)` stores both; it is distinct from the serialized network `WireMessage`.

### `ParticipantRuntime`
`ParticipantRuntime::new(participant_id, port, kind, topic, discovery_server) -> io::Result<Self>` binds a participant UDP transport, creates discovery, and registers participant and endpoint. `send_heartbeat()` sends a heartbeat control message to discovered peer endpoints for the topic. `with_heartbeat_interval(Duration)` configures the explicit heartbeat timer; `tick()` performs renewal/sending when due. `with_reliability_check_interval(Duration)` sets a reliability-check interval field. This runtime is caller-driven: `tick()` must be called; it is not an autonomous async executor.

## Publish/subscribe — `src/pubsub/`

### `Publisher<T>`
Owns topic, discovery client, UDP transport, QoS, bounded history, sequence state, publisher-session ID, and reliability bookkeeping.
- `Publisher::new(topic, discovery, transport, qos)` initializes publisher state and session identity.
- `publish(value)` serializes a value, assigns a sequence number, stores history, discovers matching subscribers, and sends data. Reliable mode tracks pending ACKs.
- `receive_control()` processes control traffic via the blocking receive path.
- `try_receive_control()` polls for control traffic without waiting when no packet is available.
- `handle_control_message(message, sender)` dispatches ACK/NACK/heartbeat behavior.
- `delivery_failed(subscriber, sequence_number)` reports the delivery failure state for a subscriber/sequence.
Best-effort mode does not maintain reliable ACK state. Reliable mode uses bounded retries (currently three attempts).

### `Subscriber<T>`
Owns topic, transport, QoS, sequence/session tracking, and liveliness state.
- `Subscriber::new(topic, discovery, transport, qos)` initializes receive state.
- `receive()` waits for data, decodes the network message, checks topic and payload, applies sequence/session rules, sends applicable control responses, and returns the decoded application value.
- `receive_control()` processes control messages where supported.
- `liveliness_state(participant_id)` returns tracked liveliness.
Reliable duplicates are acknowledged but not redelivered. Sequence state is scoped by publisher address and session ID; packets from retired sessions are ignored.

## Discovery — `src/discovery/`

### `EndpointKind`, `Endpoint`, `DiscoveredEndpoint`
Represent publisher/subscriber role and endpoint metadata such as participant, topic, and address.

### `LocalDiscovery`
In-memory registry. `new()/Default` initializes it; registration records participants/endpoints; lookup filters by topic and endpoint kind; `remove_participant(id)` removes the participant and associated endpoints. Duplicate registrations are intended to be idempotent.

### `DiscoveryServer`
Owns a UDP socket, registry, and liveliness tracker. `bind(address)` uses the default timeout; `bind_with_timeout(address, timeout)` customizes it; server loop/one-step processing methods handle requests; expiry removes inactive participants and their endpoints. Malformed datagrams should not terminate the server.

### `NetworkDiscovery`
UDP client. `bind(local_address, server_address)` creates the socket; registration, lookup, and heartbeat methods exchange serialized `DiscoveryRequest` and `DiscoveryResponse` values with the server. Operations return errors rather than treating failed lookups as successful empty results.

## QoS — `src/qos/`

### `Reliability` and `QosPolicy`
`BestEffort` avoids ACK/retransmission state; `Reliable` enables pending acknowledgements and bounded retries. `QosPolicy::new(history_depth, reliability)` sets history capacity and mode; policy builders/accessors configure or read fields. The default liveliness timeout is three seconds.

### `History<T>` / `HistoryEntry<T>`
`History::new(depth)` creates bounded sample history. New entries retain the latest samples up to depth; depth zero retains none. Lookup by sequence number supports retransmission. `HistoryEntry::new(sequence_number, data)` associates a sequence with a value.

### `SequenceTracker` / `SequenceEvent`
`SequenceTracker::new()` starts empty; observing a sequence classifies first, contiguous, duplicate, or gap/out-of-order arrivals and updates missing-sequence state. `MAX_TRACKED_GAP` is 128, bounding memory and NACK size; larger gaps may only be partially requested.

### `LivelinessTracker` / `LivelinessState`
`LivelinessTracker::new(timeout)` configures expiry. Observation refreshes a participant's last-seen time; `state(id)` reports `Alive` or `Expired`, and expiry scanning reports inactive IDs. This is a simple timeout-based mechanism, not a general distributed failure detector.

## Serialization — `src/serialization/`

`WireMessage` contains publisher session ID, sequence number, topic, and payload bytes. `ControlMessage` represents ACK, NACK, and heartbeat. `NetworkMessage` wraps data or control traffic.

- `serialize_payload` / `deserialize_payload` convert application values and bytes.
- `encode_message` / `decode_message` handle data wire messages.
- `encode_control_message` / `decode_control_message` handle control envelopes.
- `encode_network_message` / `decode_network_message` handle the top-level network enum.

Codec operations return bincode errors; endpoint layers may map these to `io::ErrorKind::InvalidData`. Session IDs changed the wire format; there is no protocol-version negotiation, so peers must use compatible builds.

## Transport — `src/transport/`

`Transport` defines `send(data, destination)` and `receive()`. `UdpTransport::bind(address)` binds a UDP socket and implements the trait. `try_receive()` polls without blocking and returns `Ok(None)` when no datagram is available; `local_addr()` reports the bound address. UDP itself does not guarantee delivery, ordering, or deduplication.

## Error conventions and related documents

Socket/discovery operations generally return `io::Result`; serialization returns bincode errors. A missing history sample cannot be retransmitted. Fan-out attempts other subscribers after an individual send failure, then surfaces the first error.

- [Test catalog](TEST_CATALOG.md)
- [Architecture overview](architecture.md)
- [Reliability behavior](RELIABILITY.md)
- [Testing guide](TESTING.md)
