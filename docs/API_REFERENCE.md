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

### Managed endpoint runtime — `EndpointRuntime<T>`

`EndpointRuntime::new(ManagedEndpoint::Publisher(...))` or `EndpointRuntime::new(ManagedEndpoint::Subscriber(...))` switches the endpoint socket to nonblocking mode and creates a caller-driven polling runtime.

- `poll_once()` runs due timer actions and then attempts at most one datagram receive; it never waits for a datagram.
- `publish(value)` forwards to a managed publisher and returns `InvalidInput` for a managed subscriber.
- `with_reliability_timeout(Duration)` configures the ACK timeout used by publisher reliability checks; `with_reliability_interval(Duration)` configures how often those checks are scheduled.
- `with_heartbeat(interval, callback)` schedules a caller-supplied callback. It executes on the polling thread and must not block.
- `with_heartbeat_peer(interval, participant_id, address)` sends heartbeats to one known peer using the endpoint socket. Discovery refresh must happen outside the polling loop.
- `pending_ack_count()` and `local_addr()` expose endpoint state useful for monitoring and tests.

The runtime does not spawn a background thread. The application must call `poll_once()` repeatedly and may stop by dropping the runtime. Timer/callback errors are returned from `poll_once()`; timer work runs before receive so a timer error does not consume an unread DATA datagram. This cooperative model bounds each poll to one receive attempt, but callback work and other synchronous operations invoked by the application must themselves remain non-blocking for the overall loop to stay responsive.

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


## Complete method inventory

This section lists additional accessors and operational methods so the reference can be used as a source map. The Rust declarations remain authoritative for exact generic bounds and multiline signatures.

| Type / module | Method | Purpose |
|---|---|---|
| §Topic§ | §name()§, §type_name()§ | Read topic name and declared type. |
| §Participant§ | §id()§, §address()§ | Read participant identity and socket address. |
| §Message<T>§ | §topic()§, §data()§ | Borrow the local message topic and payload. |
| §NetworkDiscovery§ | §register_participant(&Participant)§ | Register participant metadata with the discovery server. |
| §NetworkDiscovery§ | §register_endpoint(participant_id, topic, kind)§ | Register the participant's publisher/subscriber endpoint. |
| §NetworkDiscovery§ | §lookup(topic, kind)§ | Return discovered endpoints matching a topic and role. |
| §NetworkDiscovery§ | §heartbeat(participant_id)§ | Refresh server-side participant activity. |
| §DiscoveryServer§ | §local_addr()§ | Return the bound server address, useful when bound to port zero. |
| §LocalDiscovery§ | §participant_address(id)§ | Resolve a registered participant ID to an optional socket address. |
| §Publisher<T>§ | §retransmit(sequence, destination)§ | Send a retained sample to a specified address. |
| §Publisher<T>§ | §local_addr()§ | Return publisher transport address. |
| §Publisher<T>§ | §pending_ack_count()§ | Inspect number of pending acknowledgements. |
| §Publisher<T>§ | §expired_acknowledgements(timeout)§ | List subscriber/sequence pairs whose ACK deadline has elapsed. |
| §Publisher<T>§ | §retransmit_expired_acknowledgements(timeout)§ | Retry eligible deliveries whose ACK timed out. |
| §Publisher<T>§ | §process_reliability(timeout)§ / §reliability_tick(timeout)§ | Process the reliability timer path explicitly. |
| §Publisher<T>§ | §set_nonblocking(bool)§, §try_receive_control()§ | Configure/poll control receive without waiting. The poll returns whether a control datagram was handled. |
| §Publisher<T>§ | §discovered_subscribers()§, §refresh_subscribers()§ | Read cached discovery results and refresh them. |
| §Publisher<T>§ | §set_subscriber_refresh_interval(Duration)§ | Configure the refresh interval. |
| §Subscriber<T>§ | §try_receive()§ | Nonblocking receive; returns §Ok(None)§ when no application sample is ready. |
| §Subscriber<T>§ | §set_nonblocking(bool)§, §local_addr()§ | Configure socket receive mode and inspect bound address. |
| §Subscriber<T>§ | §handle_control_message(ControlMessage)§ | Apply a decoded control message to subscriber-side state. |
| §QosPolicy§ | §history_depth()§, §reliability()§, §liveliness_timeout()§ | Read active policy values. |
| §QosPolicy§ | §with_liveliness_timeout(Duration)§ | Builder-style liveliness timeout override. |
| §History<T>§ | §push(sequence, data)§, §len()§, §is_empty()§ | Add a sample and inspect bounded history. |
| §History<T>§ | §samples()§, §find(sequence)§ | Iterate retained entries or locate a sequence. |
| §HistoryEntry<T>§ | §sequence_number()§, §data()§ | Read entry sequence and borrow its payload. |
| §SequenceTracker§ | §observe(sequence)§, §reset()§ | Update or clear sequence state. |
| §SequenceTracker§ | §last_sequence()§, §missing_sequences()§, §has_missing()§ | Inspect high-water mark and gaps. |
| §LivelinessTracker§ | §observe(id)§, §state(id)§ | Refresh a participant and read current state. |
| §LivelinessTracker§ | §expired_participants()§, §forget(id)§ | List expired IDs and remove tracked state. |
| Serialization codecs | §serialize_payload§, §deserialize_payload§ | Convert application payloads to/from bytes. |
| Serialization codecs | §encode_message§, §decode_message§ | Convert a §WireMessage§ to/from bytes. |
| Serialization codecs | §encode_control_message§, §decode_control_message§ | Convert a §ControlMessage§ to/from bytes. |
| Serialization codecs | §encode_network_message§, §decode_network_message§ | Convert the top-level data/control enum to/from bytes. |
| §Transport§ | §send(bytes, destination)§, §receive()§ | Trait-level datagram send and blocking receive. |
| §UdpTransport§ | §bind(address)§, §local_addr()§ | Bind and inspect a UDP socket. |
| §UdpTransport§ | §set_nonblocking(bool)§, §try_receive()§ | Configure/poll receive; no packet is represented as §Ok(None)§. |

## Public data types

- §Reliability::{BestEffort, Reliable}§ selects the publisher delivery mode.
- §LivelinessState::{Alive, Expired}§ represents the tracked state for a participant.
- §SequenceEvent§ classifies sequence observations (first/contiguous, duplicate, and gap-related cases as defined by the enum).
- §EndpointKind::{Publisher, Subscriber}§ selects endpoint role for discovery.
- §DiscoveryRequest§ and §DiscoveryResponse§ are the discovery protocol request/response enums.
- §ControlMessage§ contains ACK, NACK, and heartbeat control variants.
- §NetworkMessage::{Data, Control}§ wraps data and control traffic.
- §WireMessage§ is the data envelope; its session ID is part of the serialized format.
