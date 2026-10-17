# Architecture overview

DDS-Lite Rust is a layered, synchronous, UDP-based publish/subscribe learning project.

## Layers

1. **Application API:** `Topic`, `Participant`, `Publisher<T>`, and `Subscriber<T>`.
2. **Discovery/control plane:** `NetworkDiscovery` communicates with `DiscoveryServer`; `LocalDiscovery` stores participant/endpoint records.
3. **Data/control protocol:** `WireMessage`, `NetworkMessage`, and `ControlMessage` use bincode serialization.
4. **Transport:** `UdpTransport` sends and receives datagrams.
5. **QoS helpers:** history, reliability policy, sequence tracking, and liveliness state.

## Design decisions

- Discovery returns peer addresses but does not relay samples.
- Publisher history is bounded and is the source for retransmission.
- ACK/NACK and sequence tracking sit above UDP.
- Subscriber sequence state is scoped by publisher address and session ID.
- Unit tests needing private state are attached with `#[path]` rather than making fields public.

Read [component architecture](architecture/components.md), [data flow](architecture/data-flow.md), [discovery](architecture/discovery.md), [reliable delivery](architecture/reliable-delivery.md), and [sequence diagrams](architecture/sequence-diagrams.md).
