# DDS-Lite Rust

A lightweight DDS-inspired publish/subscribe middleware implemented from scratch in Rust.

This project is a learning and portfolio project focused on understanding the architecture and core mechanisms behind Data Distribution Service (DDS) middleware.

## Goals

The project aims to explore:

* Publish/subscribe communication
* UDP-based transport
* Topic-based message routing
* Serialization and deserialization
* Participant and endpoint discovery
* Quality of Service (QoS) concepts
* Message history
* Reliable delivery and retransmission
* Heartbeats and liveliness
* Performance and latency measurement

## Project Status

🚧 Early development

The implementation is being built incrementally, with the architecture and design documented alongside the code.

## Why Rust?

Rust provides useful abstractions for building networked systems while providing memory safety without relying on garbage collection.

The project is also an opportunity to explore how Rust can be used to implement low-level distributed-systems components.

## Scope

This project is **DDS-inspired** and is not intended to be a complete implementation of the DDS or RTPS specifications.

The implementation will progressively reproduce selected concepts in a simplified form to make the underlying architecture easier to understand.

## Roadmap

* [ ] Project architecture
* [ ] Core publish/subscribe model
* [ ] UDP transport
* [ ] Message serialization
* [ ] Publisher/subscriber communication
* [ ] Discovery
* [ ] QoS
* [ ] Message history
* [ ] Reliable delivery
* [ ] Heartbeats and liveliness
* [ ] Benchmarks
* [ ] ROS 2 integration

## License

MIT
