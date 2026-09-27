# DDS-Lite Architecture

## Overview

DDS-Lite is structured as a layered publish/subscribe middleware.

The initial architecture consists of four main components:

```text
Application
    │
    ▼
Core
    │
    ├── Topics
    ├── Messages
    └── Participants
    │
    ▼
Discovery
    │
    ▼
Transport
    │
    ▼
UDP
```

## Core

The core layer defines the concepts exposed by the middleware:

* Topics
* Messages
* Publishers
* Subscribers
* Participants

The core layer should not depend directly on UDP or socket implementation details.

## Transport

The transport layer is responsible for network communication.

The first transport implementation will use UDP.

Its responsibility is limited to:

* Sending bytes
* Receiving bytes
* Managing network endpoints

The transport layer should not contain DDS-specific topic logic.

## Discovery

Discovery allows participants to determine which publishers and subscribers exist and how they can communicate.

Initially, discovery will be implemented using a simplified centralized discovery service.

The design can later be extended toward distributed discovery.

## Serialization

The serialization layer converts application messages into a representation suitable for network transmission and reconstructs them on reception.

## Design Principle

Each layer should have a clear responsibility.

Higher layers should depend on abstractions rather than directly manipulating lower-level implementation details.

For example:

```text
Publisher
    │
    ▼
Middleware
    │
    ▼
Transport
    │
    ▼
UDP Socket
```

The publisher should not directly manipulate a UDP socket.

## Future Components

The architecture will later be extended with:

* QoS
* Message history
* Reliable delivery
* Sequence numbers
* ACK/NACK
* Retransmission
* Heartbeats
* Liveliness
* Performance benchmarking
* ROS 2 integration
