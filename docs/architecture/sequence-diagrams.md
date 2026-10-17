# Sequence diagrams

The Mermaid diagrams represent the current simplified protocol.

## Discovery and registration

```mermaid
sequenceDiagram
    participant App as Participant / Endpoint
    participant Client as NetworkDiscovery
    participant Server as DiscoveryServer
    participant Registry as LocalDiscovery
    App->>Client: register participant
    Client->>Server: RegisterParticipant
    Server->>Registry: add participant
    Server-->>Client: Registered
    App->>Client: register endpoint(topic, kind)
    Client->>Server: RegisterEndpoint
    Server->>Registry: add endpoint
    Server-->>Client: Registered
    App->>Client: lookup(topic, peer kind)
    Client->>Server: Lookup
    Server->>Registry: find matching endpoints
    Server-->>Client: endpoint list
```

## Publish and acknowledge

```mermaid
sequenceDiagram
    participant App as Publisher application
    participant Pub as Publisher<T>
    participant Disc as NetworkDiscovery
    participant Sub as Subscriber<T>
    App->>Pub: publish(value)
    Pub->>Pub: serialize + assign sequence/session
    Pub->>Pub: store in bounded history
    Pub->>Disc: lookup(topic, Subscriber)
    Disc-->>Pub: endpoint addresses
    Pub->>Sub: DATA over UDP
    Sub->>Sub: decode + validate
    Sub->>Sub: check session and sequence
    Sub-->>Pub: ACK over UDP
    Pub->>Pub: clear matching pending state
    Sub-->>App: decoded value
```

## Gap recovery

```mermaid
sequenceDiagram
    participant Pub as Publisher
    participant Sub as Subscriber
    Pub->>Sub: DATA sequence N+1
    Sub->>Sub: detect missing N
    Sub-->>Pub: NACK(missing=[N])
    Pub->>Pub: look up N in history
    alt retained and retry budget remains
        Pub-->>Sub: retransmit DATA N
        Sub->>Sub: fill sequence gap
        Sub-->>Pub: ACK
        Pub->>Pub: clear pending state
    else unavailable or retry limit exhausted
        Pub->>Pub: bounded failure/cleanup
    end
```

## Heartbeat and expiry

```mermaid
sequenceDiagram
    participant Runtime as ParticipantRuntime
    participant Client as NetworkDiscovery
    participant Server as DiscoveryServer
    participant Peer as Discovered peer
    Runtime->>Client: heartbeat(participant_id)
    Client->>Server: Heartbeat request
    Server->>Server: refresh last-seen time
    Runtime->>Peer: HEARTBEAT control message
    Peer->>Peer: update liveliness tracker
    Note over Server,Peer: No heartbeat before timeout
    Server->>Server: expire participant/endpoints
    Peer->>Peer: report peer expired
```

## Restart and delayed packets

```mermaid
sequenceDiagram
    participant Old as Publisher session A
    participant Sub as Subscriber
    participant New as Restarted session B
    Old->>Sub: DATA(session A, seq 8)
    Sub->>Sub: track session A
    New->>Sub: DATA(session B, seq 1)
    Sub->>Sub: retire A; activate B
    Old-->>Sub: delayed DATA(session A, seq 9)
    Sub->>Sub: reject retired session A
    New->>Sub: DATA(session B, seq 2)
    Sub->>Sub: advance session B tracker
```

## Notes

- Diagrams show conceptual ordering, not guaranteed timing or delivery.
- A line labeled UDP is a datagram; UDP does not guarantee arrival.
- See the [API reference](../API_REFERENCE.md) for exact signatures and wire fields.
