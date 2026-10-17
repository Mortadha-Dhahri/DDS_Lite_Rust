# Component architecture

## Runtime view

```text
                       +----------------------+
                       |     Application      |
                       +----------+-----------+
                                  |
                   +--------------+--------------+
                   |                             |
           +-------v--------+            +-------v--------+
           | Publisher<T>   |            | Subscriber<T>  |
           | history + ACKs |            | sequence state |
           | session ID     |            | liveliness     |
           +-------+--------+            +-------+--------+
                   |                             |
                   +-------------+---------------+
                                 |
                        +--------v--------+
                        |  UdpTransport   |
                        +-----------------+

 Publisher/Subscriber <--> NetworkDiscovery <--> DiscoveryServer
                                                   |
                                              LocalDiscovery

 Serialization: WireMessage / NetworkMessage / ControlMessage
 QoS: History / SequenceTracker / LivelinessTracker / QosPolicy
```

## Responsibilities

| Component | Owns | Not the same as |
|---|---|---|
| `Topic` | Topic name and declared type | Network registration |
| `Participant` | ID/address metadata | A process supervisor |
| `Publisher<T>` | Sequence assignment, session ID, history, fan-out, ACK/NACK and retries | Broker or durable queue |
| `Subscriber<T>` | Decode, topic filtering, sequence/session handling, control replies, liveliness observations | Global sequence tracker |
| `NetworkDiscovery` | Client UDP protocol | The registry |
| `DiscoveryServer` | Request handling, registry, participant expiry | Data relay |
| `LocalDiscovery` | In-memory participant/endpoint records | Persistent database |
| `UdpTransport` | Datagram send/receive | Reliability/order guarantee |
| Serialization codecs | Wire representation and bincode conversion | Protocol negotiation |
| QoS helpers | History, gaps, reliability policy, deadlines | Full DDS QoS |

## Dependency direction

The crate root re-exports convenient types. Core descriptors are used by discovery and pub/sub. Pub/sub depends on discovery for peer addresses, serialization for bytes, transport for datagrams, and QoS helpers for state. Discovery uses its own request/response protocol, local registry, and liveliness tracker.

## Consequences

- Discovery returns peer addresses but does not relay application data.
- Reliability is layered above UDP.
- Bounded history is the source for retransmission; evicted samples cannot be recovered.
- Subscriber sequence state is scoped by publisher address and session ID.
- Tests needing private state are attached as child modules rather than widening production visibility.
