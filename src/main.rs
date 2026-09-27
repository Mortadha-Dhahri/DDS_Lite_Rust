// mod core;
// mod serialization;

// use core::{Message, Topic};
// use serialization::{
//     decode_message,
//     deserialize_payload,
//     encode_message,
//     serialize_payload,
//     WireMessage,
// };

// use serde::{Deserialize, Serialize};

// #[derive(Debug, Clone, Serialize, Deserialize)]
// struct VehicleState {
//     speed: f32,
//     steering_angle: f32,
// }

// fn main() {
//     let topic = Topic::new("vehicle/state", "VehicleState");

//     let state = VehicleState {
//         speed: 20.5,
//         steering_angle: 2.3,
//     };

//     let message = Message::new(topic.clone(), state);

//     // Application object -> payload bytes
//     let payload = serialize_payload(message.data())
//         .expect("failed to serialize payload");

//     // Application message -> wire message
//     let wire_message = WireMessage {
//         topic: topic.name().to_string(),
//         type_name: topic.type_name().to_string(),
//         payload,
//     };

//     // Wire message -> network bytes
//     let bytes = encode_message(&wire_message)
//         .expect("failed to encode wire message");

//     println!("Encoded message: {} bytes", bytes.len());

//     // Network bytes -> wire message
//     let received_wire_message = decode_message(&bytes)
//         .expect("failed to decode wire message");

//     // Payload bytes -> application object
//     let received_state: VehicleState =
//         deserialize_payload(&received_wire_message.payload)
//             .expect("failed to deserialize payload");

//     println!("Topic: {}", received_wire_message.topic);
//     println!("Type: {}", received_wire_message.type_name);
//     println!("Data: {:?}", received_state);
// }



// mod core;
// mod serialization;
// mod transport;

// use std::net::SocketAddr;
// use std::thread;

// use transport::{Transport, UdpTransport};

// fn main() {
//     let receiver_address: SocketAddr = "127.0.0.1:7001".parse().unwrap();

//     let receiver = UdpTransport::bind(receiver_address)
//         .expect("failed to bind receiver");

//     let sender = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
//         .expect("failed to bind sender");

//     let message = b"hello from DDS-Lite";

//     sender
//         .send(message, receiver.local_addr().unwrap())
//         .expect("failed to send message");

//     let (received, sender_address) = receiver
//         .receive()
//         .expect("failed to receive message");

//     println!("Received from: {sender_address}");
//     println!("Message: {}", String::from_utf8_lossy(&received));
// }




// mod core;
// mod discovery;
// mod serialization;
// mod transport;

// use std::net::SocketAddr;

// use discovery::{
//     DiscoveryServer,
//     Endpoint,
//     EndpointKind,
// };

// fn main() {
//     let mut discovery = DiscoveryServer::new();

//     let publisher = Endpoint {
//         participant_id: 1,
//         topic: "vehicle/state".to_string(),
//         address: "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
//         kind: EndpointKind::Publisher,
//     };

//     discovery.register(publisher);

//     let publishers = discovery.lookup("vehicle/state");

//     for endpoint in publishers {
//         println!(
//             "Found {:?} for {} at {}",
//             endpoint.kind,
//             endpoint.topic,
//             endpoint.address
//         );
//     }
// }


mod core;
mod discovery;
mod serialization;
mod transport;

use std::net::SocketAddr;

use discovery::{
    Discovery,
    Endpoint,
    EndpointKind,
    LocalDiscovery,
};

fn main() {
    let mut discovery = LocalDiscovery::new();

    let publisher = Endpoint {
        participant_id: 1,
        topic: "vehicle/state".to_string(),
        address: "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
        kind: EndpointKind::Publisher,
    };

    let subscriber = Endpoint {
        participant_id: 2,
        topic: "vehicle/state".to_string(),
        address: "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
        kind: EndpointKind::Subscriber,
    };

    discovery.register(publisher);
    discovery.register(subscriber);

    let publishers =
        discovery.lookup("vehicle/state", EndpointKind::Publisher);

    let subscribers =
        discovery.lookup("vehicle/state", EndpointKind::Subscriber);

    println!("Publishers: {publishers:#?}");
    println!("Subscribers: {subscribers:#?}");
}