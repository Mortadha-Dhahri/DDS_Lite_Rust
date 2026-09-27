mod core;
mod serialization;

use core::{Message, Topic};
use serialization::{
    decode_message,
    deserialize_payload,
    encode_message,
    serialize_payload,
    WireMessage,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VehicleState {
    speed: f32,
    steering_angle: f32,
}

fn main() {
    let topic = Topic::new("vehicle/state", "VehicleState");

    let state = VehicleState {
        speed: 20.5,
        steering_angle: 2.3,
    };

    let message = Message::new(topic.clone(), state);

    // Application object -> payload bytes
    let payload = serialize_payload(message.data())
        .expect("failed to serialize payload");

    // Application message -> wire message
    let wire_message = WireMessage {
        topic: topic.name().to_string(),
        type_name: topic.type_name().to_string(),
        payload,
    };

    // Wire message -> network bytes
    let bytes = encode_message(&wire_message)
        .expect("failed to encode wire message");

    println!("Encoded message: {} bytes", bytes.len());

    // Network bytes -> wire message
    let received_wire_message = decode_message(&bytes)
        .expect("failed to decode wire message");

    // Payload bytes -> application object
    let received_state: VehicleState =
        deserialize_payload(&received_wire_message.payload)
            .expect("failed to deserialize payload");

    println!("Topic: {}", received_wire_message.topic);
    println!("Type: {}", received_wire_message.type_name);
    println!("Data: {:?}", received_state);
}