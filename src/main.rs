mod core;

use core::{Message, Topic};

#[derive(Debug, Clone)]
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

    let message = Message::new(topic, state);

    println!("Topic: {}", message.topic().name());
    println!("Data: {:?}", message.data());
}