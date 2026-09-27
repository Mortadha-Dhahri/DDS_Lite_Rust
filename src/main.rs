mod core;

use core::Topic;

fn main() {
    let topic = Topic::new("vehicle/state", "VehicleState");

    println!("{topic:?}");
}