/*

discovery/

    discovery.rs => internal discovery abstraction
    protocol.rs => network discovery messages
    
*/ 


pub mod discovery;
pub mod protocol;

pub use protocol::{
    DiscoveredEndpoint,
    DiscoveryResponse,
    DiscoveryRequest
}

pub use discovery::{
    Discovery,
    Endpoint,
    EndpointKind,
    LocalDiscovery,
};