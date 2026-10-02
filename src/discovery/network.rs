use std::io;
use std::net::{SocketAddr, UdpSocket};

use crate::core::Participant;

use super::{DiscoveredEndpoint, DiscoveryRequest, DiscoveryResponse, EndpointKind};

pub struct NetworkDiscovery {
    socket: UdpSocket,
    server_address: SocketAddr,
}

impl NetworkDiscovery {
    pub fn bind(local_address: SocketAddr, server_address: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(local_address)?;

        Ok(Self {
            socket,
            server_address,
        })
    }

    fn request(&self, request: DiscoveryRequest) -> io::Result<DiscoveryResponse> {
        let bytes = bincode::serialize(&request)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        self.socket.send_to(&bytes, self.server_address)?;

        let mut buffer = [0u8; 65_535];

        let (size, _) = self.socket.recv_from(&mut buffer)?;

        bincode::deserialize(&buffer[..size])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub fn register_participant(&self, participant: &Participant) -> io::Result<()> {
        let request = DiscoveryRequest::RegisterParticipant {
            participant_id: participant.id(),
            address: participant.address(),
        };

        match self.request(request)? {
            DiscoveryResponse::Registered => Ok(()),

            DiscoveryResponse::Error(message) => Err(io::Error::new(io::ErrorKind::Other, message)),

            response => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected discovery response: {response:?}"),
            )),
        }
    }

    pub fn register_endpoint(
        &self,
        participant_id: u64,
        topic: impl Into<String>,
        kind: EndpointKind,
    ) -> io::Result<()> {
        let request = DiscoveryRequest::RegisterEndpoint {
            participant_id,
            topic: topic.into(),
            kind,
        };

        match self.request(request)? {
            DiscoveryResponse::Registered => Ok(()),

            DiscoveryResponse::Error(message) => Err(io::Error::new(io::ErrorKind::Other, message)),

            response => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected discovery response: {response:?}"),
            )),
        }
    }

    pub fn lookup(
        &self,
        topic: impl Into<String>,
        kind: EndpointKind,
    ) -> io::Result<Vec<DiscoveredEndpoint>> {
        let request = DiscoveryRequest::Lookup {
            topic: topic.into(),
            kind,
        };

        match self.request(request)? {
            DiscoveryResponse::Endpoints(endpoints) => Ok(endpoints),

            DiscoveryResponse::Error(message) => Err(io::Error::new(io::ErrorKind::Other, message)),

            response => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected discovery response: {response:?}"),
            )),
        }
    }
}
