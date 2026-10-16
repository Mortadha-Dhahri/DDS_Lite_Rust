use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivelinessState {
    Alive,
    Expired,
}

#[derive(Debug)]
pub struct LivelinessTracker {
    timeout: Duration,
    participants: HashMap<u64, Instant>,
}

impl LivelinessTracker {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            participants: HashMap::new(),
        }
    }

    pub fn observe(&mut self, participant_id: u64) {
        self.participants.insert(participant_id, Instant::now());
    }

    pub fn state(&self, participant_id: u64) -> LivelinessState {
        match self.participants.get(&participant_id) {
            Some(last_seen) if last_seen.elapsed() < self.timeout => LivelinessState::Alive,
            _ => LivelinessState::Expired,
        }
    }

    pub fn expired_participants(&self) -> Vec<u64> {
        self.participants
            .iter()
            .filter_map(|(participant_id, last_seen)| {
                if last_seen.elapsed() >= self.timeout {
                    Some(*participant_id)
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn forget(&mut self, participant_id: u64) {
        self.participants.remove(&participant_id);
    }
}

#[cfg(test)]
#[path = "../../unit_tests/qos/liveliness_expiry.rs"]
mod liveliness_expiry_tests;
