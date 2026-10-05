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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_participant_is_alive() {
        let mut tracker = LivelinessTracker::new(Duration::from_millis(100));

        tracker.observe(42);

        assert_eq!(tracker.state(42), LivelinessState::Alive);
    }

    #[test]
    fn unknown_participant_is_expired() {
        let tracker = LivelinessTracker::new(Duration::from_millis(100));

        assert_eq!(tracker.state(42), LivelinessState::Expired);
    }

    #[test]
    fn participant_expires_after_timeout() {
        let mut tracker = LivelinessTracker::new(Duration::from_millis(10));

        tracker.observe(42);

        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(tracker.state(42), LivelinessState::Expired);
    }
}
