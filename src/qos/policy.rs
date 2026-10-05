use std::time::Duration;

use crate::Reliability;

#[derive(Debug, Clone)]
pub struct QosPolicy {
    history_depth: usize,
    reliability: Reliability,
    liveliness_timeout: Duration,
}

impl QosPolicy {
    pub fn new(history_depth: usize, reliability: Reliability) -> Self {
        Self {
            history_depth,
            reliability,
            liveliness_timeout: Duration::from_secs(3),
        }
    }

    pub fn history_depth(&self) -> usize {
        self.history_depth
    }

    pub fn reliability(&self) -> Reliability {
        self.reliability
    }

    pub fn liveliness_timeout(&self) -> Duration {
        self.liveliness_timeout
    }

    pub fn with_liveliness_timeout(mut self, timeout: Duration) -> Self {
        self.liveliness_timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_liveliness_timeout_is_three_seconds() {
        let policy = QosPolicy::new(10, Reliability::BestEffort);

        assert_eq!(policy.liveliness_timeout(), Duration::from_secs(3));
    }

    #[test]
    fn liveliness_timeout_can_be_configured() {
        let policy = QosPolicy::new(10, Reliability::BestEffort)
            .with_liveliness_timeout(Duration::from_secs(5));

        assert_eq!(policy.liveliness_timeout(), Duration::from_secs(5));
    }
}
