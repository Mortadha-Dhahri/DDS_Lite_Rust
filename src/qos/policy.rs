use crate::Reliability;

#[derive(Debug, Clone)]
pub struct QosPolicy {
    history_depth: usize,
    reliability: Reliability,
}

impl QosPolicy {
    pub fn new(history_depth: usize, reliability: Reliability) -> Self {
        Self {
            history_depth,
            reliability,
        }
    }

    pub fn history_depth(&self) -> usize {
        self.history_depth
    }

    pub fn reliability(&self) -> Reliability {
        self.reliability
    }
}
