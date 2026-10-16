use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct HistoryEntry<T> {
    sequence_number: u64,
    data: T,
}

impl<T> HistoryEntry<T> {
    pub fn new(sequence_number: u64, data: T) -> Self {
        Self {
            sequence_number,
            data,
        }
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn data(&self) -> &T {
        &self.data
    }
}

pub struct History<T> {
    depth: usize,
    samples: VecDeque<HistoryEntry<T>>,
}

impl<T> History<T> {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            samples: VecDeque::with_capacity(depth),
        }
    }

    pub fn push(&mut self, sequence_number: u64, data: T) {
        if self.depth == 0 {
            return;
        }

        if self.samples.len() == self.depth {
            self.samples.pop_front();
        }

        self.samples
            .push_back(HistoryEntry::new(sequence_number, data));
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn samples(&self) -> impl Iterator<Item = &HistoryEntry<T>> {
        self.samples.iter()
    }

    pub fn find(&self, sequence_number: u64) -> Option<&HistoryEntry<T>> {
        self.samples
            .iter()
            .find(|entry| entry.sequence_number() == sequence_number)
    }
}

#[cfg(test)]
#[path = "../../unit_tests/qos/history_capacity.rs"]
mod history_capacity_tests;
