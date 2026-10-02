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
mod tests {
    use super::History;

    #[test]
    fn history_keeps_latest_samples() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");
        history.push(3, "three");
        history.push(4, "four");

        let sequences: Vec<_> = history
            .samples()
            .map(|entry| entry.sequence_number())
            .collect();

        assert_eq!(sequences, vec![2, 3, 4]);
    }

    #[test]
    fn zero_depth_keeps_nothing() {
        let mut history = History::new(0);

        history.push(1, "one");

        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }

    #[test]
    fn finds_sample_by_sequence_number() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");
        history.push(3, "three");

        let entry = history.find(2).unwrap();

        assert_eq!(entry.sequence_number(), 2);
        assert_eq!(entry.data(), &"two");
    }

    #[test]
    fn returns_none_for_missing_sequence() {
        let mut history = History::new(3);

        history.push(1, "one");
        history.push(2, "two");

        assert!(history.find(3).is_none());
    }
}