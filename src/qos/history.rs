use std::collections::VecDeque;

pub struct History<T> {
    depth: usize,
    samples: VecDeque<T>,
}

impl<T> History<T> {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            samples: VecDeque::with_capacity(depth),
        }
    }

    pub fn push(&mut self, sample: T) {
        if self.depth == 0 {
            return;
        }

        if self.samples.len() == self.depth {
            self.samples.pop_front();
        }

        self.samples.push_back(sample);
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn samples(&self) -> impl Iterator<Item = &T> {
        self.samples.iter()
    }   
}

#[cfg(test)]
mod tests {
    use super::History;
    
    #[test]
    fn zero_depth_keeps_nothing() {
        let mut history = History::new(0);

        history.push(1);

        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }

    #[test]
    fn history_keeps_latest_samples() {
        let mut history = History::new(3);

        history.push(1);
        history.push(2);
        history.push(3);
        history.push(4);

        let samples: Vec<_> = history.samples().copied().collect();

        assert_eq!(samples, vec![2, 3, 4]); 
    }
}