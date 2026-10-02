use super::Topic;

#[derive(Debug, Clone)]
pub struct Message<T> {
    topic: Topic,
    data: T,
}

impl<T> Message<T> {
    pub fn new(topic: Topic, data: T) -> Self {
        Self { topic, data }
    }

    pub fn topic(&self) -> &Topic {
        &self.topic
    }

    pub fn data(&self) -> &T {
        &self.data
    }
}
