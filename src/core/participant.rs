#[derive(Debug, Clone)]
pub struct Participant {
    id: u64,
}

impl Participant {
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}