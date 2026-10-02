#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Topic {
    name: String,
    type_name: String,
}

impl Topic {
    pub fn new(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}
