use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("{0} was too large. length: {1}, max: {2}")]
    TooLarge(PairComponent, usize, usize),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub enum PairComponent {
    Key,
    Value,
}

impl fmt::Display for PairComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let string = match self {
            Self::Key => "key",
            Self::Value => "value",
        };
        write!(f, "{string}")
    }
}
