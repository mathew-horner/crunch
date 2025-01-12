use std::fmt;
use std::sync::PoisonError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("general error: {0}")]
    General(#[from] anyhow::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("lock was poisoned")]
    Poison,

    #[error("{0} was too large. length: {1}, max: {2}")]
    TooLarge(PairComponent, usize, usize),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Poison
    }
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
