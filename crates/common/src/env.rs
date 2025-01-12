use std::path::PathBuf;
use std::str::FromStr;

use crate::abort;

pub trait FromEnv: Sized {
    fn from_env(value: &str) -> anyhow::Result<Self>;
}

impl FromEnv for bool {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(value.to_lowercase() == "true" || value == "1")
    }
}

impl FromEnv for u16 {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(value.parse()?)
    }
}

impl FromEnv for u64 {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(value.parse()?)
    }
}

impl FromEnv for usize {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(value.parse()?)
    }
}

impl FromEnv for PathBuf {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(PathBuf::from_str(value)?)
    }
}

/// Read the value of an environment variable and parse it to the given type, or
/// return the given `default`.
pub fn parse_env<T: FromEnv>(
    component: &str,
    namespace: Option<&str>,
    variable_name: &str,
    default: T,
) -> T {
    let mut prefix = format!("CRUNCH_{}", component.to_uppercase());
    if let Some(namespace) = namespace {
        prefix.push('_');
        prefix.push_str(&namespace.to_uppercase());
    }
    let name = format!("{prefix}__{}", variable_name.to_uppercase());
    std::env::var(&name)
        .map(|value| {
            T::from_env(value.as_str()).unwrap_or_else(|error| {
                let typename = std::any::type_name::<T>();
                abort!("failed to parse environment variable", name, value, typename, error);
            })
        })
        .unwrap_or(default)
}
