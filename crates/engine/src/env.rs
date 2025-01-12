use crate::util::abort;

pub trait FromEnv: Sized {
    fn from_env(value: &str) -> anyhow::Result<Self>;
}

impl FromEnv for bool {
    fn from_env(value: &str) -> anyhow::Result<Self> {
        Ok(value.to_lowercase() == "true" || value == "1")
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

/// Read the value of an environment variable and parse it to the given type, or
/// return the given `default`.
pub fn parse_env<T: FromEnv>(namespace: &str, variable_name: &str, default: T) -> T {
    let name = format!("CRUNCH_{}__{}", namespace.to_uppercase(), variable_name.to_uppercase());
    std::env::var(&name)
        .map(|value| {
            T::from_env(value.as_str()).unwrap_or_else(|error| {
                let typename = std::any::type_name::<T>();
                abort!("failed to parse environment variable", name, value, typename, error);
            })
        })
        .unwrap_or(default)
}
