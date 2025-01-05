pub trait FromEnv {
    /// Parse an environment variable value to a type.
    fn from_env(value: &str) -> Self;
}

impl FromEnv for bool {
    fn from_env(value: &str) -> Self {
        value.to_lowercase() == "true" || value == "1"
    }
}

impl FromEnv for u64 {
    fn from_env(value: &str) -> Self {
        value.parse().unwrap()
    }
}

impl FromEnv for usize {
    fn from_env(value: &str) -> Self {
        value.parse().unwrap()
    }
}

/// Read the value of an environment variable and parse it to the given type, or
/// return the given `default`.
pub fn parse_env<T: FromEnv>(namespace: &str, variable_name: &str, default: T) -> T {
    let namespace = namespace.to_uppercase();
    let variable_name = variable_name.to_uppercase();
    std::env::var(&format!("CRUNCH_{namespace}__{variable_name}"))
        .map(|value| T::from_env(value.as_str()))
        .unwrap_or(default)
}
