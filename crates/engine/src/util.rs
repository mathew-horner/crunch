// TODO: The assignment code can probably move to the repl crate.
use anyhow::{anyhow, Result};

pub struct Assignment<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> Assignment<'a> {
    pub fn parse(string: &'a str) -> Result<Assignment<'a>> {
        let (key, value) = string.split_once("=").ok_or_else(|| anyhow!("invalid input"))?;
        let key = key.trim();
        let value = value.trim();
        Ok(Assignment { key, value })
    }
}

macro_rules! format_variable {
    ($variable:ident, $value:expr) => {
        format!("{}={}", $variable, $value)
    };
}

// This shouldn't need to be `pub` in *theory*, but since it is used internally
// by another macro, which are expanded at the call site, it needs to be `pub`.
pub(crate) use format_variable;

/// Panic with a given message followed by a list of "key=value" pairs.
///
/// Example:
///
/// `abort!("some error", var1, var2, var3);`
/// -> "some error var1=<value> var2=<value> var3=<value>"
macro_rules! abort {
    ($message:expr, $($args:tt),*) => {{
        let mut message = String::new();
        message.push_str($message);
        $(
            message.push_str(" ");
            let name = stringify!($args);
            message.push_str(&crate::util::format_variable!(name, $args));
        )*
        panic!("{message}");
    }}
}

pub(crate) use abort;
