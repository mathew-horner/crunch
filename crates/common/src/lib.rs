pub mod env;

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
            message.push_str(&crate::format_variable!(name, $args));
        )*
        panic!("{message}");
    }}
}

pub(crate) use abort;
