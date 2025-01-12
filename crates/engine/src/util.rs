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
