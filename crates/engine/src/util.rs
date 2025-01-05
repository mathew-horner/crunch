pub struct Assignment<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> Assignment<'a> {
    /// Parse user input into an assignment operation.
    pub fn parse(string: &'a str) -> Result<Assignment<'a>, String> {
        // TODO: Use a proper error type like anyhow.
        let (key, value) = string.split_once("=").ok_or_else(|| "invalid input".to_owned())?;
        let key = key.trim();
        let value = value.trim();
        Ok(Assignment { key, value })
    }
}
