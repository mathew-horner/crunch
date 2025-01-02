pub struct Assignment {
    pub key: String,
    pub value: String,
}

pub fn parse_assignment(string: &str) -> Result<Assignment, String> {
    let tokens = string.split("=").map(|t| t.trim().to_string()).collect::<Vec<String>>();

    if tokens.len() != 2 {
        return Err("String is in an invalid format!".into());
    }

    Ok(Assignment { key: tokens[0].to_owned(), value: tokens[1].to_owned() })
}
