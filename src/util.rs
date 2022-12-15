use std::str::FromStr;

use anyhow::anyhow;

pub struct Assignment {
    pub key: String,
    pub value: String,
}

impl FromStr for Assignment {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let tokens = s
            .split("=")
            .map(|t| t.trim().to_string())
            .collect::<Vec<String>>();

        if tokens.len() != 2 {
            return Err(anyhow!("string is in an invalid format!"));
        }

        Ok(Assignment {
            key: tokens[0].to_owned(),
            value: tokens[1].to_owned(),
        })
    }
}
