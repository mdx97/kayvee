use anyhow::anyhow;

pub struct Assignment<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

impl<'a> TryFrom<&'a str> for Assignment<'a> {
    type Error = anyhow::Error;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let tokens: Vec<_> = s.split("=").map(str::trim).collect();
        if tokens.len() != 2 {
            return Err(anyhow!("string is in an invalid format!"));
        }

        Ok(Assignment {
            key: tokens[0],
            value: tokens[1],
        })
    }
}
