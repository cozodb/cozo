use std::fmt::{Debug, Display, Formatter};
use std::str::Utf8Error;

use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};

#[derive(Debug, thiserror::Error)]
pub enum KeywordError {
    #[error("cannot convert to keyword: {0}")]
    InvalidKeyword(String),

    #[error("reserved keyword: {0}")]
    ReservedKeyword(Keyword),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error("unexpected json {0}")]
    UnexpectedJson(serde_json::Value),
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize, Serialize)]
pub struct Keyword(pub(crate) SmartString<LazyCompact>);

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, ":{}", self.0)
    }
}

impl Debug for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<&str> for Keyword {
    fn from(value: &str) -> Self {
        let value = value.strip_prefix(':').unwrap_or(value);
        Self(value.into())
    }
}

impl TryFrom<&[u8]> for Keyword {
    type Error = KeywordError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(std::str::from_utf8(value)?.into())
    }
}

impl Keyword {
    pub(crate) fn is_reserved(&self) -> bool {
        self.0.is_empty() || self.0.starts_with(['_', ':', '<', '.', '*', '?', '!'])
    }
    pub(crate) fn to_string_no_prefix(&self) -> String {
        format!("{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::keyword::Keyword;

    #[test]
    fn reserved_kw() {
        assert!(Keyword("_a".into()).is_reserved());
        assert!(Keyword(":a".into()).is_reserved());
        assert!(Keyword("".into()).is_reserved());
    }
}
