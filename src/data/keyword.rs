use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use std::str::Utf8Error;

#[derive(Debug, thiserror::Error)]
pub enum KeywordError {
    #[error("invalid keyword: {0}")]
    InvalidKeyword(String),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug, Deserialize, Serialize)]
pub struct Keyword {
    #[serde(rename = "n")]
    pub(crate) ns: SmartString<LazyCompact>,
    #[serde(rename = "i")]
    pub(crate) ident: SmartString<LazyCompact>,
}

impl TryFrom<&str> for Keyword {
    type Error = KeywordError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let make_err = || KeywordError::InvalidKeyword(value.to_string());
        let mut kw_iter = value.split('/');
        let ns = kw_iter.next().ok_or_else(make_err)?;
        let ident = kw_iter.next().ok_or_else(make_err)?;
        if kw_iter.next().is_none() {
            Ok(Keyword {
                ns: ns.into(),
                ident: ident.into(),
            })
        } else {
            Err(make_err())
        }
    }
}

impl TryFrom<&[u8]> for Keyword {
    type Error = KeywordError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(value)?.try_into()
    }
}
