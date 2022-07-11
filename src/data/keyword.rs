use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::str::Utf8Error;

#[derive(Debug, thiserror::Error)]
pub enum KeywordError {
    #[error("cannot convert to keyword: {0}")]
    InvalidKeyword(String),

    #[error("reserved keyword: {0}")]
    ReservedKeyword(Keyword),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),
}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize, Serialize)]
pub struct Keyword {
    #[serde(rename = "n")]
    pub(crate) ns: SmartString<LazyCompact>,
    #[serde(rename = "i")]
    pub(crate) ident: SmartString<LazyCompact>,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.ns.is_empty() {
            write!(f, ":{}", self.ident)
        } else {
            write!(f, ":{}/{}", self.ns, self.ident)
        }
    }
}

impl Debug for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl TryFrom<&str> for Keyword {
    type Error = KeywordError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let make_err = || KeywordError::InvalidKeyword(value.to_string());
        let mut kw_iter = value.split('/');
        let ns = kw_iter.next().ok_or_else(make_err)?;
        let ident = match kw_iter.next() {
            None => {
                return Ok(Keyword {
                    ns: "".into(),
                    ident: ns.into(),
                })
            }
            Some(ident) => ident,
        };
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

impl Keyword {
    pub(crate) fn is_reserved(&self) -> bool {
        self.ns.is_empty() && self.ident.starts_with('_')
    }
}
