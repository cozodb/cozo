use std::fmt::{Debug, Display, Formatter};

use anyhow::{ensure, Result};
use lazy_static::lazy_static;
use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize, Serialize, Hash)]
pub(crate) struct Symbol(pub(crate) SmartString<LazyCompact>);

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl TryFrom<&[u8]> for Symbol {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(std::str::from_utf8(value)?.into())
    }
}

impl Symbol {
    pub(crate) fn is_reserved(&self) -> bool {
        self.0.is_empty()
            || self
                .0
                .starts_with(['_', ':', '<', '.', '*', '#', '$', '?', '!', ']', '['])
    }
    pub(crate) fn is_query_var(&self) -> bool {
        self.0.starts_with('?') && self.0.len() > 1
    }
    pub(crate) fn is_ignored_var(&self) -> bool {
        self.0 == "?_"
    }
    pub(crate) fn validate_not_reserved(&self) -> Result<()> {
        ensure!(
            !self.is_reserved(),
            "reserved symbol not allowed here: {}",
            self.0
        );
        Ok(())
    }
    pub(crate) fn validate_query_var(&self) -> Result<()> {
        ensure!(
            self.is_query_var(),
            "query var must start with '?': {}",
            self.0
        );
        Ok(())
    }
    pub(crate) fn is_prog_entry(&self) -> bool {
        self.0 == "?"
    }
}

lazy_static! {
    pub(crate) static ref PROG_ENTRY: Symbol = Symbol::from("?");
}

#[cfg(test)]
mod tests {
    use crate::data::symb::Symbol;

    #[test]
    fn reserved_symb() {
        assert!(Symbol("_a".into()).is_reserved());
        assert!(Symbol(":a".into()).is_reserved());
        assert!(Symbol("".into()).is_reserved());
    }
}
