use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};

use crate::parse::SourceSpan;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Symbol {
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) span: SourceSpan,
}

impl Deref for Symbol {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

impl Hash for Symbol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state)
    }
}

impl PartialEq for Symbol {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Symbol {}

impl PartialOrd for Symbol {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for Symbol {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Debug for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Symbol {
    pub(crate) fn new(name: impl Into<SmartString<LazyCompact>>, span: SourceSpan) -> Self {
        Self {
            name: name.into(),
            span,
        }
    }
    pub(crate) fn is_prog_entry(&self) -> bool {
        self.name == "?"
    }
}

pub(crate) const PROG_ENTRY: &str = "?";
