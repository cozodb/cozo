/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use miette::{bail, Diagnostic, Result};
use serde_derive::{Deserialize, Serialize};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::parse::SourceSpan;

/// Names with associated source span
#[derive(Clone, Deserialize, Serialize)]
pub struct Symbol {
    pub(crate) name: SmartString<LazyCompact>,
    #[serde(skip)]
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
        Some(self.cmp(other))
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
    pub(crate) fn is_temp_store_name(&self) -> bool {
        self.name.starts_with('_')
    }
    pub(crate) fn is_prog_entry(&self) -> bool {
        self.name == "?"
    }
    pub(crate) fn is_ignored_symbol(&self) -> bool {
        self.name == "_"
    }
    pub(crate) fn is_generated_ignored_symbol(&self) -> bool {
        self.name.starts_with('~')
    }
    pub(crate) fn ensure_valid_field(&self) -> Result<()> {
        if self.name.contains('(') || self.name.contains(')') {
            #[derive(Debug, Error, Diagnostic)]
            #[error("The symbol {0} is not valid as a field")]
            #[diagnostic(code(parser::symbol_invalid_as_field))]
            struct SymbolInvalidAsField(String, #[label] SourceSpan);

            bail!(SymbolInvalidAsField(self.name.to_string(), self.span))
        }
        Ok(())
    }
}

pub(crate) const PROG_ENTRY: &str = "?";
