/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use either::Either;
use miette::{bail, Diagnostic, IntoDiagnostic, Result};
use pest::error::InputLocation;
use pest::Parser;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::program::InputProgram;
use crate::data::relation::NullableColType;
use crate::data::value::{DataValue, ValidityTs};
use crate::parse::imperative::parse_imperative_block;
use crate::parse::query::parse_query;
use crate::parse::schema::parse_nullable_type;
use crate::parse::sys::{parse_sys, SysOp};
use crate::FixedRule;

pub(crate) mod expr;
pub(crate) mod imperative;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod sys;

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Single(InputProgram),
    Imperative(ImperativeProgram),
    Sys(SysOp),
}

#[derive(Debug)]
pub(crate) enum ImperativeStmt {
    Break {
        target: Option<SmartString<LazyCompact>>,
        span: SourceSpan,
    },
    Continue {
        target: Option<SmartString<LazyCompact>>,
        span: SourceSpan,
    },
    ReturnNil,
    ReturnProgram {
        prog: InputProgram,
        // span: SourceSpan,
    },
    ReturnTemp {
        rel: SmartString<LazyCompact>,
    },
    Program {
        prog: InputProgram,
    },
    IgnoreErrorProgram {
        prog: InputProgram,
    },
    If {
        condition: ImperativeCondition,
        then_branch: ImperativeProgram,
        else_branch: ImperativeProgram,
        negated: bool,
        span: SourceSpan,
    },
    Loop {
        label: Option<SmartString<LazyCompact>>,
        body: ImperativeProgram,
    },
    TempSwap {
        left: SmartString<LazyCompact>,
        right: SmartString<LazyCompact>,
        // span: SourceSpan,
    },
    TempDebug {
        temp: SmartString<LazyCompact>,
    },
}

pub(crate) type ImperativeCondition = Either<SmartString<LazyCompact>, InputProgram>;

pub(crate) type ImperativeProgram = Vec<ImperativeStmt>;

impl ImperativeStmt {
    pub(crate) fn needs_write_locks(&self, collector: &mut BTreeSet<SmartString<LazyCompact>>) {
        match self {
            ImperativeStmt::ReturnProgram { prog, .. }
            | ImperativeStmt::Program { prog, .. }
            | ImperativeStmt::IgnoreErrorProgram { prog, .. } => {
                if let Some(name) = prog.needs_write_lock() {
                    collector.insert(name);
                }
            }
            ImperativeStmt::If {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                if let ImperativeCondition::Right(prog) = condition {
                    if let Some(name) = prog.needs_write_lock() {
                        collector.insert(name);
                    }
                }
                for prog in then_branch.iter().chain(else_branch.iter()) {
                    prog.needs_write_locks(collector);
                }
            }
            ImperativeStmt::Loop { body, .. } => {
                for prog in body {
                    prog.needs_write_locks(collector);
                }
            }
            ImperativeStmt::TempDebug { .. }
            | ImperativeStmt::ReturnTemp { .. }
            | ImperativeStmt::Break { .. }
            | ImperativeStmt::Continue { .. }
            | ImperativeStmt::ReturnNil { .. }
            | ImperativeStmt::TempSwap { .. } => {}
        }
    }
}

impl CozoScript {
    pub(crate) fn get_single_program(self) -> Result<InputProgram> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("expect script to contain only a single program")]
        #[diagnostic(code(parser::expect_singleton))]
        struct ExpectSingleProgram;
        match self {
            CozoScript::Single(s) => Ok(s),
            CozoScript::Imperative(_) | CozoScript::Sys(_) => {
                bail!(ExpectSingleProgram)
            }
        }
    }
}

/// Span of the element in the source script, with starting and ending positions.
#[derive(
    Eq, PartialEq, Debug, serde_derive::Serialize, serde_derive::Deserialize, Copy, Clone, Default,
)]
pub struct SourceSpan(pub usize, pub usize);

impl Display for SourceSpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0, self.0 + self.1)
    }
}

impl SourceSpan {
    pub(crate) fn merge(self, other: Self) -> Self {
        let s1 = self.0;
        let e1 = self.0 + self.1;
        let s2 = other.0;
        let e2 = other.0 + other.1;
        let s = min(s1, s2);
        let e = max(e1, e2);
        Self(s, e - s)
    }
}

impl From<&'_ SourceSpan> for miette::SourceSpan {
    fn from(s: &'_ SourceSpan) -> Self {
        miette::SourceSpan::new(s.0.into(), s.1.into())
    }
}

impl From<SourceSpan> for miette::SourceSpan {
    fn from(s: SourceSpan) -> Self {
        miette::SourceSpan::new(s.0.into(), s.1.into())
    }
}

#[derive(thiserror::Error, Diagnostic, Debug)]
#[error("The query parser has encountered unexpected input / end of input at {span}")]
#[diagnostic(code(parser::pest))]
pub(crate) struct ParseError {
    #[label]
    pub(crate) span: SourceSpan,
}

pub(crate) fn parse_type(src: &str) -> Result<NullableColType> {
    let parsed = CozoScriptParser::parse(Rule::col_type_with_term, src)
        .into_diagnostic()?
        .next()
        .unwrap();
    parse_nullable_type(parsed.into_inner().next().unwrap())
}

pub(crate) fn parse_script(
    src: &str,
    param_pool: &BTreeMap<String, DataValue>,
    fixed_rules: &BTreeMap<String, Arc<Box<dyn FixedRule>>>,
    cur_vld: ValidityTs,
) -> Result<CozoScript> {
    let parsed = CozoScriptParser::parse(Rule::script, src)
        .map_err(|err| {
            let span = match err.location {
                InputLocation::Pos(p) => SourceSpan(p, 0),
                InputLocation::Span((start, end)) => SourceSpan(start, end - start),
            };
            ParseError { span }
        })?
        .next()
        .unwrap();
    Ok(match parsed.as_rule() {
        Rule::query_script => {
            let q = parse_query(parsed.into_inner(), param_pool, fixed_rules, cur_vld)?;
            CozoScript::Single(q)
        }
        Rule::imperative_script => {
            let p = parse_imperative_block(parsed, param_pool, fixed_rules, cur_vld)?;
            CozoScript::Imperative(p)
        }

        Rule::sys_script => CozoScript::Sys(parse_sys(
            parsed.into_inner(),
            param_pool,
            fixed_rules,
            cur_vld,
        )?),
        _ => unreachable!(),
    })
}

trait ExtractSpan {
    fn extract_span(&self) -> SourceSpan;
}

impl ExtractSpan for Pair<'_> {
    fn extract_span(&self) -> SourceSpan {
        let span = self.as_span();
        let start = span.start();
        let end = span.end();
        SourceSpan(start, end - start)
    }
}
