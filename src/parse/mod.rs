use std::collections::BTreeMap;

use miette::{Diagnostic, Result};
use pest::error::InputLocation;
use pest::Parser;

use crate::data::program::InputProgram;
use crate::data::value::DataValue;
use crate::parse::query::parse_query;
use crate::parse::schema::{parse_schema, AttrTxItem};
use crate::parse::sys::{parse_sys, SysOp};
use crate::parse::tx::{parse_tx, TripleTx};

pub(crate) mod expr;
pub(crate) mod pull;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod sys;
pub(crate) mod tx;

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Query(InputProgram),
    Tx(TripleTx),
    Schema(Vec<AttrTxItem>),
    Sys(SysOp),
}

#[derive(thiserror::Error, Diagnostic, Debug)]
#[error("The query parser has encountered unexpected input / end of input")]
#[diagnostic(code(parse::pest))]
pub struct ParseError {
    #[label("here")]
    span: (usize, usize),
}

impl From<pest::error::Error<Rule>> for ParseError {
    fn from(err: pest::error::Error<Rule>) -> Self {
        match err.location {
            InputLocation::Pos(p) => Self { span: (p, 0) },
            InputLocation::Span((start, end)) => Self {
                span: (start, end - start),
            },
        }
    }
}

pub(crate) fn parse_script(
    src: &str,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<CozoScript> {
    let parsed = CozoScriptParser::parse(Rule::script, src)
        .map_err(|e| ParseError::from(e))?
        .next()
        .unwrap();
    Ok(match parsed.as_rule() {
        Rule::query_script => CozoScript::Query(parse_query(parsed.into_inner(), param_pool)?),
        Rule::schema_script => CozoScript::Schema(parse_schema(parsed.into_inner())?),
        Rule::tx_script => CozoScript::Tx(parse_tx(parsed.into_inner(), param_pool)?),
        Rule::sys_script => CozoScript::Sys(parse_sys(parsed.into_inner())?),
        _ => unreachable!(),
    })
}
