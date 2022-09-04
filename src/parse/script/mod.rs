use std::collections::BTreeMap;

use miette::{IntoDiagnostic, Result};
use pest::Parser;

use crate::data::program::InputProgram;
use crate::data::value::DataValue;
use crate::parse::script::query::parse_query;
use crate::parse::script::schema::{AttrTxItem, parse_schema};
use crate::parse::script::sys::{parse_sys, SysOp};
use crate::parse::script::tx::{parse_tx, Quintuple};

pub(crate) mod expr;
pub(crate) mod pull;
pub(crate) mod query;
pub(crate) mod tx;
pub(crate) mod schema;
pub(crate) mod sys;

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Query(InputProgram),
    Tx(Vec<Quintuple>),
    Schema(Vec<AttrTxItem>),
    Sys(SysOp)
}

pub(crate) fn parse_script(
    src: &str,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<CozoScript> {
    let parsed = CozoScriptParser::parse(Rule::script, src)
        .into_diagnostic()?
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
