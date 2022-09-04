use std::collections::BTreeMap;

use miette::{IntoDiagnostic, Result};
use pest::Parser;

use crate::data::program::InputProgram;
use crate::data::value::DataValue;
use crate::parse::script::query::parse_query;

pub(crate) mod query;
pub(crate) mod expr;
pub(crate) mod pull;

#[derive(pest_derive::Parser)]
#[grammar = "cozoscript.pest"]
pub(crate) struct CozoScriptParser;

pub(crate) type Pair<'a> = pest::iterators::Pair<'a, Rule>;
pub(crate) type Pairs<'a> = pest::iterators::Pairs<'a, Rule>;

pub(crate) enum CozoScript {
    Query(InputProgram),
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
        Rule::schema_script => todo!(),
        Rule::tx_script => todo!(),
        Rule::sys_script => todo!(),
        _ => unreachable!(),
    })
}
