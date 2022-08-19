use std::str::FromStr;

use anyhow::Result;
use itertools::Itertools;
use pest::Parser;
use serde_json::json;

use crate::data::json::JsonValue;
use crate::parse::cozoscript::{CozoScriptParser, Pair, Pairs, Rule};
use crate::parse::cozoscript::query::build_expr;
use crate::parse::cozoscript::string::parse_string;

pub(crate) fn parse_tx_to_json(src: &str) -> Result<JsonValue> {
    let parsed = CozoScriptParser::parse(Rule::tx_script, src)?;
    parsed_to_json(parsed)
}

fn parsed_to_json(src: Pairs<'_>) -> Result<JsonValue> {
    let mut ret = vec![];
    for pair in src {
        if pair.as_rule() == Rule::EOI {
            break;
        }

        ret.push(parse_tx_clause(pair)?);
    }
    Ok(json!({ "tx": ret }))
}

fn parse_tx_clause(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    let nxt = src.next().unwrap();
    match nxt.as_rule() {
        Rule::tx_map => {
            let map = parse_tx_map(nxt)?;
            Ok(json!({ "put": map }))
        }
        _ => {
            let op = nxt.as_str();
            let map = parse_tx_map(src.next().unwrap())?;
            Ok(json!({ op: map }))
        }
    }
}

fn parse_tx_map(src: Pair<'_>) -> Result<JsonValue> {
    src.into_inner().map(parse_tx_pair).try_collect()
}

fn parse_tx_pair(src: Pair<'_>) -> Result<(String, JsonValue)> {
    let mut src = src.into_inner();
    let name = src.next().unwrap();
    let name = match name.as_rule() {
        Rule::tx_special_ident | Rule::compound_ident => name.as_str().to_string(),
        _ => parse_string(name)?,
    };
    let el = parse_el(src.next().unwrap())?;
    Ok((name, el))
}

fn parse_el(src: Pair<'_>) -> Result<JsonValue> {
    match src.as_rule() {
        Rule::tx_map => parse_tx_map(src),
        Rule::tx_list => parse_tx_list(src),
        Rule::expr => build_expr(src),
        Rule::neg_num => Ok(JsonValue::from_str(src.as_str())?),
        _ => unreachable!(),
    }
}

fn parse_tx_list(src: Pair<'_>) -> Result<JsonValue> {
    src.into_inner().map(parse_el).try_collect()
}
