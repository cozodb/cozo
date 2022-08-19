use anyhow::{bail, Result};
use pest::Parser;
use serde_json::{json, Map};

use crate::data::json::JsonValue;
use crate::parse::cozoscript::{CozoScriptParser, Pair, Pairs, Rule};

pub(crate) fn parse_schema_to_json(src: &str) -> Result<JsonValue> {
    let parsed = CozoScriptParser::parse(Rule::schema_script, src)?;
    parsed_to_json(parsed)
}

fn parsed_to_json(src: Pairs<'_>) -> Result<JsonValue> {
    let mut ret = vec![];
    for pair in src {
        if pair.as_rule() == Rule::EOI {
            break;
        }
        for clause in parse_schema_clause(pair)? {
            ret.push(clause);
        }
    }
    Ok(json!({ "attrs": ret }))
}

fn parse_schema_clause(src: Pair<'_>) -> Result<Vec<JsonValue>> {
    let mut src = src.into_inner();
    let op = match src.next().unwrap().as_rule() {
        Rule::schema_put => "put",
        Rule::schema_retract => "retract",
        _ => unreachable!(),
    };
    let ident = src.next().unwrap().into_inner().next().unwrap().as_str();
    let mut ret = vec![];
    let attr_def = src.next().unwrap();
    match attr_def.as_rule() {
        Rule::simple_schema_def => {
            let mut ret_map = json!({ "name": ident, "cardinality": "one" });
            parse_attr_defs(attr_def.into_inner(), ret_map.as_object_mut().unwrap())?;
            ret.push(json!({ op: ret_map }));
        }
        Rule::nested_schema_def => {
            for clause in attr_def.into_inner() {
                let mut clause_row = clause.into_inner();
                let nested_ident = clause_row.next().unwrap().as_str();
                let combined_ident = format!("{}.{}", ident, nested_ident);
                let mut ret_map = json!({ "name": combined_ident, "cardinality": "one" });
                parse_attr_defs(clause_row, ret_map.as_object_mut().unwrap())?;
                ret.push(json!({ op: ret_map }));
            }
        }
        _ => unreachable!(),
    }
    Ok(ret)
}

fn parse_attr_defs(src: Pairs<'_>, map: &mut Map<String, JsonValue>) -> Result<()> {
    for pair in src {
        match pair.as_str() {
            "one" => map.insert("cardinality".to_string(), json!("one")),
            "many" => map.insert("cardinality".to_string(), json!("many")),
            "history" => map.insert("history".to_string(), json!(true)),
            "no_history" => map.insert("history".to_string(), json!(false)),
            "identity" => map.insert("index".to_string(), json!("identity")),
            "index" => map.insert("index".to_string(), json!("indexed")),
            "no_index" => map.insert("index".to_string(), json!("none")),
            "unique" => map.insert("index".to_string(), json!("unique")),
            "ref" => map.insert("type".to_string(), json!("ref")),
            "component" => map.insert("type".to_string(), json!("component")),
            "bool" => map.insert("type".to_string(), json!("bool")),
            "int" => map.insert("type".to_string(), json!("int")),
            "float" => map.insert("type".to_string(), json!("float")),
            "string" => map.insert("type".to_string(), json!("string")),
            "uuid" => map.insert("type".to_string(), json!("uuid")),
            "timestamp" => map.insert("type".to_string(), json!("timestamp")),
            "bytes" => map.insert("type".to_string(), json!("bytes")),
            "list" => map.insert("type".to_string(), json!("list")),
            v => bail!("cannot interpret {} as attribute property", v),
        };
    }
    Ok(())
}
