use std::borrow::BorrowMut;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use itertools::Itertools;
use lazy_static::lazy_static;
use pest::prec_climber::{Assoc, Operator, PrecClimber};
use pest::Parser;
use serde_json::{json, Map};

use crate::data::json::JsonValue;
use crate::parse::cozoscript::number::parse_int;
use crate::parse::cozoscript::schema::parsed_schema_to_json;
use crate::parse::cozoscript::string::parse_string;
use crate::parse::cozoscript::tx::parsed_tx_to_json;
use crate::parse::cozoscript::{CozoScriptParser, Pair, Pairs, Rule};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ScriptType {
    Query,
    Schema,
    Tx,
}

pub(crate) fn parse_query_to_json(src: &str) -> Result<(ScriptType, JsonValue)> {
    let parsed = CozoScriptParser::parse(Rule::script, src)?.next().unwrap();
    Ok(match parsed.as_rule() {
        Rule::query_script => (
            ScriptType::Query,
            parsed_query_to_json(parsed.into_inner())?,
        ),
        Rule::schema_script => (
            ScriptType::Schema,
            parsed_schema_to_json(parsed.into_inner())?,
        ),
        Rule::tx_script => (ScriptType::Tx, parsed_tx_to_json(parsed.into_inner())?),
        _ => unreachable!(),
    })
}

fn parsed_query_to_json(src: Pairs<'_>) -> Result<JsonValue> {
    let mut ret_map = Map::default();
    let mut rules = vec![];
    let mut const_rules = Map::default();
    let mut algo_applies = vec![];
    for pair in src {
        match pair.as_rule() {
            Rule::rule => rules.push(parse_rule(pair)?),
            Rule::algo_rule => {
                let apply = parse_algo_rule(pair)?;
                algo_applies.push(apply);
            }
            Rule::const_rule => {
                let mut src = pair.into_inner();
                let name = src.next().unwrap().as_str();
                let data = build_expr(src.next().unwrap())?;
                let data = data
                    .as_array()
                    .ok_or_else(|| anyhow!("expect const rules to be specified as an array"))?;
                let entries = const_rules
                    .entry(name.to_string())
                    .or_insert_with(|| json!([]))
                    .borrow_mut();
                let entries = entries.as_array_mut().unwrap();
                entries.extend_from_slice(data);
            }
            Rule::limit_option => {
                let limit = parse_limit_or_offset(pair)?;
                ret_map.insert("limit".to_string(), json!(limit));
            }
            Rule::offset_option => {
                let offset = parse_limit_or_offset(pair)?;
                ret_map.insert("offset".to_string(), json!(offset));
            }
            Rule::sort_option => {
                let mut collected = vec![];
                for part in pair.into_inner() {
                    let mut var = "";
                    let mut dir = "asc";
                    for a in part.into_inner() {
                        match a.as_rule() {
                            Rule::var => var = a.as_str(),
                            Rule::sort_asc => dir = "asc",
                            Rule::sort_desc => dir = "desc",
                            _ => unreachable!(),
                        }
                    }
                    collected.push(json!({ var: dir }))
                }
                ret_map.insert("sort".to_string(), json!(collected));
            }
            Rule::out_option => {
                ret_map.insert(
                    "out".to_string(),
                    parse_out_option(pair.into_inner().next().unwrap())?,
                );
            }
            Rule::view_option => {
                let mut args = pair.into_inner();
                let op = args.next().unwrap().as_str();
                let name = args.next().unwrap().as_str();
                ret_map.insert("view".to_string(), json!({ op: name }));
            }
            Rule::EOI => break,
            r => unreachable!("{:?}", r),
        }
    }
    ret_map.insert("const_rules".to_string(), json!(const_rules));
    ret_map.insert("q".to_string(), json!(rules));
    ret_map.insert("algo_rules".to_string(), json!(algo_applies));
    Ok(json!(ret_map))
}

fn parse_out_option(src: Pair<'_>) -> Result<JsonValue> {
    Ok(match src.as_rule() {
        Rule::out_list_spec => {
            let l: Vec<_> = src.into_inner().map(parse_pull_spec).try_collect()?;
            json!(l)
        }
        Rule::out_map_spec => {
            let m: Map<_, _> = src
                .into_inner()
                .map(|p| -> Result<(String, JsonValue)> {
                    let mut p = p.into_inner();
                    let name = p.next().unwrap().as_str();
                    let spec = parse_pull_spec(p.next().unwrap())?;
                    Ok((name.to_string(), spec))
                })
                .try_collect()?;
            json!(m)
        }
        _ => unreachable!(),
    })
}

fn parse_pull_spec(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    let name = src.next().unwrap().as_str();
    let args: Vec<_> = src
        .next()
        .unwrap()
        .into_inner()
        .map(parse_pull_arg)
        .try_collect()?;
    Ok(json!({"pull": name, "spec": args}))
}

fn parse_pull_arg(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    let pull_def = src.next().unwrap();
    let mut ret = match pull_def.as_rule() {
        Rule::pull_all => {
            json!("*")
        }
        Rule::pull_id => {
            json!("_id")
        }
        Rule::pull_attr => {
            let mut pull_def = pull_def.into_inner();
            let mut ret = json!(pull_def.next().unwrap().as_str());
            if let Some(args) = pull_def.next() {
                let args: Vec<_> = args.into_inner().map(parse_pull_arg).try_collect()?;
                if !args.is_empty() {
                    if !ret.is_object() {
                        ret = json!({ "pull": ret });
                    }
                    ret.as_object_mut()
                        .unwrap()
                        .insert("spec".to_string(), json!(args));
                }
            }
            ret
        }
        _ => unreachable!(),
    };
    for modifier in src {
        if !ret.is_object() {
            ret = json!({ "pull": ret });
        }
        let inner_map = ret.as_object_mut().unwrap();
        match modifier.as_rule() {
            Rule::pull_as => {
                inner_map.insert(
                    "as".to_string(),
                    json!(modifier.into_inner().next().unwrap().as_str()),
                );
            }
            Rule::pull_limit => {
                let n = modifier.into_inner().next().unwrap().as_str();
                inner_map.insert("limit".to_string(), json!(str2usize(n)?));
            }
            Rule::pull_offset => {
                let n = modifier.into_inner().next().unwrap().as_str();
                inner_map.insert("offset".to_string(), json!(str2usize(n)?));
            }
            Rule::pull_default => {
                let d = build_expr(modifier.into_inner().next().unwrap())?;
                inner_map.insert("default".to_string(), d);
            }
            Rule::pull_recurse => {
                let d = build_expr(modifier.into_inner().next().unwrap())?;
                inner_map.insert("recurse".to_string(), d);
            }
            Rule::pull_depth => {
                let n = modifier.into_inner().next().unwrap().as_str();
                inner_map.insert("depth".to_string(), json!(str2usize(n)?));
            }
            _ => unreachable!(),
        }
    }
    Ok(json!(ret))
}

fn parse_limit_or_offset(src: Pair<'_>) -> Result<usize> {
    let src = src.into_inner().next().unwrap().as_str();
    str2usize(src)
}

fn str2usize(src: &str) -> Result<usize> {
    Ok(usize::from_str(&src.replace('_', ""))?)
}

fn parse_algo_rule(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    let out_symbol = src.next().unwrap().as_str();
    let algo_name = &src.next().unwrap().as_str().strip_suffix('!').unwrap();
    let mut algo_rels = vec![];
    let mut algo_opts = Map::default();
    for nxt in src {
        match nxt.as_rule() {
            Rule::algo_rel => {
                let inner = nxt.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::ident => algo_rels.push(json!({"rule": inner.as_str()})),
                    Rule::view_ident => {
                        algo_rels.push(json!({"view": inner.as_str().strip_prefix(':').unwrap()}))
                    }
                    Rule::algo_triple_rel => {
                        let mut inner = inner.into_inner();
                        let mut backward = false;
                        let ident = inner.next().unwrap();
                        let ident = match ident.as_rule() {
                            Rule::rev_triple_marker => {
                                backward = true;
                                inner.next().unwrap()
                            }
                            _ => ident,
                        };
                        let ident = ident.as_str();
                        algo_rels.push(json!({"triple": ident, "backward": backward}));
                    }
                    _ => unreachable!(),
                }
            }
            Rule::algo_opt_pair => {
                let mut inner = nxt.into_inner();
                let name = inner.next().unwrap().as_str();
                let val = inner.next().unwrap();
                let val = build_expr(val)?;
                algo_opts.insert(name.to_string(), val);
            }
            _ => unreachable!(),
        }
    }
    Ok(
        json!({"algo_out": out_symbol, "algo_name": algo_name, "relations": algo_rels, "options": algo_opts}),
    )
}

fn parse_rule(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    let head = src.next().unwrap();
    let (name, head) = parse_rule_head(head)?;
    let mut at = None;
    let mut body = src.next().unwrap();
    if body.as_rule() == Rule::expr {
        at = Some(build_expr(body)?);
        body = src.next().unwrap();
    }
    let mut body_clauses = vec![head];
    for atom_src in body.into_inner() {
        body_clauses.push(parse_disjunction(atom_src)?)
    }
    let mut ret = json!({"rule": name, "args": body_clauses});
    if let Some(at) = at {
        ret.as_object_mut().unwrap().insert("at".to_string(), at);
    }
    Ok(ret)
}

fn parse_rule_head(src: Pair<'_>) -> Result<(String, JsonValue)> {
    let mut src = src.into_inner();
    let name = src.next().unwrap().as_str();
    let args: Vec<_> = src.map(parse_rule_head_arg).try_collect()?;
    Ok((name.to_string(), json!(args)))
}

fn parse_rule_head_arg(src: Pair<'_>) -> Result<JsonValue> {
    let src = src.into_inner().next().unwrap();
    Ok(match src.as_rule() {
        Rule::var => json!(src.as_str()),
        Rule::aggr_arg => {
            let mut inner = src.into_inner();
            let aggr_name = inner.next().unwrap().as_str();
            let var = inner.next().unwrap().as_str();
            let args: Vec<_> = inner.map(build_expr).try_collect()?;
            json!({"aggr": aggr_name, "symb": var, "args": args})
        }
        _ => unreachable!(),
    })
}

fn parse_disjunction(src: Pair<'_>) -> Result<JsonValue> {
    let res: Vec<_> = src.into_inner().map(parse_atom).try_collect()?;
    Ok(if res.len() == 1 {
        res.into_iter().next().unwrap()
    } else {
        json!({ "disj": res })
    })
}

fn parse_atom(src: Pair<'_>) -> Result<JsonValue> {
    Ok(match src.as_rule() {
        Rule::rule_body => {
            let grouped: Vec<_> = src.into_inner().map(parse_disjunction).try_collect()?;
            json!({ "conj": grouped })
        }
        Rule::disjunction => parse_disjunction(src)?,
        Rule::triple => parse_triple(src)?,
        Rule::negation => {
            let inner = parse_atom(src.into_inner().next().unwrap())?;
            json!({ "not_exists": inner })
        }
        Rule::expr => build_expr(src)?,
        Rule::unify => {
            let mut src = src.into_inner();
            let var = src.next().unwrap().as_str();
            let expr = build_expr(src.next().unwrap())?;
            json!({"unify": var, "expr": expr})
        }
        Rule::unify_multi => {
            let mut src = src.into_inner();
            let var = src.next().unwrap().as_str();
            let expr = build_expr(src.next().unwrap())?;
            json!({"unify": var, "expr": expr, "multi": true})
        }
        Rule::rule_apply => {
            let mut src = src.into_inner();
            let name = src.next().unwrap().as_str();
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(build_expr)
                .try_collect()?;
            json!({"rule": name, "args": args})
        }
        Rule::view_apply => {
            let mut src = src.into_inner();
            let name = &src.next().unwrap().as_str()[1..];
            let args: Vec<_> = src
                .next()
                .unwrap()
                .into_inner()
                .map(build_expr)
                .try_collect()?;
            json!({"view": name, "args": args})
        }
        rule => unreachable!("{:?}", rule),
    })
}

fn parse_triple(src: Pair<'_>) -> Result<JsonValue> {
    let mut src = src.into_inner();
    Ok(json!([
        parse_triple_arg(src.next().unwrap())?,
        parse_triple_attr(src.next().unwrap())?,
        parse_triple_arg(src.next().unwrap())?
    ]))
}

fn parse_triple_arg(src: Pair<'_>) -> Result<JsonValue> {
    match src.as_rule() {
        Rule::expr => build_expr(src),
        Rule::triple_pull => {
            let mut src = src.into_inner();
            let attr = src.next().unwrap();
            let val = build_expr(src.next().unwrap())?;
            Ok(json!({ attr.as_str(): val }))
        }
        _ => unreachable!(),
    }
}

fn parse_triple_attr(src: Pair<'_>) -> Result<JsonValue> {
    let s = src.into_inner().map(|p| p.as_str()).join(".");
    Ok(json!(s))
}

lazy_static! {
    static ref PREC_CLIMBER: PrecClimber<Rule> = {
        use pest::prec_climber::Assoc::*;

        PrecClimber::new(vec![
            Operator::new(Rule::op_or, Left),
            Operator::new(Rule::op_and, Left),
            Operator::new(Rule::op_gt, Left)
                | Operator::new(Rule::op_lt, Left)
                | Operator::new(Rule::op_ge, Left)
                | Operator::new(Rule::op_le, Left),
            Operator::new(Rule::op_mod, Left),
            Operator::new(Rule::op_eq, Left) | Operator::new(Rule::op_ne, Left),
            Operator::new(Rule::op_add, Left)
                | Operator::new(Rule::op_sub, Left)
                | Operator::new(Rule::op_str_cat, Left),
            Operator::new(Rule::op_mul, Left) | Operator::new(Rule::op_div, Left),
            Operator::new(Rule::op_pow, Assoc::Right),
        ])
    };
}

fn build_expr_infix(
    lhs: Result<JsonValue>,
    op: Pair<'_>,
    rhs: Result<JsonValue>,
) -> Result<JsonValue> {
    let args = vec![lhs?, rhs?];
    let name = match op.as_rule() {
        Rule::op_add => "add",
        Rule::op_sub => "sub",
        Rule::op_mul => "mul",
        Rule::op_div => "div",
        Rule::op_mod => "mod",
        Rule::op_pow => "pow",
        Rule::op_eq => "eq",
        Rule::op_ne => "neq",
        Rule::op_gt => "gt",
        Rule::op_ge => "ge",
        Rule::op_lt => "lt",
        Rule::op_le => "le",
        Rule::op_str_cat => "str_cat",
        Rule::op_or => "or",
        Rule::op_and => "and",
        _ => unreachable!(),
    };
    Ok(json!({"op": name, "args": args}))
}

pub(crate) fn build_expr(pair: Pair<'_>) -> Result<JsonValue> {
    PREC_CLIMBER.climb(pair.into_inner(), build_unary, build_expr_infix)
}

fn build_unary(pair: Pair<'_>) -> Result<JsonValue> {
    match pair.as_rule() {
        Rule::expr => build_unary(pair.into_inner().next().unwrap()),
        Rule::grouping => build_expr(pair.into_inner().next().unwrap()),
        Rule::unary => {
            let s = pair.as_str();
            let mut inner = pair.into_inner();
            let p = inner.next().unwrap();
            let op = p.as_rule();
            Ok(match op {
                Rule::term => build_unary(p)?,
                Rule::var => json!(s),
                Rule::param => json!({ "param": s }),
                Rule::minus => {
                    let inner = build_unary(inner.next().unwrap())?;
                    json!({"op": "minus", "args": [inner]})
                }
                Rule::negate => {
                    let inner = build_unary(inner.next().unwrap())?;
                    json!({"op": "negate", "args": [inner]})
                }
                Rule::pos_int => {
                    let i = s.replace('_', "").parse::<i64>()?;
                    json!(i)
                }
                Rule::hex_pos_int => {
                    let i = parse_int(s, 16);
                    json!(i)
                }
                Rule::octo_pos_int => {
                    let i = parse_int(s, 8);
                    json!(i)
                }
                Rule::bin_pos_int => {
                    let i = parse_int(s, 2);
                    json!(i)
                }
                Rule::dot_float | Rule::sci_float => {
                    let f = s.replace('_', "").parse::<f64>()?;
                    json!(f)
                }
                Rule::null => JsonValue::Null,
                Rule::boolean => JsonValue::Bool(s == "true"),
                Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => {
                    let s = parse_string(p)?;
                    json!(s)
                }
                Rule::list => {
                    let mut collected = vec![];
                    for p in p.into_inner() {
                        collected.push(build_expr(p)?)
                    }
                    json!(collected)
                }
                Rule::apply => {
                    let mut p = p.into_inner();
                    let ident = p.next().unwrap().as_str();
                    let args: Vec<_> = p
                        .next()
                        .unwrap()
                        .into_inner()
                        .map(build_expr)
                        .try_collect()?;
                    json!({"op": ident, "args": args})
                }

                r => unreachable!("Encountered unknown op {:?}", r),
            })
        }
        _ => {
            println!("Unhandled rule {:?}", pair.as_rule());
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parse::cozoscript::query::parse_query_to_json;

    #[test]
    fn test_parse() {
        let src = r#"
        friend_of_friend[?a, ?b] := [?a person.friend ?b];
        friend_of_friend[?a, ?b] := friend_of_friend[?a, ?c], [?c person.friend ?b];

        ?[?a, ?b] := [?a person.friend ?b], [?a person.age ?age], ?age > 18 + 9;
        :limit 20;
        :offset 30;
        "#;
        let (_, parsed) = parse_query_to_json(src).unwrap();
        // println!("{}", to_string_pretty(&parsed).unwrap());
        println!("{}", parsed);
    }
}
