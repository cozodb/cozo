/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::fts::ast::{FtsExpr, FtsLiteral, FtsNear};
use crate::parse::expr::parse_string;
use crate::parse::{CozoScriptParser, Pair, Rule};
use itertools::Itertools;
use lazy_static::lazy_static;
use miette::{IntoDiagnostic, Result};
use pest::pratt_parser::{Op, PrattParser};
use pest::Parser;
use smartstring::SmartString;

pub(crate) fn parse_fts_query(q: &str) -> Result<FtsExpr> {
    let mut pairs = CozoScriptParser::parse(Rule::fts_doc, q).into_diagnostic()?;
    let pairs = pairs.next().unwrap().into_inner();
    let pairs: Vec<_> = pairs
        .filter(|r| r.as_rule() != Rule::EOI)
        .map(parse_fts_expr)
        .try_collect()?;
    Ok(if pairs.len() == 1 {
        pairs.into_iter().next().unwrap()
    } else {
        FtsExpr::And(pairs)
    })
}

fn parse_fts_expr(pair: Pair<'_>) -> Result<FtsExpr> {
    debug_assert!(pair.as_rule() == Rule::fts_expr);
    let pairs = pair.into_inner();
    PRATT_PARSER
        .map_primary(build_term)
        .map_infix(build_infix)
        .parse(pairs)
}

fn build_infix(lhs: Result<FtsExpr>, op: Pair<'_>, rhs: Result<FtsExpr>) -> Result<FtsExpr> {
    let lhs = lhs?;
    let rhs = rhs?;
    Ok(match op.as_rule() {
        Rule::fts_and => FtsExpr::And(vec![lhs, rhs]),
        Rule::fts_or => FtsExpr::Or(vec![lhs, rhs]),
        Rule::fts_not => FtsExpr::Not(Box::new(lhs), Box::new(rhs)),
        _ => unreachable!("unexpected rule: {:?}", op.as_rule()),
    })
}

fn build_term(pair: Pair<'_>) -> Result<FtsExpr> {
    Ok(match pair.as_rule() {
        Rule::fts_grouped => {
            let collected: Vec<_> = pair.into_inner().map(parse_fts_expr).try_collect()?;
            if collected.len() == 1 {
                collected.into_iter().next().unwrap()
            } else {
                FtsExpr::And(collected)
            }
        }
        Rule::fts_near => {
            let mut literals = vec![];
            let mut distance = 10;
            for pair in pair.into_inner() {
                match pair.as_rule() {
                    Rule::pos_int => {
                        let i = pair
                            .as_str()
                            .replace('_', "")
                            .parse::<i64>()
                            .into_diagnostic()?;
                        distance = i as u32;
                    }
                    _ => literals.push(build_phrase(pair)?),
                }
            }
            FtsExpr::Near(FtsNear { literals, distance })
        }
        Rule::fts_phrase => FtsExpr::Literal(build_phrase(pair)?),
        r => panic!("unexpected rule: {:?}", r),
    })
}

fn build_phrase(pair: Pair<'_>) -> Result<FtsLiteral> {
    let mut inner = pair.into_inner();
    let kernel = inner.next().unwrap();
    let core_text = match kernel.as_rule() {
        Rule::fts_phrase_group => SmartString::from(kernel.as_str().trim()),
        Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => parse_string(kernel)?,
        _ => unreachable!("unexpected rule: {:?}", kernel.as_rule()),
    };
    let mut is_quoted = false;
    let mut booster = 1.0;
    for pair in inner {
        match pair.as_rule() {
            Rule::fts_prefix_marker => is_quoted = true,
            Rule::fts_booster => {
                let boosted = pair.into_inner().next().unwrap();
                match boosted.as_rule() {
                    Rule::dot_float => {
                        let f = boosted
                            .as_str()
                            .replace('_', "")
                            .parse::<f64>()
                            .into_diagnostic()?;
                        booster = f;
                    }
                    Rule::int => {
                        let i = boosted
                            .as_str()
                            .replace('_', "")
                            .parse::<i64>()
                            .into_diagnostic()?;
                        booster = i as f64;
                    }
                    _ => unreachable!("unexpected rule: {:?}", boosted.as_rule()),
                }
            }
            _ => unreachable!("unexpected rule: {:?}", pair.as_rule()),
        }
    }
    Ok(FtsLiteral {
        value: core_text,
        is_prefix: is_quoted,
        booster: booster.into(),
    })
}

lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::Assoc::*;

        PrattParser::new()
            .op(Op::infix(Rule::fts_not, Left))
            .op(Op::infix(Rule::fts_and, Left))
            .op(Op::infix(Rule::fts_or, Left))
    };
}

#[cfg(test)]
mod tests {
    use crate::fts::ast::{FtsExpr, FtsNear};
    use crate::parse::fts::parse_fts_query;

    #[test]
    fn test_parse() {
        let src = " hello world OR bye bye world";
        let res = parse_fts_query(src).unwrap().flatten();
        assert!(matches!(res, FtsExpr::Or(_)));
        let src = " hello world AND bye bye world";
        let res = parse_fts_query(src).unwrap().flatten();
        assert!(matches!(res, FtsExpr::And(_)));
        let src = " hello world NOT bye bye NOT 'ok, mates'";
        let res = parse_fts_query(src).unwrap().flatten();
        assert!(matches!(res, FtsExpr::Not(_, _)));
        let src = " NEAR(abc def \"ghi\"^22.8) ";
        let res = parse_fts_query(src).unwrap().flatten();
        assert!(matches!(res, FtsExpr::Near(FtsNear { distance: 10, .. })));
        println!("{:#?}", res);
    }
}
