/*
 *  Copyright 2023, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use std::collections::BTreeMap;
use std::sync::Arc;

use either::{Left, Right};
use itertools::Itertools;
use miette::{Diagnostic, Result};
use smartstring::SmartString;
use thiserror::Error;

use crate::parse::query::parse_query;
use crate::parse::sys::parse_sys;
use crate::parse::{
    ExtractSpan, ImperativeProgram, ImperativeStmt, ImperativeStmtClause, ImperativeSysop, Pair,
    Rule, SourceSpan,
};
use crate::{DataValue, FixedRule, ValidityTs};

pub(crate) fn parse_imperative_block(
    src: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
    fixed_rules: &BTreeMap<String, Arc<Box<dyn FixedRule>>>,
    cur_vld: ValidityTs,
) -> Result<ImperativeProgram> {
    let mut collected = vec![];

    for pair in src.into_inner() {
        if pair.as_rule() == Rule::EOI {
            break;
        }
        collected.push(parse_imperative_stmt(
            pair,
            param_pool,
            fixed_rules,
            cur_vld,
        )?);
    }

    Ok(collected)
}

#[derive(Debug, Error, Diagnostic)]
#[error("cannot manipulate permanent relation in imperative script")]
#[diagnostic(code(parser::manipulate_perm_rel_in_script))]
struct CannotManipulatePermRel(#[label] SourceSpan);

#[derive(Debug, Error, Diagnostic)]
#[error("duplicate marker found")]
#[diagnostic(code(parser::dup_marker))]
struct DuplicateMarker(#[label] SourceSpan);

fn parse_imperative_stmt(
    pair: Pair<'_>,
    param_pool: &BTreeMap<String, DataValue>,
    fixed_rules: &BTreeMap<String, Arc<Box<dyn FixedRule>>>,
    cur_vld: ValidityTs,
) -> Result<ImperativeStmt> {
    Ok(match pair.as_rule() {
        Rule::break_stmt => {
            let span = pair.extract_span();
            let target = pair
                .into_inner()
                .next()
                .map(|p| SmartString::from(p.as_str()));
            ImperativeStmt::Break { target, span }
        }
        Rule::continue_stmt => {
            let span = pair.extract_span();
            let target = pair
                .into_inner()
                .next()
                .map(|p| SmartString::from(p.as_str()));
            ImperativeStmt::Continue { target, span }
        }
        Rule::return_stmt => {
            // let span = pair.extract_span();
            let mut rets = vec![];
            for p in pair.into_inner() {
                match p.as_rule() {
                    Rule::ident | Rule::underscore_ident => {
                        let rel = SmartString::from(p.as_str());
                        rets.push(Right(rel));
                    }
                    Rule::query_script_inner => {
                        let mut src = p.into_inner();
                        let prog = parse_query(
                            src.next().unwrap().into_inner(),
                            param_pool,
                            fixed_rules,
                            cur_vld,
                        )?;
                        let store_as = src.next().map(|p| SmartString::from(p.as_str().trim()));
                        rets.push(Left(ImperativeStmtClause { prog, store_as }))
                    }
                    _ => unreachable!(),
                }
            }
            ImperativeStmt::Return { returns: rets }
        }
        Rule::if_chain | Rule::if_not_chain => {
            let negated = pair.as_rule() == Rule::if_not_chain;
            let mut inner = pair.into_inner();
            let condition = inner.next().unwrap();
            let cond = match condition.as_rule() {
                Rule::underscore_ident => Left(SmartString::from(condition.as_str())),
                Rule::imperative_clause => {
                    let mut src = condition.into_inner();
                    let prog = parse_query(
                        src.next().unwrap().into_inner(),
                        param_pool,
                        fixed_rules,
                        cur_vld,
                    )?;
                    let store_as = src.next().map(|p| SmartString::from(p.as_str().trim()));
                    Right(ImperativeStmtClause { prog, store_as })
                }
                _ => unreachable!(),
            };
            let body = inner
                .next()
                .unwrap()
                .into_inner()
                .map(|p| parse_imperative_stmt(p, param_pool, fixed_rules, cur_vld))
                .try_collect()?;
            let else_body = match inner.next() {
                None => vec![],
                Some(rest) => rest
                    .into_inner()
                    .map(|p| parse_imperative_stmt(p, param_pool, fixed_rules, cur_vld))
                    .try_collect()?,
            };
            ImperativeStmt::If {
                condition: cond,
                then_branch: body,
                else_branch: else_body,
                negated,
            }
        }
        Rule::loop_block => {
            let mut inner = pair.into_inner();
            let mut mark = None;
            let mut nxt = inner.next().unwrap();
            if nxt.as_rule() == Rule::ident {
                mark = Some(SmartString::from(nxt.as_str()));
                nxt = inner.next().unwrap();
            }
            let body = parse_imperative_block(nxt, param_pool, fixed_rules, cur_vld)?;
            ImperativeStmt::Loop { label: mark, body }
        }
        Rule::temp_swap => {
            // let span = pair.extract_span();
            let mut pairs = pair.into_inner();
            let left = pairs.next().unwrap();
            let left_name = left.as_str();
            let right = pairs.next().unwrap();
            let right_name = right.as_str();

            ImperativeStmt::TempSwap {
                left: SmartString::from(left_name),
                right: SmartString::from(right_name),
            }
        }
        Rule::debug_stmt => {
            // let span = pair.extract_span();
            let name_p = pair.into_inner().next().unwrap();
            let name = name_p.as_str();

            ImperativeStmt::TempDebug {
                temp: SmartString::from(name),
            }
        }
        Rule::imperative_sysop => {
            let mut src = pair.into_inner();
            let sysop = parse_sys(
                src.next().unwrap().into_inner(),
                param_pool,
                fixed_rules,
                cur_vld,
            )?;
            let store_as = src.next().map(|p| SmartString::from(p.as_str().trim()));
            ImperativeStmt::SysOp {
                sysop: ImperativeSysop { sysop, store_as },
            }
        }
        Rule::imperative_clause => {
            let mut src = pair.into_inner();
            let prog = parse_query(
                src.next().unwrap().into_inner(),
                param_pool,
                fixed_rules,
                cur_vld,
            )?;
            let store_as = src.next().map(|p| SmartString::from(p.as_str().trim()));
            ImperativeStmt::Program {
                prog: ImperativeStmtClause { prog, store_as },
            }
        }
        Rule::ignore_error_script => {
            let pair = pair.into_inner().next().unwrap();
            let mut src = pair.into_inner();
            let prog = parse_query(
                src.next().unwrap().into_inner(),
                param_pool,
                fixed_rules,
                cur_vld,
            )?;
            let store_as = src.next().map(|p| SmartString::from(p.as_str().trim()));
            ImperativeStmt::IgnoreErrorProgram {
                prog: ImperativeStmtClause { prog, store_as },
            }
        }
        r => unreachable!("{r:?}"),
    })
}
