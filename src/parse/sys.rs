/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{Diagnostic, Result};
use thiserror::Error;

use crate::data::program::InputProgram;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::query::parse_query;
use crate::parse::{ExtractSpan, Pairs, Rule, SourceSpan};
use crate::runtime::relation::AccessLevel;

pub(crate) enum SysOp {
    Compact,
    ListRelation(Symbol),
    ListRelations,
    ListRunning,
    KillRunning(u64),
    Explain(Box<InputProgram>),
    RemoveRelation(Vec<Symbol>),
    RenameRelation(Vec<(Symbol, Symbol)>),
    ShowTrigger(Symbol),
    SetTriggers(Symbol, Vec<String>, Vec<String>, Vec<String>),
    SetAccessLevel(Vec<Symbol>, AccessLevel),
}

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot interpret {0} as process ID")]
#[diagnostic(code(parser::not_proc_id))]
struct ProcessIdError(String, #[label] SourceSpan);

pub(crate) fn parse_sys(
    mut src: Pairs<'_>,
    param_pool: &BTreeMap<String, DataValue>,
) -> Result<SysOp> {
    let inner = src.next().unwrap();
    Ok(match inner.as_rule() {
        Rule::compact_op => SysOp::Compact,
        Rule::running_op => SysOp::ListRunning,
        Rule::kill_op => {
            let i_str = inner.into_inner().next().unwrap();
            let i = i_str
                .as_str()
                .parse::<u64>()
                .map_err(|_| ProcessIdError(i_str.as_str().to_string(), i_str.extract_span()))?;
            SysOp::KillRunning(i)
        }
        Rule::explain_op => {
            let prog = parse_query(inner.into_inner().next().unwrap().into_inner(), param_pool)?;
            SysOp::Explain(Box::new(prog))
        }
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rel = inner
                .into_inner()
                .map(|rels_p| Symbol::new(rels_p.as_str(), rels_p.extract_span()))
                .collect_vec();

            SysOp::RemoveRelation(rel)
        }
        Rule::list_relation_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::ListRelation(rel)
        }
        Rule::rename_relations_op => {
            let rename_pairs = inner
                .into_inner()
                .map(|pair| {
                    let mut src = pair.into_inner();
                    let rels_p = src.next().unwrap();
                    let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
                    let rels_p = src.next().unwrap();
                    let new_rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
                    (rel, new_rel)
                })
                .collect_vec();
            SysOp::RenameRelation(rename_pairs)
        }
        Rule::access_level_op => {
            let mut ps = inner.into_inner();
            let access_level = match ps.next().unwrap().as_str() {
                "normal" => AccessLevel::Normal,
                "protected" => AccessLevel::Protected,
                "read_only" => AccessLevel::ReadOnly,
                "hidden" => AccessLevel::Hidden,
                _ => unreachable!()
            };
            let mut rels = vec![];
            for rel_p in ps {
                let rel = Symbol::new(rel_p.as_str(), rel_p.extract_span());
                rels.push(rel)
            }
            SysOp::SetAccessLevel(rels, access_level)
        }
        Rule::trigger_relation_show_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::ShowTrigger(rel)
        }
        Rule::trigger_relation_op => {
            let mut src = inner.into_inner();
            let rels_p = src.next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            let mut puts = vec![];
            let mut rms = vec![];
            let mut replaces = vec![];
            for clause in src {
                let mut clause_inner = clause.into_inner();
                let op = clause_inner.next().unwrap();
                let script = clause_inner.next().unwrap();
                let script_str = script.as_str();
                parse_query(script.into_inner(), &Default::default())?;
                match op.as_rule() {
                    Rule::trigger_put => puts.push(script_str.to_string()),
                    Rule::trigger_rm => rms.push(script_str.to_string()),
                    Rule::trigger_replace => replaces.push(script_str.to_string()),
                    r => unreachable!("{:?}", r),
                }
            }
            SysOp::SetTriggers(rel, puts, rms, replaces)
        }
        rule => unreachable!("{:?}", rule),
    })
}
