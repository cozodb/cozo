use std::collections::BTreeSet;

use miette::{bail, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::{ExtractSpan, Pairs, Rule, SourceSpan};
use crate::parse::expr::build_expr;

#[derive(
Debug,
Eq,
PartialEq,
Ord,
PartialOrd,
Copy,
Clone,
serde_derive::Serialize,
serde_derive::Deserialize,
)]
pub(crate) enum CompactTarget {
    Relations,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) enum SysOp {
    Compact(BTreeSet<CompactTarget>),
    ListRelations,
    ListRunning,
    KillRunning(u64),
    RemoveRelation(Symbol),
    RenameRelation(Symbol, Symbol),
    ExecuteLocalScript(SmartString<LazyCompact>),
}

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot interpret {0} as process ID")]
#[diagnostic(code(parser::not_proc_id))]
struct ProcessIdError(String, #[label] SourceSpan);

pub(crate) fn parse_sys(mut src: Pairs<'_>) -> Result<SysOp> {
    let inner = src.next().unwrap();
    Ok(match inner.as_rule() {
        Rule::compact_op => {
            let ops = inner
                .into_inner()
                .map(|v| match v.as_rule() {
                    Rule::compact_opt_relations => CompactTarget::Relations,
                    _ => unreachable!(),
                })
                .collect();
            SysOp::Compact(ops)
        }
        Rule::running_op => SysOp::ListRunning,
        Rule::kill_op => {
            let i_str = inner.into_inner().next().unwrap();
            let i = u64::from_str_radix(i_str.as_str(), 10)
                .map_err(|_| ProcessIdError(i_str.as_str().to_string(), i_str.extract_span()))?;
            SysOp::KillRunning(i)
        }
        Rule::execute_op => {
            let ex = inner.into_inner().next().unwrap();
            let span = ex.extract_span();
            let s = build_expr(ex, &Default::default())?;
            let path = match s.eval_to_const() {
                Ok(DataValue::Str(s)) => s,
                _ => {
                    #[derive(Debug, Error, Diagnostic)]
                    #[error("Expect path string")]
                    #[diagnostic(code(parser::bad_path_given))]
                    struct NotAPathError(#[label] SourceSpan);
                    bail!(NotAPathError(span));
                }
            };
            SysOp::ExecuteLocalScript(path)
        }
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rels_p = inner.into_inner().next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::RemoveRelation(rel)
        }
        Rule::rename_relations_op => {
            let mut src = inner.into_inner();
            let rels_p = src.next().unwrap();
            let rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            let rels_p = src.next().unwrap();
            let new_rel = Symbol::new(rels_p.as_str(), rels_p.extract_span());
            SysOp::RenameRelation(rel, new_rel)
        }
        _ => unreachable!(),
    })
}
