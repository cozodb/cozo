use std::collections::BTreeSet;

use miette::{Diagnostic, Result};
use thiserror::Error;

use crate::data::symb::Symbol;
use crate::parse::{ExtractSpan, Pairs, Rule, SourceSpan};

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
    Triples,
    Relations,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) enum SysOp {
    Compact(BTreeSet<CompactTarget>),
    ListSchema,
    ListRelations,
    ListRunning,
    KillRunning(u64),
    RemoveRelations(Vec<Symbol>),
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
                    Rule::compact_opt_triples => CompactTarget::Triples,
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
        Rule::list_schema_op => SysOp::ListSchema,
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rels = inner
                .into_inner()
                .map(|v| Symbol::new(v.as_str(), v.extract_span()))
                .collect();
            SysOp::RemoveRelations(rels)
        }
        _ => unreachable!(),
    })
}
