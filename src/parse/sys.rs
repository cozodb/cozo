use std::collections::BTreeSet;

use miette::{IntoDiagnostic, Result};

use crate::data::symb::Symbol;
use crate::parse::{Pairs, Rule};

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
            let i_str = inner.into_inner().next().unwrap().as_str();
            let i = u64::from_str_radix(i_str, 10).into_diagnostic()?;
            SysOp::KillRunning(i)
        }
        Rule::list_schema_op => SysOp::ListSchema,
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rels = inner
                .into_inner()
                .map(|v| Symbol::from(v.as_str()))
                .collect();
            SysOp::RemoveRelations(rels)
        }
        _ => unreachable!(),
    })
}
