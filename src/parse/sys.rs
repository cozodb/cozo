use std::collections::BTreeSet;

use miette::{bail, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::expr::build_expr;
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
    RemoveAttribute(Symbol),
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
        Rule::list_schema_op => SysOp::ListSchema,
        Rule::list_relations_op => SysOp::ListRelations,
        Rule::remove_relations_op => {
            let rels = inner
                .into_inner()
                .map(|v| Symbol::new(v.as_str(), v.extract_span()))
                .collect();
            SysOp::RemoveRelations(rels)
        }
        Rule::remove_attribute_op => {
            let attr_name_pair = inner.into_inner().next().unwrap();
            let attr_name = Symbol::new(attr_name_pair.as_str(), attr_name_pair.extract_span());
            SysOp::RemoveAttribute(attr_name)
        }
        _ => unreachable!(),
    })
}
