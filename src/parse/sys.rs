use std::collections::BTreeSet;

use miette::{bail, Diagnostic, ensure, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::id::Validity;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::{ExtractSpan, Pairs, ParseError, Rule, SourceSpan};
use crate::parse::expr::build_expr;
use crate::parse::tx::EntityRep;

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
    RemoveRelation(Symbol),
    RenameRelation(Symbol, Symbol),
    RemoveAttribute(Symbol),
    RenameAttribute(Symbol, Symbol),
    ExecuteLocalScript(SmartString<LazyCompact>),
    History {
        from: Option<Validity>,
        to: Option<Validity>,
        entities: Vec<EntityRep>,
        attributes: Vec<Symbol>,
    },
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
        Rule::remove_attribute_op => {
            let p = inner.into_inner().next().unwrap();
            let attr_name = Symbol::new(p.as_str(), p.extract_span());
            SysOp::RemoveAttribute(attr_name)
        }
        Rule::rename_attribute_op => {
            let mut src = inner.into_inner();
            let p = src.next().unwrap();
            let attr_name = Symbol::new(p.as_str(), p.extract_span());
            let p = src.next().unwrap();
            let new_attr_name = Symbol::new(p.as_str(), p.extract_span());
            SysOp::RenameAttribute(attr_name, new_attr_name)
        }
        Rule::history_op => {
            let mut from = None;
            let mut to = None;
            let mut attributes = vec![];
            let mut entities = vec![];
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::from_clause => {
                        let expr = build_expr(p.into_inner().next().unwrap(), &Default::default())?;
                        let vld = Validity::try_from(expr)?;
                        from = Some(vld)
                    }
                    Rule::to_clause => {
                        let expr = build_expr(p.into_inner().next().unwrap(), &Default::default())?;
                        let vld = Validity::try_from(expr)?;
                        to = Some(vld)
                    }
                    Rule::expr => {
                        let span = p.extract_span();
                        match build_expr(p, &Default::default())?.eval_to_const()? {
                            v @ DataValue::Str(_) => {
                                let e = v.get_entity_id().ok_or_else(|| ParseError { span })?;
                                entities.push(EntityRep::Id(e))
                            }
                            DataValue::List(c) => {
                                ensure!(c.len() == 2, ParseError { span });
                                let mut c = c.into_iter();
                                let attr = match c.next().unwrap() {
                                    DataValue::Str(s) => s,
                                    _ => bail!(ParseError { span }),
                                };
                                let val = c.next().unwrap();
                                entities.push(EntityRep::PullByKey(attr, val));
                            }
                            _ => {}
                        }
                    }
                    Rule::compound_ident => {
                        attributes.push(Symbol::new(SmartString::from(p.as_str()), p.extract_span()))
                    }
                    _ => unreachable!()
                }
            }
            SysOp::History {
                from,
                to,
                entities,
                attributes,
            }
        }
        _ => unreachable!(),
    })
}
