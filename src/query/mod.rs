use anyhow::Result;
use itertools::Itertools;
use serde_json::{json, Map};

use crate::data::attr::AttributeTyping;
use crate::data::json::JsonValue;
use crate::parse::query::OutSpec;
use crate::query::pull::CurrentPath;
use crate::query::relation::flatten_err;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;
use crate::Validity;

pub(crate) mod compile;
pub(crate) mod eval;
pub(crate) mod graph;
pub(crate) mod logical;
pub(crate) mod magic;
pub(crate) mod pull;
pub(crate) mod relation;
pub(crate) mod reorder;
pub(crate) mod stratify;

impl SessionTx {
    pub(crate) fn run_pull_on_query_results(
        &mut self,
        res_store: TempStore,
        out_spec: Option<OutSpec>,
        vld: Validity,
    ) -> Result<QueryResult<'_>> {
        match out_spec {
            None => Ok(Box::new(res_store.scan_all().map_ok(|tuple| {
                JsonValue::Array(tuple.0.into_iter().map(JsonValue::from).collect_vec())
            }))),
            Some((pull_specs, out_keys)) => {
                // type OutSpec = (Vec<(usize, Option<PullSpecs>)>, Option<Vec<String>>);
                Ok(Box::new(
                    res_store
                        .scan_all()
                        .map_ok(move |tuple| -> Result<JsonValue> {
                            let tuple = tuple.0;
                            let res_iter =
                                pull_specs.iter().map(|(idx, spec)| -> Result<JsonValue> {
                                    let val = tuple.get(*idx).unwrap();
                                    match spec {
                                        None => Ok(JsonValue::from(val.clone())),
                                        Some(specs) => {
                                            let eid = AttributeTyping::Ref
                                                .coerce_value(val.clone())?
                                                .get_entity_id()
                                                .unwrap();
                                            let mut collected = Default::default();
                                            let mut recursive_seen = Default::default();
                                            for (idx, spec) in specs.iter().enumerate() {
                                                self.pull(
                                                    eid,
                                                    vld,
                                                    spec,
                                                    0,
                                                    &specs,
                                                    CurrentPath::new(idx)?,
                                                    &mut collected,
                                                    &mut recursive_seen,
                                                )?;
                                            }
                                            Ok(JsonValue::Object(collected))
                                        }
                                    }
                                });
                            match &out_keys {
                                None => {
                                    let v: Vec<_> = res_iter.try_collect()?;
                                    Ok(json!(v))
                                }
                                Some(keys) => {
                                    let map: Map<_, _> = keys
                                        .iter()
                                        .zip(res_iter)
                                        .map(|(k, v)| match v {
                                            Ok(v) => Ok((k.clone(), v)),
                                            Err(e) => Err(e),
                                        })
                                        .try_collect()?;
                                    Ok(json!(map))
                                }
                            }
                        })
                        .map(flatten_err),
                ))
            }
        }
    }
}

pub type QueryResult<'a> = Box<dyn Iterator<Item = Result<JsonValue>> + 'a>;
