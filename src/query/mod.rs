use std::collections::BTreeMap;

use anyhow::Result;
use itertools::Itertools;
use serde_json::json;

use crate::data::attr::AttributeTyping;
use crate::data::json::JsonValue;
use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::query::compile::{DatalogProgram, QueryCompilationError};
use crate::query::pull::{CurrentPath, PullSpecs};
use crate::query::relation::flatten_err;
use crate::runtime::transact::SessionTx;
use crate::Validity;

pub(crate) mod compile;
pub(crate) mod eval;
pub(crate) mod pull;
pub(crate) mod relation;
pub(crate) mod logical;
pub(crate) mod graph;
pub(crate) mod stratify;

impl SessionTx {
    pub fn run_query(&mut self, payload: &JsonValue) -> Result<QueryResult<'_>> {
        let vld = match payload.get("since") {
            None => Validity::current(),
            Some(v) => Validity::try_from(v)?,
        };
        let q = payload.get("q").ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(payload.clone(), "expect key 'q'".to_string())
        })?;
        let rules_payload = q.as_array().ok_or_else(|| {
            QueryCompilationError::UnexpectedForm(q.clone(), "expect array".to_string())
        })?;
        if rules_payload.is_empty() {
            return Err(QueryCompilationError::UnexpectedForm(
                payload.clone(),
                "empty rules".to_string(),
            )
                .into());
        }
        let prog = if rules_payload.first().unwrap().is_array() {
            let q = json!([{"rule": "?", "args": rules_payload}]);
            self.parse_rule_sets(&q, vld)?
        } else {
            self.parse_rule_sets(q, vld)?
        };
        match payload.get("out") {
            None => {
                let res = self.stratified_evaluate(&prog)?;
                Ok(Box::new(res.scan_all().map_ok(|tuple| {
                    JsonValue::Array(tuple.0.into_iter().map(JsonValue::from).collect_vec())
                })))
            }
            Some(JsonValue::Object(out_spec_map)) => {
                let vld = prog.get(&PROG_ENTRY).unwrap().rules.first().unwrap().vld;
                let out_spec = out_spec_map.values().cloned().collect_vec();
                let pull_specs = self.parse_pull_specs_for_query(&out_spec, &prog)?;
                let res = self.stratified_evaluate(&prog)?;
                let map_keys = out_spec_map.keys().cloned().collect_vec();
                Ok(Box::new(
                    res.scan_all()
                        .map_ok(move |tuple| -> Result<JsonValue> {
                            let tuple = tuple.0;
                            let collected: Vec<_> = pull_specs
                                .iter()
                                .map(|(idx, spec)| -> Result<JsonValue> {
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
                                })
                                .try_collect()?;
                            let res = map_keys.iter().cloned().zip(collected).collect();
                            Ok(JsonValue::Object(res))
                        })
                        .map(flatten_err),
                ))
            }
            Some(JsonValue::Array(out_spec)) => {
                let vld = prog.get(&PROG_ENTRY).unwrap().rules.first().unwrap().vld;
                let pull_specs = self.parse_pull_specs_for_query(out_spec, &prog)?;
                let res = self.stratified_evaluate(&prog)?;
                Ok(Box::new(
                    res.scan_all()
                        .map_ok(move |tuple| -> Result<JsonValue> {
                            let tuple = tuple.0;
                            let collected: Vec<_> = pull_specs
                                .iter()
                                .map(|(idx, spec)| -> Result<JsonValue> {
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
                                                    specs,
                                                    CurrentPath::new(idx)?,
                                                    &mut collected,
                                                    &mut recursive_seen,
                                                )?;
                                            }
                                            Ok(JsonValue::Object(collected))
                                        }
                                    }
                                })
                                .try_collect()?;
                            Ok(JsonValue::Array(collected))
                        })
                        .map(flatten_err),
                ))
            }
            Some(v) => Err(QueryCompilationError::UnexpectedForm(
                v.clone(),
                "out specification should be an array".to_string(),
            )
                .into()),
        }
    }
    fn parse_pull_specs_for_query(
        &mut self,
        out_spec: &Vec<JsonValue>,
        prog: &DatalogProgram,
    ) -> Result<Vec<(usize, Option<PullSpecs>)>> {
        let entry_bindings: BTreeMap<_, _> = prog
            .get(&PROG_ENTRY)
            .unwrap()
            .rules
            .first()
            .unwrap()
            .head
            .iter()
            .enumerate()
            .map(|(i, h)| (&h.name, i))
            .collect();
        out_spec
            .iter()
            .map(|spec| -> Result<(usize, Option<PullSpecs>)> {
                match spec {
                    JsonValue::String(s) => {
                        let kw = Keyword::from(s as &str);
                        let idx = *entry_bindings
                            .get(&kw)
                            .ok_or_else(|| QueryCompilationError::BindingNotFound(kw.clone()))?;
                        Ok((idx, None))
                    }
                    JsonValue::Object(m) => {
                        let kw = m
                            .get("pull")
                            .ok_or_else(|| {
                                QueryCompilationError::UnexpectedForm(
                                    JsonValue::Object(m.clone()),
                                    "expect key 'pull'".to_string(),
                                )
                            })?
                            .as_str()
                            .ok_or_else(|| {
                                QueryCompilationError::UnexpectedForm(
                                    JsonValue::Object(m.clone()),
                                    "expect key 'pull' to have a binding as value".to_string(),
                                )
                            })?;
                        let kw = Keyword::from(kw);
                        let idx = *entry_bindings
                            .get(&kw)
                            .ok_or_else(|| QueryCompilationError::BindingNotFound(kw.clone()))?;
                        let spec = m.get("spec").ok_or_else(|| {
                            QueryCompilationError::UnexpectedForm(
                                JsonValue::Object(m.clone()),
                                "expect key 'spec'".to_string(),
                            )
                        })?;
                        let specs = self.parse_pull(spec, 0)?;
                        Ok((idx, Some(specs)))
                    }
                    v => Err(QueryCompilationError::UnexpectedForm(
                        v.clone(),
                        "expect binding or map".to_string(),
                    )
                        .into()),
                }
            })
            .try_collect()
    }
}

pub type QueryResult<'a> = Box<dyn Iterator<Item=Result<JsonValue>> + 'a>;
