/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use either::{Left, Right};
use itertools::Itertools;
use lazy_static::lazy_static;
use miette::{
    bail, ensure, Diagnostic, GraphicalReportHandler, GraphicalTheme, IntoDiagnostic,
    JSONReportHandler, Result, WrapErr,
};
use serde_json::{json, Map};
use smartstring::SmartString;
use thiserror::Error;

use crate::data::json::JsonValue;
use crate::data::program::{InputProgram, QueryAssertion, RelationOp};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::parse::sys::SysOp;
use crate::parse::{parse_script, CozoScript, SourceSpan};
use crate::query::compile::{CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::query::relation::{
    FilteredRA, InMemRelationRA, InnerJoin, NegJoin, RelAlgebra, ReorderRA, StoredRA, UnificationRA,
};
use crate::runtime::relation::{RelationHandle, RelationId};
use crate::runtime::transact::SessionTx;
use crate::storage::{Storage, StoreTx};

struct RunningQueryHandle {
    started_at: f64,
    poison: Poison,
}

struct RunningQueryCleanup {
    id: u64,
    running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
}

impl Drop for RunningQueryCleanup {
    fn drop(&mut self) {
        let mut map = self.running_queries.lock().unwrap();
        if let Some(handle) = map.remove(&self.id) {
            handle.poison.0.store(true, Ordering::Relaxed);
        }
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct DbManifest {
    pub storage_version: u64,
}

/// The database object of Cozo.
#[derive(Clone)]
pub struct Db<S> {
    db: S,
    relation_store_id: Arc<AtomicU64>,
    queries_count: Arc<AtomicU64>,
    running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
}

impl<S> Debug for Db<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Db")
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("Initialization of database failed")]
#[diagnostic(code(db::init))]
pub(crate) struct BadDbInit(#[help] pub(crate) String);

lazy_static! {
    static ref TEXT_ERR_HANDLER: GraphicalReportHandler =
        miette::GraphicalReportHandler::new().with_theme(GraphicalTheme::unicode());
    static ref JSON_ERR_HANDLER: JSONReportHandler = miette::JSONReportHandler::new();
}

impl<'s, S: Storage<'s>> Db<S> {
    /// Create a new database object with the given storage.
    /// You must call [`initialize`](Self::initialize) immediately after creation.
    /// Due to lifetime restrictions we are not able to call that for you automatically.
    pub fn new(storage: S) -> Result<Self> {
        let ret = Self {
            db: storage,
            relation_store_id: Arc::new(Default::default()),
            queries_count: Arc::new(Default::default()),
            running_queries: Arc::new(Mutex::new(Default::default())),
        };
        Ok(ret)
    }

    /// Must be called after creation of the database to initialize the runtime state.
    pub fn initialize(&'s self) -> Result<()> {
        self.load_last_ids()?;
        Ok(())
    }

    fn compact_relation(&'s self) -> Result<()> {
        let l = Tuple::default().encode_as_key(RelationId(0));
        let u = Tuple(vec![DataValue::Bot]).encode_as_key(RelationId(u64::MAX));
        self.db.range_compact(&l, &u)?;
        Ok(())
    }

    fn load_last_ids(&'s self) -> Result<()> {
        let mut tx = self.transact()?;
        self.relation_store_id
            .store(tx.load_last_relation_store_id()?.0, Ordering::Release);
        tx.commit_tx()?;
        Ok(())
    }
    fn transact(&'s self) -> Result<SessionTx<'_>> {
        let ret = SessionTx {
            tx: Box::new(self.db.transact(false)?),
            mem_store_id: Default::default(),
            relation_store_id: self.relation_store_id.clone(),
        };
        Ok(ret)
    }
    fn transact_write(&'s self) -> Result<SessionTx<'_>> {
        let ret = SessionTx {
            tx: Box::new(self.db.transact(true)?),
            mem_store_id: Default::default(),
            relation_store_id: self.relation_store_id.clone(),
        };
        Ok(ret)
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    pub fn run_script(
        &'s self,
        payload: &str,
        params: &Map<String, JsonValue>,
    ) -> Result<JsonValue> {
        let start = Instant::now();
        match self.do_run_script(payload, params) {
            Ok(mut json) => {
                let took = start.elapsed().as_secs_f64();
                let map = json.as_object_mut().unwrap();
                map.insert("ok".to_string(), json!(true));
                map.insert("took".to_string(), json!(took));
                Ok(json)
            }
            err => err,
        }
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    /// Fold any error into the return JSON itself.
    pub fn run_script_fold_err(
        &'s self,
        payload: &str,
        params: &Map<String, JsonValue>,
    ) -> JsonValue {
        match self.run_script(payload, params) {
            Ok(json) => json,
            Err(mut err) => {
                if err.source_code().is_none() {
                    err = err.with_source_code(payload.to_string());
                }
                let mut text_err = String::new();
                let mut json_err = String::new();
                TEXT_ERR_HANDLER
                    .render_report(&mut text_err, err.as_ref())
                    .expect("render text error failed");
                JSON_ERR_HANDLER
                    .render_report(&mut json_err, err.as_ref())
                    .expect("render json error failed");
                let mut json: serde_json::Value =
                    serde_json::from_str(&json_err).expect("parse rendered json error failed");
                let map = json.as_object_mut().unwrap();
                map.insert("ok".to_string(), json!(false));
                map.insert("display".to_string(), json!(text_err));
                json
            }
        }
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters formatted as JSON.
    pub fn run_script_str(&'s self, payload: &str, params: &str) -> String {
        let params_json = if params.is_empty() {
            Map::default()
        } else {
            match serde_json::from_str::<serde_json::Value>(params) {
                Ok(serde_json::Value::Object(map)) => map,
                Ok(_) => {
                    return json!({"ok": false, "message": "params argument is not valid JSON"})
                        .to_string()
                }
                Err(_) => {
                    return json!({"ok": false, "message": "params argument is not a JSON map"})
                        .to_string()
                }
            }
        };
        self.run_script_fold_err(payload, &params_json).to_string()
    }
    fn do_run_script(
        &'s self,
        payload: &str,
        params: &Map<String, JsonValue>,
    ) -> Result<JsonValue> {
        let param_pool = params
            .iter()
            .map(|(k, v)| (k.clone(), DataValue::from(v)))
            .collect();
        match parse_script(payload, &param_pool)? {
            CozoScript::Multi(ps) => {
                let is_write = ps.iter().any(|p| p.out_opts.store_relation.is_some());
                let mut cleanups = vec![];
                let mut res = json!(null);
                {
                    let mut tx = if is_write {
                        self.transact_write()?
                    } else {
                        self.transact()?
                    };

                    for p in ps {
                        let sleep_opt = p.out_opts.sleep;
                        let (q_res, q_cleanups) = self.run_query(&mut tx, p)?;
                        res = q_res;
                        cleanups.extend(q_cleanups);
                        if let Some(secs) = sleep_opt {
                            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
                        }
                    }
                    if is_write {
                        tx.commit_tx()?;
                    } else {
                        tx.commit_tx()?;
                        assert!(cleanups.is_empty(), "non-empty cleanups on read-only tx");
                    }
                }

                for (lower, upper) in cleanups {
                    self.db.del_range(&lower, &upper)?;
                }
                Ok(res)
            }
            CozoScript::Sys(op) => self.run_sys_op(op),
        }
    }
    fn explain_compiled(&self, strata: &[CompiledProgram]) -> Result<JsonValue> {
        let mut ret: Vec<JsonValue> = vec![];
        const STRATUM: &str = "stratum";
        const ATOM_IDX: &str = "atom_idx";
        const OP: &str = "op";
        const RULE_IDX: &str = "rule_idx";
        const RULE_NAME: &str = "rule";
        const REF_NAME: &str = "ref";
        const OUT_BINDINGS: &str = "out_relation";
        const JOINS_ON: &str = "joins_on";
        const FILTERS: &str = "filters/expr";

        let headers = [
            STRATUM,
            RULE_IDX,
            RULE_NAME,
            ATOM_IDX,
            OP,
            REF_NAME,
            JOINS_ON,
            FILTERS,
            OUT_BINDINGS,
        ];

        for (stratum, p) in strata.iter().enumerate() {
            let mut clause_idx = -1;
            for (rule_name, v) in p {
                match v {
                    CompiledRuleSet::Rules(rules) => {
                        for CompiledRule { aggr, relation, .. } in rules.iter() {
                            clause_idx += 1;
                            let mut ret_for_relation = vec![];
                            let mut rel_stack = vec![relation];
                            let mut idx = 0;
                            let mut atom_type = "out";
                            for (a, _) in aggr.iter().flatten() {
                                if a.is_meet {
                                    if atom_type == "out" {
                                        atom_type = "meet_aggr_out";
                                    }
                                } else {
                                    atom_type = "aggr_out";
                                }
                            }

                            ret_for_relation.push(json!({
                                STRATUM: stratum,
                                ATOM_IDX: idx,
                                OP: atom_type,
                                RULE_IDX: clause_idx,
                                RULE_NAME: rule_name.to_string(),
                                OUT_BINDINGS: relation.bindings_after_eliminate().into_iter().map(|v| v.to_string()).collect_vec()
                            }));
                            idx += 1;

                            while let Some(rel) = rel_stack.pop() {
                                let (atom_type, ref_name, joins_on, filters) = match rel {
                                    r @ RelAlgebra::Fixed(..) => {
                                        if r.is_unit() {
                                            continue;
                                        }
                                        ("fixed", json!(null), json!(null), json!(null))
                                    }
                                    RelAlgebra::InMem(InMemRelationRA {
                                        storage, filters, ..
                                    }) => (
                                        "load_mem",
                                        json!(storage.rule_name.to_string()),
                                        json!(null),
                                        json!(filters.iter().map(|f| f.to_string()).collect_vec()),
                                    ),
                                    RelAlgebra::Stored(StoredRA {
                                        storage, filters, ..
                                    }) => (
                                        "load_stored",
                                        json!(format!(":{}", storage.name)),
                                        json!(null),
                                        json!(filters.iter().map(|f| f.to_string()).collect_vec()),
                                    ),
                                    RelAlgebra::Join(inner) => {
                                        if inner.left.is_unit() {
                                            rel_stack.push(&inner.right);
                                            continue;
                                        }
                                        let t = inner.join_type();
                                        let InnerJoin {
                                            left,
                                            right,
                                            joiner,
                                            ..
                                        } = inner.as_ref();
                                        rel_stack.push(left);
                                        rel_stack.push(right);
                                        (t, json!(null), json!(joiner.as_map()), json!(null))
                                    }
                                    RelAlgebra::NegJoin(inner) => {
                                        let t = inner.join_type();
                                        let NegJoin {
                                            left,
                                            right,
                                            joiner,
                                            ..
                                        } = inner.as_ref();
                                        rel_stack.push(left);
                                        rel_stack.push(right);
                                        (t, json!(null), json!(joiner.as_map()), json!(null))
                                    }
                                    RelAlgebra::Reorder(ReorderRA { relation, .. }) => {
                                        rel_stack.push(relation);
                                        ("reorder", json!(null), json!(null), json!(null))
                                    }
                                    RelAlgebra::Filter(FilteredRA { parent, pred, .. }) => {
                                        rel_stack.push(parent);
                                        (
                                            "filter",
                                            json!(null),
                                            json!(null),
                                            json!(pred.iter().map(|f| f.to_string()).collect_vec()),
                                        )
                                    }
                                    RelAlgebra::Unification(UnificationRA {
                                        parent,
                                        binding,
                                        expr,
                                        is_multi,
                                        ..
                                    }) => {
                                        rel_stack.push(parent);
                                        (
                                            if *is_multi { "multi-unify" } else { "unify" },
                                            json!(binding.name),
                                            json!(null),
                                            json!(expr.to_string()),
                                        )
                                    }
                                };
                                ret_for_relation.push(json!({
                                    STRATUM: stratum,
                                    ATOM_IDX: idx,
                                    OP: atom_type,
                                    RULE_IDX: clause_idx,
                                    RULE_NAME: rule_name.to_string(),
                                    REF_NAME: ref_name,
                                    OUT_BINDINGS: rel.bindings_after_eliminate().into_iter().map(|v| v.to_string()).collect_vec(),
                                    JOINS_ON: joins_on,
                                    FILTERS: filters,
                                }));
                                idx += 1;
                            }
                            ret_for_relation.reverse();
                            ret.extend(ret_for_relation)
                        }
                    }
                    CompiledRuleSet::Algo(_) => ret.push(json!({
                        STRATUM: stratum,
                        ATOM_IDX: 0,
                        OP: "algo",
                        RULE_IDX: 0,
                        RULE_NAME: rule_name.to_string(),
                    })),
                }
            }
        }

        let ret = ret
            .into_iter()
            .map(|m| {
                headers
                    .iter()
                    .map(|i| m.get(*i).unwrap_or(&JsonValue::Null).clone())
                    .collect_vec()
            })
            .collect_vec();

        Ok(json!({"headers": headers, "rows": ret}))
    }
    fn run_sys_op(&'s self, op: SysOp) -> Result<JsonValue> {
        match op {
            SysOp::Explain(prog) => {
                let mut tx = self.transact()?;
                let program = prog
                    .to_normalized_program(&tx)?
                    .stratify()?
                    .magic_sets_rewrite(&tx)?;
                let (compiled, _) = tx.stratified_magic_compile(&program)?;
                tx.commit_tx()?;
                self.explain_compiled(&compiled)
            }
            SysOp::Compact => {
                self.compact_relation()?;
                Ok(json!({"headers": ["status"], "rows": [["OK"]]}))
            }
            SysOp::ListRelations => self.list_relations(),
            SysOp::RemoveRelation(rel_names) => {
                let mut bounds = vec![];
                {
                    let mut tx = self.transact_write()?;
                    for rs in rel_names {
                        let bound = tx.destroy_relation(&rs)?;
                        bounds.push(bound);
                    }
                    tx.commit_tx()?;
                }
                for (lower, upper) in bounds {
                    self.db.del_range(&lower, &upper)?;
                }
                Ok(json!({"headers": ["status"], "rows": [["OK"]]}))
            }
            SysOp::ListRelation(rs) => self.list_relation(&rs),
            SysOp::RenameRelation(rename_pairs) => {
                let mut tx = self.transact_write()?;
                for (old, new) in rename_pairs {
                    tx.rename_relation(old, new)?;
                }
                tx.commit_tx()?;
                Ok(json!({"headers": ["status"], "rows": [["OK"]]}))
            }
            SysOp::ListRunning => self.list_running(),
            SysOp::KillRunning(id) => {
                let queries = self.running_queries.lock().unwrap();
                Ok(match queries.get(&id) {
                    None => {
                        json!({"headers": ["status"], "rows": [["NOT_FOUND"]]})
                    }
                    Some(handle) => {
                        handle.poison.0.store(true, Ordering::Relaxed);
                        json!({"headers": ["status"], "rows": [["KILLING"]]})
                    }
                })
            }
            SysOp::ShowTrigger(name) => {
                let mut tx = self.transact()?;
                let rel = tx.get_relation(&name, false)?;
                let mut ret = vec![];
                for (i, trigger) in rel.put_triggers.iter().enumerate() {
                    ret.push(json!(["put", i, trigger]))
                }
                for (i, trigger) in rel.rm_triggers.iter().enumerate() {
                    ret.push(json!(["rm", i, trigger]))
                }
                for (i, trigger) in rel.replace_triggers.iter().enumerate() {
                    ret.push(json!(["replace", i, trigger]))
                }
                tx.commit_tx()?;
                Ok(json!({"headers": ["type", "idx", "trigger"], "rows": ret}))
            }
            SysOp::SetTriggers(name, puts, rms, replaces) => {
                let mut tx = self.transact_write()?;
                tx.set_relation_triggers(name, puts, rms, replaces)?;
                tx.commit_tx()?;
                Ok(json!({"headers": ["status"], "rows": [["OK"]]}))
            }
            SysOp::SetAccessLevel(names, level) => {
                let mut tx = self.transact_write()?;
                for name in names {
                    tx.set_access_level(name, level)?;
                }
                tx.commit_tx()?;
                Ok(json!({"headers": ["status"], "rows": [["OK"]]}))
            }
        }
    }
    pub(crate) fn run_query(
        &self,
        tx: &mut SessionTx<'_>,
        input_program: InputProgram,
    ) -> Result<(JsonValue, Vec<(Vec<u8>, Vec<u8>)>)> {
        let mut clean_ups = vec![];
        if let Some((meta, op)) = &input_program.out_opts.store_relation {
            if *op == RelationOp::Create {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Stored relation {0} conflicts with an existing one")]
                #[diagnostic(code(eval::stored_relation_conflict))]
                struct StoreRelationConflict(String);

                ensure!(
                    !tx.relation_exists(&meta.name)?,
                    StoreRelationConflict(meta.name.to_string())
                )
            } else if *op != RelationOp::Replace {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Stored relation {0} not found")]
                #[diagnostic(code(eval::stored_relation_not_found))]
                struct StoreRelationNotFoundError(String);

                let existing = tx.get_relation(&meta.name, true)?;

                ensure!(
                    tx.relation_exists(&meta.name)?,
                    StoreRelationNotFoundError(meta.name.to_string())
                );

                existing.ensure_compatible(meta)?;
            }
        };
        let program = input_program
            .to_normalized_program(tx)?
            .stratify()?
            .magic_sets_rewrite(tx)?;
        let (compiled, stores) = tx.stratified_magic_compile(&program)?;

        let poison = Poison::default();
        if let Some(secs) = input_program.out_opts.timeout {
            poison.set_timeout(secs)?;
        }
        let id = self.queries_count.fetch_add(1, Ordering::AcqRel);

        let now = SystemTime::now();
        let since_the_epoch = now
            .duration_since(UNIX_EPOCH)
            .into_diagnostic()?
            .as_secs_f64();

        let handle = RunningQueryHandle {
            started_at: since_the_epoch,
            poison: poison.clone(),
        };
        self.running_queries.lock().unwrap().insert(id, handle);
        let _guard = RunningQueryCleanup {
            id,
            running_queries: self.running_queries.clone(),
        };

        let (result, early_return) = tx.stratified_magic_evaluate(
            &compiled,
            &stores,
            if input_program.out_opts.sorters.is_empty() {
                input_program.out_opts.num_to_take()
            } else {
                None
            },
            if input_program.out_opts.sorters.is_empty() {
                input_program.out_opts.offset
            } else {
                None
            },
            poison,
        )?;
        if let Some(assertion) = &input_program.out_opts.assertion {
            match assertion {
                QueryAssertion::AssertNone(span) => {
                    if let Some(tuple) = result.scan_all().next() {
                        let tuple = tuple?;

                        #[derive(Debug, Error, Diagnostic)]
                        #[error(
                            "The query is asserted to return no result, but a tuple {0:?} is found"
                        )]
                        #[diagnostic(code(eval::assert_none_failure))]
                        struct AssertNoneFailure(Tuple, #[label] SourceSpan);
                        bail!(AssertNoneFailure(tuple, *span))
                    }
                }
                QueryAssertion::AssertSome(span) => {
                    if let Some(tuple) = result.scan_all().next() {
                        let _ = tuple?;
                    } else {
                        #[derive(Debug, Error, Diagnostic)]
                        #[error("The query is asserted to return some results, but returned none")]
                        #[diagnostic(code(eval::assert_some_failure))]
                        struct AssertSomeFailure(#[label] SourceSpan);
                        bail!(AssertSomeFailure(*span))
                    }
                }
            }
        }
        let json_headers = match input_program.get_entry_out_head() {
            Err(_) => JsonValue::Null,
            Ok(headers) => headers.into_iter().map(|v| json!(v.name)).collect(),
        };
        if !input_program.out_opts.sorters.is_empty() {
            let entry_head = input_program.get_entry_out_head()?;
            let sorted_result =
                tx.sort_and_collect(result, &input_program.out_opts.sorters, &entry_head)?;
            let sorted_iter = if let Some(offset) = input_program.out_opts.offset {
                Left(sorted_result.into_iter().skip(offset))
            } else {
                Right(sorted_result.into_iter())
            };
            let sorted_iter = if let Some(limit) = input_program.out_opts.limit {
                Left(sorted_iter.take(limit))
            } else {
                Right(sorted_iter)
            };
            let sorted_iter = sorted_iter.map(Ok);
            if let Some((meta, relation_op)) = &input_program.out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        sorted_iter,
                        *relation_op,
                        meta,
                        &input_program.get_entry_out_head_or_default()?,
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                Ok((json!({"headers": ["status"], "rows": [["OK"]]}), clean_ups))
            } else {
                let ret: Vec<Vec<JsonValue>> = sorted_iter
                    .map_ok(|tuple| -> Vec<JsonValue> {
                        tuple.0.into_iter().map(JsonValue::from).collect()
                    })
                    .try_collect()?;

                Ok((json!({ "rows": ret, "headers": json_headers }), clean_ups))
            }
        } else {
            let scan = if early_return {
                Right(Left(result.scan_early_returned()))
            } else if input_program.out_opts.limit.is_some()
                || input_program.out_opts.offset.is_some()
            {
                let limit = input_program.out_opts.limit.unwrap_or(usize::MAX);
                let offset = input_program.out_opts.offset.unwrap_or(0);
                Right(Right(result.scan_all().skip(offset).take(limit)))
            } else {
                Left(result.scan_all())
            };

            if let Some((meta, relation_op)) = &input_program.out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        scan,
                        *relation_op,
                        meta,
                        &input_program.get_entry_out_head_or_default()?,
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                Ok((json!({"headers": ["status"], "rows": [["OK"]]}), clean_ups))
            } else {
                let ret: Vec<Vec<JsonValue>> = scan
                    .map_ok(|tuple| -> Vec<JsonValue> {
                        tuple.0.into_iter().map(JsonValue::from).collect()
                    })
                    .try_collect()?;

                Ok((json!({ "rows": ret, "headers": json_headers }), clean_ups))
            }
        }
    }
    pub(crate) fn list_running(&self) -> Result<JsonValue> {
        let res = self
            .running_queries
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| json!([k, format!("{:?}", v.started_at)]))
            .collect_vec();
        Ok(json!({"rows": res, "headers": ["id", "started_at"]}))
    }
    fn list_relation(&'s self, name: &str) -> Result<JsonValue> {
        let mut tx = self.transact()?;
        let handle = tx.get_relation(name, false)?;
        let mut ret = vec![];
        let mut idx = 0;
        for col in &handle.metadata.keys {
            ret.push(json!([
                col.name,
                true,
                idx,
                col.typing.to_string(),
                col.default_gen.is_some()
            ]));
            idx += 1;
        }
        for col in &handle.metadata.non_keys {
            ret.push(json!([
                col.name,
                false,
                idx,
                col.typing.to_string(),
                col.default_gen.is_some()
            ]));
            idx += 1;
        }
        tx.commit_tx()?;
        Ok(json!({"rows": ret, "headers": ["column", "is_key", "index", "type", "has_default"]}))
    }
    fn list_relations(&'s self) -> Result<JsonValue> {
        let lower =
            Tuple(vec![DataValue::Str(SmartString::from(""))]).encode_as_key(RelationId::SYSTEM);
        let upper = Tuple(vec![DataValue::Str(SmartString::from(String::from(
            LARGEST_UTF_CHAR,
        )))])
        .encode_as_key(RelationId::SYSTEM);
        let tx = self.db.transact(false)?;
        let mut collected = vec![];
        for kv_res in tx.range_scan(&lower, &upper) {
            let (k_slice, v_slice) = kv_res?;
            if upper <= k_slice {
                break;
            }
            let meta = RelationHandle::decode(&v_slice)?;
            let n_keys = meta.metadata.keys.len();
            let n_dependents = meta.metadata.non_keys.len();
            let arity = n_keys + n_dependents;
            let name = meta.name;
            let access_level = meta.access_level.to_string();
            collected.push(json!([
                name,
                arity,
                access_level,
                n_keys,
                n_dependents,
                meta.put_triggers.len(),
                meta.rm_triggers.len(),
                meta.replace_triggers.len(),
            ]));
        }
        Ok(json!({"rows": collected, "headers":
                ["name", "arity", "access_level", "n_keys", "n_non_keys", "n_put_triggers", "n_rm_triggers", "n_replace_triggers"]}))
    }
}

#[derive(Clone, Default)]
pub(crate) struct Poison(pub(crate) Arc<AtomicBool>);

impl Poison {
    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Process is killed before completion")]
        #[diagnostic(code(eval::killed))]
        #[diagnostic(help("A process may be killed by timeout, or explicit command"))]
        struct ProcessKilled;

        if self.0.load(Ordering::Relaxed) {
            bail!(ProcessKilled)
        }
        Ok(())
    }
    pub(crate) fn set_timeout(&self, secs: f64) -> Result<()> {
        #[cfg(feature = "nothread")]
        bail!("Cannot set timeout when threading is disallowed");

        let pill = self.0.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
            pill.store(true, Ordering::Relaxed);
        });
        Ok(())
    }
}
