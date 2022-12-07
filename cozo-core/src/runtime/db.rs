/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use either::{Left, Right};
use itertools::Itertools;
use miette::{bail, ensure, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
use serde_json::json;
use smartstring::SmartString;
use thiserror::Error;

use crate::data::json::JsonValue;
use crate::data::program::{InputProgram, QueryAssertion, RelationOp};
use crate::data::relation::ColumnDef;
use crate::data::tuple::{Tuple, TupleT};
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::decode_tuple_from_kv;
use crate::parse::sys::SysOp;
use crate::parse::{parse_script, CozoScript, SourceSpan};
use crate::query::compile::{CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::query::ra::{
    FilteredRA, InnerJoin, NegJoin, RelAlgebra, ReorderRA, StoredRA, TempStoreRA, UnificationRA,
};
use crate::runtime::relation::{AccessLevel, InsufficientAccessLevel, RelationHandle, RelationId};
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

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug)]
/// Rows in a relation, together with headers for the fields.
pub struct NamedRows {
    /// The headers
    pub headers: Vec<String>,
    /// The rows
    pub rows: Vec<Vec<JsonValue>>,
}

impl NamedRows {
    /// Convert to a JSON object
    pub fn into_json(self) -> JsonValue {
        json!({
            "headers": self.headers,
            "rows": self.rows
        })
    }
}

const STATUS_STR: &str = "status";
const OK_STR: &str = "OK";

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

    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    pub fn run_script(
        &'s self,
        payload: &str,
        params: BTreeMap<String, JsonValue>,
    ) -> Result<NamedRows> {
        let params = params
            .into_iter()
            .map(|(k, v)| (k, DataValue::from(v)))
            .collect();
        self.do_run_script(payload, &params)
    }
    /// Export relations to JSON data.
    ///
    /// `relations` contains names of the stored relations to export.
    ///
    /// If `as_objects` is `true`, then the output contains objects (maps) for each row,
    /// otherwise the output contains arrays for each row, with headers attached separately.
    pub fn export_relations<'a>(
        &'s self,
        relations: impl Iterator<Item = &'a str>,
    ) -> Result<BTreeMap<String, NamedRows>> {
        let tx = self.transact()?;
        let mut ret: BTreeMap<String, NamedRows> = BTreeMap::new();
        for rel in relations {
            let handle = tx.get_relation(rel, false)?;

            if handle.access_level < AccessLevel::ReadOnly {
                bail!(InsufficientAccessLevel(
                    handle.name.to_string(),
                    "data export".to_string(),
                    handle.access_level
                ));
            }

            let mut cols = handle
                .metadata
                .keys
                .iter()
                .map(|col| col.name.clone())
                .collect_vec();
            cols.extend(
                handle
                    .metadata
                    .non_keys
                    .iter()
                    .map(|col| col.name.clone())
                    .collect_vec(),
            );

            let start = Tuple::default().encode_as_key(handle.id);
            let end = Tuple::default().encode_as_key(handle.id.next());

            let mut rows = vec![];
            for data in tx.tx.range_scan(&start, &end) {
                let (k, v) = data?;
                let tuple = decode_tuple_from_kv(&k, &v);
                let row = tuple.into_iter().map(JsonValue::from).collect_vec();
                rows.push(row);
            }
            let headers = cols.iter().map(|col| col.to_string()).collect_vec();
            ret.insert(rel.to_string(), NamedRows { headers, rows });
        }
        Ok(ret)
    }
    /// Import relations. The argument `data` accepts data in the shape of
    /// what was returned by [Self::export_relations].
    /// The target stored relations must already exist in the database.
    ///
    /// Note that triggers are _not_ run for the relations, if any exists.
    /// If you need to activate triggers, use queries with parameters.
    pub fn import_relations(&'s self, data: BTreeMap<String, NamedRows>) -> Result<()> {
        #[derive(Debug, Diagnostic, Error)]
        #[error("cannot import data for relation '{0}': {1}")]
        #[diagnostic(code(import::bad_data))]
        struct BadDataForRelation(String, JsonValue);

        let mut tx = self.transact_write()?;

        for (relation_op, in_data) in data {
            let is_delete;
            let relation: &str = match relation_op.strip_prefix('-') {
                None => {
                    is_delete = false;
                    &relation_op
                }
                Some(s) => {
                    is_delete = true;
                    s
                }
            };
            let handle = tx.get_relation(relation, false)?;

            if handle.access_level < AccessLevel::Protected {
                bail!(InsufficientAccessLevel(
                    handle.name.to_string(),
                    "data import".to_string(),
                    handle.access_level
                ));
            }

            let header2idx: BTreeMap<_, _> = in_data
                .headers
                .iter()
                .enumerate()
                .map(|(i, k)| -> Result<(&str, usize)> { Ok((k as &str, i)) })
                .try_collect()?;

            let key_indices: Vec<_> = handle
                .metadata
                .keys
                .iter()
                .map(|col| -> Result<(usize, &ColumnDef)> {
                    let idx = header2idx.get(&col.name as &str).ok_or_else(|| {
                        miette!(
                            "required header {} not found for relation {}",
                            col.name,
                            relation
                        )
                    })?;
                    Ok((*idx, col))
                })
                .try_collect()?;

            let val_indices: Vec<_> = if is_delete {
                vec![]
            } else {
                handle
                    .metadata
                    .non_keys
                    .iter()
                    .map(|col| -> Result<(usize, &ColumnDef)> {
                        let idx = header2idx.get(&col.name as &str).ok_or_else(|| {
                            miette!(
                                "required header {} not found for relation {}",
                                col.name,
                                relation
                            )
                        })?;
                        Ok((*idx, col))
                    })
                    .try_collect()?
            };

            for row in in_data.rows {
                let keys: Vec<_> = key_indices
                    .iter()
                    .map(|(i, col)| -> Result<DataValue> {
                        let v = row
                            .get(*i)
                            .ok_or_else(|| miette!("row too short: {:?}", row))?;
                        col.typing.coerce(DataValue::from(v))
                    })
                    .try_collect()?;
                let k_store = handle.encode_key_for_store(&keys, Default::default())?;
                if is_delete {
                    tx.tx.del(&k_store)?;
                } else {
                    let vals: Vec<_> = val_indices
                        .iter()
                        .map(|(i, col)| -> Result<DataValue> {
                            let v = row
                                .get(*i)
                                .ok_or_else(|| miette!("row too short: {:?}", row))?;
                            col.typing.coerce(DataValue::from(v))
                        })
                        .try_collect()?;
                    let v_store = handle.encode_val_only_for_store(&vals, Default::default())?;
                    tx.tx.put(&k_store, &v_store)?;
                }
            }
        }
        tx.commit_tx()?;
        Ok(())
    }
    /// Backup the running database into an Sqlite file
    #[allow(unused_variables)]
    pub fn backup_db(&'s self, out_file: String) -> Result<()> {
        #[cfg(feature = "storage-sqlite")]
        {
            let sqlite_db = crate::new_cozo_sqlite(out_file)?;
            if sqlite_db.relation_store_id.load(Ordering::SeqCst) != 0 {
                bail!("Cannot create backup: data exists in the target database.");
            }
            let mut s_tx = sqlite_db.transact_write()?;
            let mut tx = self.transact()?;
            let iter = tx.tx.range_scan(&[], &[0xFF]);
            s_tx.tx.batch_put(iter)?;
            tx.commit_tx()?;
            s_tx.commit_tx()?;
            Ok(())
        }
        #[cfg(not(feature = "storage-sqlite"))]
        bail!("backup requires the 'storage-sqlite' feature to be enabled")
    }
    /// Restore from an Sqlite backup
    #[allow(unused_variables)]
    pub fn restore_backup(&'s self, in_file: &str) -> Result<()> {
        #[cfg(feature = "storage-sqlite")]
        {
            let sqlite_db = crate::new_cozo_sqlite(in_file.to_string())?;
            let mut s_tx = sqlite_db.transact()?;
            let mut tx = self.transact_write()?;
            let store_id = tx.relation_store_id.load(Ordering::SeqCst);
            if store_id != 0 {
                bail!(
                    "Cannot restore backup: data exists in the current database. \
                You can only restore into a new database (store id: {}).",
                    store_id
                );
            }
            let iter = s_tx.tx.range_scan(&[], &[1]);
            tx.tx.batch_put(iter)?;
            s_tx.commit_tx()?;
            tx.commit_tx()?;
            Ok(())
        }
        #[cfg(not(feature = "storage-sqlite"))]
        bail!("backup requires the 'storage-sqlite' feature to be enabled")
    }
    /// Import data from relations in a backup file.
    /// The target stored relations must already exist in the database.
    ///
    /// Note that triggers are _not_ run for the relations, if any exists.
    /// If you need to activate triggers, use queries with parameters.
    #[allow(unused_variables)]
    pub fn import_from_backup(&'s self, in_file: &str, relations: &[String]) -> Result<()> {
        #[cfg(not(feature = "storage-sqlite"))]
        bail!("backup requires the 'storage-sqlite' feature to be enabled");

        #[cfg(feature = "storage-sqlite")]
        {
            let source_db = crate::new_cozo_sqlite(in_file.to_string())?;
            let mut src_tx = source_db.transact()?;
            let mut dst_tx = self.transact_write()?;

            for relation in relations {
                let src_handle = src_tx.get_relation(relation, false)?;
                let dst_handle = dst_tx.get_relation(relation, false)?;

                if dst_handle.access_level < AccessLevel::Protected {
                    bail!(InsufficientAccessLevel(
                        dst_handle.name.to_string(),
                        "data import".to_string(),
                        dst_handle.access_level
                    ));
                }

                let src_lower = Tuple::default().encode_as_key(src_handle.id);
                let src_upper = Tuple::default().encode_as_key(src_handle.id.next());

                let data_it = src_tx.tx.range_scan(&src_lower, &src_upper).map(
                    |src_pair| -> Result<(Vec<u8>, Vec<u8>)> {
                        let (mut src_k, mut src_v) = src_pair?;
                        dst_handle.amend_key_prefix(&mut src_k);
                        dst_handle.amend_key_prefix(&mut src_v);
                        Ok((src_k, src_v))
                    },
                );
                dst_tx.tx.batch_put(Box::new(data_it))?;
            }

            src_tx.commit_tx()?;
            dst_tx.commit_tx()
        }
    }

    fn compact_relation(&'s self) -> Result<()> {
        let l = Tuple::default().encode_as_key(RelationId(0));
        let u = vec![DataValue::Bot].encode_as_key(RelationId(u64::MAX));
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
    fn do_run_script(
        &'s self,
        payload: &str,
        param_pool: &BTreeMap<String, DataValue>,
    ) -> Result<NamedRows> {
        match parse_script(payload, param_pool)? {
            CozoScript::Multi(ps) => {
                let is_write = ps.iter().any(|p| p.out_opts.store_relation.is_some());
                let mut cleanups = vec![];
                let mut res = NamedRows {
                    headers: vec![],
                    rows: vec![],
                };
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
                        #[cfg(not(feature = "wasm"))]
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
    fn explain_compiled(&self, strata: &[CompiledProgram]) -> Result<NamedRows> {
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

        let headers = vec![
            STRATUM.to_string(),
            RULE_IDX.to_string(),
            RULE_NAME.to_string(),
            ATOM_IDX.to_string(),
            OP.to_string(),
            REF_NAME.to_string(),
            JOINS_ON.to_string(),
            FILTERS.to_string(),
            OUT_BINDINGS.to_string(),
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
                                    RelAlgebra::TempStore(TempStoreRA {
                                        storage_key,
                                        filters,
                                        ..
                                    }) => (
                                        "load_mem",
                                        json!(storage_key.to_string()),
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

        let rows = ret
            .into_iter()
            .map(|m| {
                headers
                    .iter()
                    .map(|i| m.get(i).unwrap_or(&JsonValue::Null).clone())
                    .collect_vec()
            })
            .collect_vec();

        Ok(NamedRows { headers, rows })
    }
    fn run_sys_op(&'s self, op: SysOp) -> Result<NamedRows> {
        match op {
            SysOp::Explain(prog) => {
                let mut tx = self.transact()?;
                let (stratified_program, _) = prog.to_normalized_program(&tx)?.stratify()?;
                let program = stratified_program.magic_sets_rewrite(&tx)?;
                let compiled = tx.stratified_magic_compile(&program)?;
                tx.commit_tx()?;
                self.explain_compiled(&compiled)
            }
            SysOp::Compact => {
                self.compact_relation()?;
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![json!(OK_STR)]],
                })
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
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![json!(OK_STR)]],
                })
            }
            SysOp::ListRelation(rs) => self.list_relation(&rs),
            SysOp::RenameRelation(rename_pairs) => {
                let mut tx = self.transact_write()?;
                for (old, new) in rename_pairs {
                    tx.rename_relation(old, new)?;
                }
                tx.commit_tx()?;
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![json!(OK_STR)]],
                })
            }
            SysOp::ListRunning => self.list_running(),
            SysOp::KillRunning(id) => {
                let queries = self.running_queries.lock().unwrap();
                Ok(match queries.get(&id) {
                    None => NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![json!("NOT_FOUND")]],
                    },
                    Some(handle) => {
                        handle.poison.0.store(true, Ordering::Relaxed);
                        NamedRows {
                            headers: vec![STATUS_STR.to_string()],
                            rows: vec![vec![json!("KILLING")]],
                        }
                    }
                })
            }
            SysOp::ShowTrigger(name) => {
                let mut tx = self.transact()?;
                let rel = tx.get_relation(&name, false)?;
                let mut rows: Vec<Vec<JsonValue>> = vec![];
                for (i, trigger) in rel.put_triggers.iter().enumerate() {
                    rows.push(vec![json!("put"), json!(i), json!(trigger)])
                }
                for (i, trigger) in rel.rm_triggers.iter().enumerate() {
                    rows.push(vec![json!("rm"), json!(i), json!(trigger)])
                }
                for (i, trigger) in rel.replace_triggers.iter().enumerate() {
                    rows.push(vec![json!("replace"), json!(i), json!(trigger)])
                }
                tx.commit_tx()?;
                Ok(NamedRows {
                    headers: vec!["type".to_string(), "idx".to_string(), "trigger".to_string()],
                    rows,
                })
            }
            SysOp::SetTriggers(name, puts, rms, replaces) => {
                let mut tx = self.transact_write()?;
                tx.set_relation_triggers(name, puts, rms, replaces)?;
                tx.commit_tx()?;
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![json!(OK_STR)]],
                })
            }
            SysOp::SetAccessLevel(names, level) => {
                let mut tx = self.transact_write()?;
                for name in names {
                    tx.set_access_level(name, level)?;
                }
                tx.commit_tx()?;
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![json!(OK_STR)]],
                })
            }
        }
    }
    /// This is the entry to query evaluation
    pub(crate) fn run_query(
        &self,
        tx: &mut SessionTx<'_>,
        input_program: InputProgram,
    ) -> Result<(NamedRows, Vec<(Vec<u8>, Vec<u8>)>)> {
        // cleanups contain stored relations that should be deleted at the end of query
        let mut clean_ups = vec![];

        // Some checks in case the query specifies mutation
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

                let existing = tx.get_relation(&meta.name, false)?;

                ensure!(
                    tx.relation_exists(&meta.name)?,
                    StoreRelationNotFoundError(meta.name.to_string())
                );

                existing.ensure_compatible(meta)?;
            }
        };

        // query compilation
        let (stratified_program, store_lifetimes) =
            input_program.to_normalized_program(tx)?.stratify()?;
        let program = stratified_program.magic_sets_rewrite(tx)?;
        let compiled = tx.stratified_magic_compile(&program)?;

        // poison is used to terminate queries early
        let poison = Poison::default();
        if let Some(secs) = input_program.out_opts.timeout {
            poison.set_timeout(secs)?;
        }
        // give the query an ID and store it so that it can be queried and cancelled
        let id = self.queries_count.fetch_add(1, Ordering::AcqRel);

        // time the query
        #[cfg(not(feature = "wasm"))]
        let now = SystemTime::now();
        #[cfg(not(feature = "wasm"))]
        let since_the_epoch = now
            .duration_since(UNIX_EPOCH)
            .into_diagnostic()?
            .as_secs_f64();

        #[cfg(feature = "wasm")]
        let since_the_epoch = js_sys::Date::now();

        let handle = RunningQueryHandle {
            started_at: since_the_epoch,
            poison: poison.clone(),
        };
        self.running_queries.lock().unwrap().insert(id, handle);

        // RAII cleanups of running query handle
        let _guard = RunningQueryCleanup {
            id,
            running_queries: self.running_queries.clone(),
        };

        let total_num_to_take = if input_program.out_opts.sorters.is_empty() {
            input_program.out_opts.num_to_take()
        } else {
            None
        };

        let num_to_skip = if input_program.out_opts.sorters.is_empty() {
            input_program.out_opts.offset
        } else {
            None
        };

        // the real evaluation
        let (result, early_return) = tx.stratified_magic_evaluate(
            &compiled,
            store_lifetimes,
            total_num_to_take,
            num_to_skip,
            poison,
        )?;

        // deal with assertions
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

        if !input_program.out_opts.sorters.is_empty() {
            // sort outputs if required
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
                Ok((
                    NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![json!(OK_STR)]],
                    },
                    clean_ups,
                ))
            } else {
                // not sorting outputs
                let rows: Vec<Vec<JsonValue>> = sorted_iter
                    .map_ok(|tuple| -> Vec<JsonValue> {
                        tuple.into_iter().map(JsonValue::from).collect()
                    })
                    .try_collect()?;
                let headers: Vec<String> = match input_program.get_entry_out_head() {
                    Ok(headers) => headers.into_iter().map(|v| v.name.to_string()).collect(),
                    Err(_) => match rows.get(0) {
                        None => vec![],
                        Some(row) => (0..row.len()).map(|i| format!("_{}", i)).collect_vec(),
                    },
                };
                Ok((NamedRows { headers, rows }, clean_ups))
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
                Ok((
                    NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![json!(OK_STR)]],
                    },
                    clean_ups,
                ))
            } else {
                let rows: Vec<Vec<JsonValue>> = scan
                    .map_ok(|tuple| -> Vec<JsonValue> {
                        tuple.into_iter().map(JsonValue::from).collect()
                    })
                    .try_collect()?;

                let headers: Vec<String> = match input_program.get_entry_out_head() {
                    Ok(headers) => headers.into_iter().map(|v| v.name.to_string()).collect(),
                    Err(_) => match rows.get(0) {
                        None => vec![],
                        Some(row) => (0..row.len()).map(|i| format!("_{}", i)).collect_vec(),
                    },
                };

                Ok((NamedRows { headers, rows }, clean_ups))
            }
        }
    }
    pub(crate) fn list_running(&self) -> Result<NamedRows> {
        let rows = self
            .running_queries
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| vec![json!(k), json!(format!("{:?}", v.started_at))])
            .collect_vec();
        Ok(NamedRows {
            headers: vec!["id".to_string(), "started_at".to_string()],
            rows,
        })
    }
    fn list_relation(&'s self, name: &str) -> Result<NamedRows> {
        let mut tx = self.transact()?;
        let handle = tx.get_relation(name, false)?;
        let mut rows = vec![];
        let mut idx = 0;
        for col in &handle.metadata.keys {
            rows.push(vec![
                json!(col.name),
                json!(true),
                json!(idx),
                json!(col.typing.to_string()),
                json!(col.default_gen.is_some()),
            ]);
            idx += 1;
        }
        for col in &handle.metadata.non_keys {
            rows.push(vec![
                json!(col.name),
                json!(false),
                json!(idx),
                json!(col.typing.to_string()),
                json!(col.default_gen.is_some()),
            ]);
            idx += 1;
        }
        tx.commit_tx()?;
        Ok(NamedRows {
            headers: vec![
                "column".to_string(),
                "is_key".to_string(),
                "index".to_string(),
                "type".to_string(),
                "has_default".to_string(),
            ],
            rows,
        })
    }
    fn list_relations(&'s self) -> Result<NamedRows> {
        let lower = vec![DataValue::Str(SmartString::from(""))].encode_as_key(RelationId::SYSTEM);
        let upper = vec![DataValue::Str(SmartString::from(String::from(
            LARGEST_UTF_CHAR,
        )))]
        .encode_as_key(RelationId::SYSTEM);
        let tx = self.db.transact(false)?;
        let mut rows: Vec<Vec<JsonValue>> = vec![];
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
            rows.push(vec![
                json!(name),
                json!(arity),
                json!(access_level),
                json!(n_keys),
                json!(n_dependents),
                json!(meta.put_triggers.len()),
                json!(meta.rm_triggers.len()),
                json!(meta.replace_triggers.len()),
            ]);
        }
        Ok(NamedRows {
            headers: vec![
                "name".to_string(),
                "arity".to_string(),
                "access_level".to_string(),
                "n_keys".to_string(),
                "n_non_keys".to_string(),
                "n_put_triggers".to_string(),
                "n_rm_triggers".to_string(),
                "n_replace_triggers".to_string(),
            ],
            rows,
        })
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
    #[cfg(feature = "nothread")]
    pub(crate) fn set_timeout(&self, _secs: f64) -> Result<()> {
        bail!("Cannot set timeout when threading is disallowed");
    }
    #[cfg(not(feature = "nothread"))]
    pub(crate) fn set_timeout(&self, secs: f64) -> Result<()> {
        let pill = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
            pill.0.store(true, Ordering::Relaxed);
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serde_json::json;

    use crate::new_cozo_mem;

    #[test]
    fn test_limit_offset() {
        let db = new_cozo_mem().unwrap();
        let res = db
            .run_script("?[a] := a in [5,3,1,2,4] :limit 2", Default::default())
            .unwrap()
            .rows
            .into_iter()
            .flatten()
            .collect_vec();
        assert_eq!(json!(res), json!([3, 5]));
        let res = db
            .run_script(
                "?[a] := a in [5,3,1,2,4] :limit 2 :offset 1",
                Default::default(),
            )
            .unwrap()
            .rows
            .into_iter()
            .flatten()
            .collect_vec();
        assert_eq!(json!(res), json!([1, 3]));
        let res = db
            .run_script(
                "?[a] := a in [5,3,1,2,4] :limit 2 :offset 4",
                Default::default(),
            )
            .unwrap()
            .rows
            .into_iter()
            .flatten()
            .collect_vec();
        assert_eq!(json!(res), json!([4]));
        let res = db
            .run_script(
                "?[a] := a in [5,3,1,2,4] :limit 2 :offset 5",
                Default::default(),
            )
            .unwrap()
            .rows
            .into_iter()
            .flatten()
            .collect_vec();
        assert_eq!(json!(res), json!([]));
    }
    #[test]
    fn test_normal_aggr_empty() {
        let db = new_cozo_mem().unwrap();
        let res = db
            .run_script("?[count(a)] := a in []", Default::default())
            .unwrap()
            .rows;
        assert_eq!(res, vec![vec![json!(0)]]);
    }
    #[test]
    fn test_meet_aggr_empty() {
        let db = new_cozo_mem().unwrap();
        let res = db
            .run_script("?[min(a)] := a in []", Default::default())
            .unwrap()
            .rows;
        assert_eq!(res, vec![vec![json!(null)]]);

        let res = db
            .run_script("?[min(a), count(a)] := a in []", Default::default())
            .unwrap()
            .rows;
        assert_eq!(res, vec![vec![json!(null), json!(0)]]);
    }
}
