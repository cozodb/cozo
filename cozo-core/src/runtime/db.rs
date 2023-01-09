/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use either::{Either, Left, Right};
use itertools::Itertools;
#[allow(unused_imports)]
use miette::{bail, ensure, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
use serde_json::json;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::PredicateTypeError;
use crate::data::functions::{current_validity, op_to_bool};
use crate::data::json::JsonValue;
use crate::data::program::{InputProgram, QueryAssertion, RelationOp};
use crate::data::relation::ColumnDef;
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleT};
use crate::data::value::{DataValue, ValidityTs, LARGEST_UTF_CHAR};
use crate::fixed_rule::DEFAULT_FIXED_RULES;
use crate::parse::sys::SysOp;
use crate::parse::{
    parse_script, CozoScript, ImperativeCondition, ImperativeProgram, ImperativeStmt, SourceSpan,
};
use crate::query::compile::{CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::query::ra::{
    FilteredRA, InnerJoin, NegJoin, RelAlgebra, ReorderRA, StoredRA, StoredWithValidityRA,
    TempStoreRA, UnificationRA,
};
use crate::runtime::relation::{AccessLevel, InsufficientAccessLevel, RelationHandle, RelationId};
use crate::runtime::transact::SessionTx;
use crate::storage::temp::TempStorage;
use crate::storage::{Storage, StoreTx};
use crate::{decode_tuple_from_kv, FixedRule};

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

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum CallbackOp {
    Put,
    Rm,
}

pub type TxCallback = Box<dyn FnMut(CallbackOp, Tuple) + Send + Sync>;

/// The database object of Cozo.
#[derive(Clone)]
pub struct Db<S> {
    db: S,
    temp_db: TempStorage,
    relation_store_id: Arc<AtomicU64>,
    queries_count: Arc<AtomicU64>,
    running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
    pub(crate) algorithms: Arc<BTreeMap<String, Arc<Box<dyn FixedRule>>>>,
    callback_count: Arc<AtomicU64>,
    event_callbacks: Arc<RwLock<BTreeMap<String, BTreeMap<u64, TxCallback>>>>,
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

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug, Clone, Default)]
/// Rows in a relation, together with headers for the fields.
pub struct NamedRows {
    /// The headers
    pub headers: Vec<String>,
    /// The rows
    pub rows: Vec<Tuple>,
}

impl NamedRows {
    /// Convert to a JSON object
    pub fn into_json(self) -> JsonValue {
        let rows = self
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| JsonValue::from(val))
                    .collect::<JsonValue>()
            })
            .collect::<JsonValue>();
        json!({
            "headers": self.headers,
            "rows": rows
        })
    }
}

const STATUS_STR: &str = "status";
const OK_STR: &str = "OK";

enum ControlCode {
    Termination(NamedRows),
    Break(Option<SmartString<LazyCompact>>, SourceSpan),
    Continue(Option<SmartString<LazyCompact>>, SourceSpan),
}

impl<'s, S: Storage<'s>> Db<S> {
    /// Create a new database object with the given storage.
    /// You must call [`initialize`](Self::initialize) immediately after creation.
    /// Due to lifetime restrictions we are not able to call that for you automatically.
    pub fn new(storage: S) -> Result<Self> {
        let ret = Self {
            db: storage,
            temp_db: Default::default(),
            relation_store_id: Arc::new(Default::default()),
            queries_count: Arc::new(Default::default()),
            running_queries: Arc::new(Mutex::new(Default::default())),
            algorithms: DEFAULT_FIXED_RULES.clone(),
            callback_count: Arc::new(Default::default()),
            event_callbacks: Arc::new(Default::default()),
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
        params: BTreeMap<String, DataValue>,
    ) -> Result<NamedRows> {
        let cur_vld = current_validity();
        self.do_run_script(payload, &params, cur_vld)
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
            for data in tx.store_tx.range_scan(&start, &end) {
                let (k, v) = data?;
                let tuple = decode_tuple_from_kv(&k, &v);
                rows.push(tuple);
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

        let cur_vld = current_validity();

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
                        col.typing.coerce(v.clone(), cur_vld)
                    })
                    .try_collect()?;
                let k_store = handle.encode_key_for_store(&keys, Default::default())?;
                if is_delete {
                    tx.store_tx.del(&k_store)?;
                } else {
                    let vals: Vec<_> = val_indices
                        .iter()
                        .map(|(i, col)| -> Result<DataValue> {
                            let v = row
                                .get(*i)
                                .ok_or_else(|| miette!("row too short: {:?}", row))?;
                            col.typing.coerce(v.clone(), cur_vld)
                        })
                        .try_collect()?;
                    let v_store = handle.encode_val_only_for_store(&vals, Default::default())?;
                    tx.store_tx.put(&k_store, &v_store)?;
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
            let mut tx = self.transact()?;
            let iter = tx.store_tx.range_scan(&[], &[0xFF]);
            sqlite_db.db.batch_put(iter)?;
            tx.commit_tx()?;
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
            {
                let mut tx = self.transact()?;
                let store_id = tx.relation_store_id.load(Ordering::SeqCst);
                if store_id != 0 {
                    bail!(
                        "Cannot restore backup: data exists in the current database. \
                You can only restore into a new database (store id: {}).",
                        store_id
                    );
                }
                tx.commit_tx()?;
            }
            let iter = s_tx.store_tx.total_scan();
            self.db.batch_put(iter)?;
            s_tx.commit_tx()?;
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

                let data_it = src_tx.store_tx.range_scan(&src_lower, &src_upper).map(
                    |src_pair| -> Result<(Vec<u8>, Vec<u8>)> {
                        let (mut src_k, mut src_v) = src_pair?;
                        dst_handle.amend_key_prefix(&mut src_k);
                        dst_handle.amend_key_prefix(&mut src_v);
                        Ok((src_k, src_v))
                    },
                );
                for result in data_it {
                    let (key, val) = result?;
                    dst_tx.store_tx.put(&key, &val)?;
                }
            }

            src_tx.commit_tx()?;
            dst_tx.commit_tx()
        }
    }
    /// Register a custom fixed rule implementation
    pub fn register_fixed_rule(
        &mut self,
        name: String,
        rule_impl: Box<dyn FixedRule>,
    ) -> Result<()> {
        let inner = Arc::make_mut(&mut self.algorithms);
        match inner.entry(name) {
            Entry::Vacant(ent) => {
                ent.insert(Arc::new(rule_impl));
                Ok(())
            }
            Entry::Occupied(ent) => {
                bail!("A fixed rule with the name {} is already loaded", ent.key())
            }
        }
    }

    /// Register callbacks to run when changes to relations are committed.
    /// The returned ID can be used to unregister the callbacks.
    /// It is OK to register callbacks for relations that do not exist (yet).
    /// TODO: not yet implemented
    #[allow(dead_code)]
    pub(crate) fn register_callback(&self, relation: String, cb: TxCallback) -> u64 {
        let id = self.callback_count.fetch_add(1, Ordering::AcqRel);
        let mut guard = self.event_callbacks.write().unwrap();
        let entries = guard.entry(relation).or_default();
        entries.insert(id, cb);
        id
    }

    /// Unregister callbacks to run when changes to relations are committed.
    #[allow(dead_code)]
    pub(crate) fn unregister_callback(&self, relation: String, id: u64) -> bool {
        let mut guard = self.event_callbacks.write().unwrap();
        match guard.entry(relation) {
            Entry::Vacant(_) => false,
            Entry::Occupied(mut ent) => {
                let entries = ent.get_mut();
                entries.remove(&id).is_some()
            }
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
            store_tx: Box::new(self.db.transact(false)?),
            temp_store_tx: self.temp_db.transact(true)?,
            relation_store_id: self.relation_store_id.clone(),
            temp_store_id: Default::default(),
        };
        Ok(ret)
    }
    fn transact_write(&'s self) -> Result<SessionTx<'_>> {
        let ret = SessionTx {
            store_tx: Box::new(self.db.transact(true)?),
            temp_store_tx: self.temp_db.transact(true)?,
            relation_store_id: self.relation_store_id.clone(),
            temp_store_id: Default::default(),
        };
        Ok(ret)
    }
    fn execute_imperative_condition(
        &'s self,
        p: &ImperativeCondition,
        tx: &mut SessionTx<'_>,
        cleanups: &mut Vec<(Vec<u8>, Vec<u8>)>,
        cur_vld: ValidityTs,
        span: SourceSpan,
    ) -> Result<bool> {
        let res = match p {
            Left(rel) => {
                let relation = tx.get_relation(rel, false)?;
                relation.as_named_rows(tx)?
            }
            Right(p) => self.execute_single_program(p.clone(), tx, cleanups, cur_vld)?,
        };
        Ok(match res.rows.first() {
            None => false,
            Some(row) => {
                if row.is_empty() {
                    false
                } else {
                    op_to_bool(&row[row.len() - 1..])?
                        .get_bool()
                        .ok_or_else(|| PredicateTypeError(span, row.last().cloned().unwrap()))?
                }
            }
        })
    }
    fn execute_single_program(
        &'s self,
        p: InputProgram,
        tx: &mut SessionTx<'_>,
        cleanups: &mut Vec<(Vec<u8>, Vec<u8>)>,
        cur_vld: ValidityTs,
    ) -> Result<NamedRows> {
        #[allow(unused_variables)]
        let sleep_opt = p.out_opts.sleep;
        let (q_res, q_cleanups) = self.run_query(tx, p, cur_vld)?;
        cleanups.extend(q_cleanups);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(secs) = sleep_opt {
            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
        }
        Ok(q_res)
    }
    fn do_run_script(
        &'s self,
        payload: &str,
        param_pool: &BTreeMap<String, DataValue>,
        cur_vld: ValidityTs,
    ) -> Result<NamedRows> {
        match parse_script(payload, param_pool, &self.algorithms, cur_vld)? {
            CozoScript::Single(p) => {
                let is_write = p.needs_write_tx();
                let mut cleanups = vec![];
                let res;
                {
                    let mut tx = if is_write {
                        self.transact_write()?
                    } else {
                        self.transact()?
                    };

                    res = self.execute_single_program(p, &mut tx, &mut cleanups, cur_vld)?;

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
            CozoScript::Imperative(ps) => {
                let is_write = ps.iter().any(|p| p.needs_write_tx());
                let mut cleanups: Vec<(Vec<u8>, Vec<u8>)> = vec![];
                let ret;
                {
                    let mut tx = if is_write {
                        self.transact_write()?
                    } else {
                        self.transact()?
                    };
                    match self.execute_imperative_stmts(&ps, &mut tx, &mut cleanups, cur_vld)? {
                        Left(res) => ret = res,
                        Right(ctrl) => match ctrl {
                            ControlCode::Termination(res) => {
                                ret = res;
                            }
                            ControlCode::Break(_, span) | ControlCode::Continue(_, span) => {
                                #[derive(Debug, Error, Diagnostic)]
                                #[error("control flow has nowhere to go")]
                                #[diagnostic(code(eval::dangling_ctrl_flow))]
                                struct DanglingControlFlow(#[label] SourceSpan);

                                bail!(DanglingControlFlow(span))
                            }
                        },
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
                Ok(ret)
            }
            CozoScript::Sys(op) => self.run_sys_op(op),
        }
    }
    fn execute_imperative_stmts(
        &'s self,
        ps: &ImperativeProgram,
        tx: &mut SessionTx<'_>,
        cleanups: &mut Vec<(Vec<u8>, Vec<u8>)>,
        cur_vld: ValidityTs,
    ) -> Result<Either<NamedRows, ControlCode>> {
        let mut ret = NamedRows::default();
        for p in ps {
            match p {
                ImperativeStmt::Break { target, span, .. } => {
                    return Ok(Right(ControlCode::Break(target.clone(), *span)));
                }
                ImperativeStmt::Continue { target, span, .. } => {
                    return Ok(Right(ControlCode::Continue(target.clone(), *span)));
                }
                ImperativeStmt::ReturnNil { .. } => {
                    return Ok(Right(ControlCode::Termination(NamedRows::default())))
                }
                ImperativeStmt::ReturnProgram { prog, .. } => {
                    ret = self.execute_single_program(prog.clone(), tx, cleanups, cur_vld)?;
                    return Ok(Right(ControlCode::Termination(ret)));
                }
                ImperativeStmt::ReturnTemp { rel, .. } => {
                    let relation = tx.get_relation(rel, false)?;
                    return Ok(Right(ControlCode::Termination(relation.as_named_rows(tx)?)));
                }
                ImperativeStmt::TempDebug { temp, .. } => {
                    let relation = tx.get_relation(temp, false)?;
                    println!("{}: {:?}", temp, relation.as_named_rows(tx)?);
                    ret = NamedRows::default();
                }
                ImperativeStmt::Program { prog, .. } => {
                    ret = self.execute_single_program(prog.clone(), tx, cleanups, cur_vld)?;
                }
                ImperativeStmt::IgnoreErrorProgram { prog, .. } => {
                    match self.execute_single_program(prog.clone(), tx, cleanups, cur_vld) {
                        Ok(res) => ret = res,
                        Err(_) => {
                            ret = NamedRows {
                                headers: vec!["status".to_string()],
                                rows: vec![vec![DataValue::from("FAILED")]],
                            }
                        }
                    }
                }
                ImperativeStmt::If {
                    condition,
                    then_branch,
                    else_branch,
                    span,
                } => {
                    let cond_val =
                        self.execute_imperative_condition(condition, tx, cleanups, cur_vld, *span)?;
                    let to_execute = if cond_val { then_branch } else { else_branch };
                    match self.execute_imperative_stmts(to_execute, tx, cleanups, cur_vld)? {
                        Left(rows) => {
                            ret = rows;
                        }
                        Right(ctrl) => return Ok(Right(ctrl)),
                    }
                }
                ImperativeStmt::While {
                    label,
                    condition,
                    body,
                    span,
                } => {
                    ret = Default::default();
                    loop {
                        let cond_val = self.execute_imperative_condition(
                            condition, tx, cleanups, cur_vld, *span,
                        )?;
                        if cond_val {
                            match self.execute_imperative_stmts(body, tx, cleanups, cur_vld)? {
                                Left(_) => {}
                                Right(ctrl) => match ctrl {
                                    ControlCode::Termination(ret) => {
                                        return Ok(Right(ControlCode::Termination(ret)))
                                    }
                                    ControlCode::Break(break_label, span) => {
                                        if break_label.is_none() || break_label == *label {
                                            break;
                                        } else {
                                            return Ok(Right(ControlCode::Break(
                                                break_label,
                                                span,
                                            )));
                                        }
                                    }
                                    ControlCode::Continue(cont_label, span) => {
                                        if cont_label.is_none() || cont_label == *label {
                                            continue;
                                        } else {
                                            return Ok(Right(ControlCode::Continue(
                                                cont_label, span,
                                            )));
                                        }
                                    }
                                },
                            }
                        } else {
                            ret = NamedRows::default();
                            break;
                        }
                    }
                }
                ImperativeStmt::DoWhile {
                    label,
                    body,
                    condition,
                    span,
                } => {
                    ret = Default::default();
                    loop {
                        match self.execute_imperative_stmts(body, tx, cleanups, cur_vld)? {
                            Left(_) => {}
                            Right(ctrl) => match ctrl {
                                ControlCode::Termination(ret) => {
                                    return Ok(Right(ControlCode::Termination(ret)))
                                }
                                ControlCode::Break(break_label, span) => {
                                    if break_label.is_none() || break_label == *label {
                                        break;
                                    } else {
                                        return Ok(Right(ControlCode::Break(break_label, span)));
                                    }
                                }
                                ControlCode::Continue(cont_label, span) => {
                                    if cont_label.is_none() || cont_label == *label {
                                        continue;
                                    } else {
                                        return Ok(Right(ControlCode::Continue(cont_label, span)));
                                    }
                                }
                            },
                        }
                    }
                    let cond_val =
                        self.execute_imperative_condition(condition, tx, cleanups, cur_vld, *span)?;
                    if !cond_val {
                        ret = NamedRows::default();
                        break;
                    }
                }
                ImperativeStmt::TempSwap { left, right, .. } => {
                    tx.rename_temp_relation(
                        Symbol::new(left.clone(), Default::default()),
                        Symbol::new(SmartString::from("*temp*"), Default::default()),
                    )?;
                    tx.rename_temp_relation(
                        Symbol::new(right.clone(), Default::default()),
                        Symbol::new(left.clone(), Default::default()),
                    )?;
                    tx.rename_temp_relation(
                        Symbol::new(SmartString::from("*temp*"), Default::default()),
                        Symbol::new(right.clone(), Default::default()),
                    )?;
                    ret = NamedRows::default();
                    break;
                }
                ImperativeStmt::TempRemove { temp, .. } => {
                    tx.destroy_temp_relation(temp)?;
                    ret = NamedRows::default();
                }
            }
        }
        return Ok(Left(ret));
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
                                    RelAlgebra::StoredWithValidity(StoredWithValidityRA {
                                        storage,
                                        filters,
                                        ..
                                    }) => (
                                        "load_stored_with_validity",
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
                                    RelAlgebra::Filter(FilteredRA {
                                        parent,
                                        filters: pred,
                                        ..
                                    }) => {
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
                    CompiledRuleSet::Fixed(_) => ret.push(json!({
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
                    .map(|i| DataValue::from(m.get(i).unwrap_or(&JsonValue::Null)))
                    .collect_vec()
            })
            .collect_vec();

        Ok(NamedRows { headers, rows })
    }
    fn run_sys_op(&'s self, op: SysOp) -> Result<NamedRows> {
        match op {
            SysOp::Explain(prog) => {
                let mut tx = self.transact()?;
                let (normalized_program, _) = prog.into_normalized_program(&tx)?;
                let (stratified_program, _) = normalized_program.into_stratified_program()?;
                let program = stratified_program.magic_sets_rewrite(&tx)?;
                let compiled = tx.stratified_magic_compile(program)?;
                tx.commit_tx()?;
                self.explain_compiled(&compiled)
            }
            SysOp::Compact => {
                self.compact_relation()?;
                Ok(NamedRows {
                    headers: vec![STATUS_STR.to_string()],
                    rows: vec![vec![DataValue::from(OK_STR)]],
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
                    rows: vec![vec![DataValue::from(OK_STR)]],
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
                    rows: vec![vec![DataValue::from(OK_STR)]],
                })
            }
            SysOp::ListRunning => self.list_running(),
            SysOp::KillRunning(id) => {
                let queries = self.running_queries.lock().unwrap();
                Ok(match queries.get(&id) {
                    None => NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![DataValue::from("NOT_FOUND")]],
                    },
                    Some(handle) => {
                        handle.poison.0.store(true, Ordering::Relaxed);
                        NamedRows {
                            headers: vec![STATUS_STR.to_string()],
                            rows: vec![vec![DataValue::from("KILLING")]],
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
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|val| DataValue::from(val))
                            .collect_vec()
                    })
                    .collect_vec();
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
                    rows: vec![vec![DataValue::from(OK_STR)]],
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
                    rows: vec![vec![DataValue::from(OK_STR)]],
                })
            }
        }
    }
    /// This is the entry to query evaluation
    pub(crate) fn run_query(
        &self,
        tx: &mut SessionTx<'_>,
        input_program: InputProgram,
        cur_vld: ValidityTs,
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

                existing.ensure_compatible(meta, *op == RelationOp::Rm)?;
            }
        };

        // query compilation
        let entry_head_or_default = input_program.get_entry_out_head_or_default()?;
        let (normalized_program, out_opts) = input_program.into_normalized_program(tx)?;
        let (stratified_program, store_lifetimes) = normalized_program.into_stratified_program()?;
        let program = stratified_program.magic_sets_rewrite(tx)?;
        let compiled = tx.stratified_magic_compile(program)?;

        // poison is used to terminate queries early
        let poison = Poison::default();
        if let Some(secs) = out_opts.timeout {
            poison.set_timeout(secs)?;
        }
        // give the query an ID and store it so that it can be queried and cancelled
        let id = self.queries_count.fetch_add(1, Ordering::AcqRel);

        // time the query
        #[cfg(not(target_arch = "wasm32"))]
        let now = SystemTime::now();
        #[cfg(not(target_arch = "wasm32"))]
        let since_the_epoch = now
            .duration_since(UNIX_EPOCH)
            .into_diagnostic()?
            .as_secs_f64();

        #[cfg(target_arch = "wasm32")]
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

        let total_num_to_take = if out_opts.sorters.is_empty() {
            out_opts.num_to_take()
        } else {
            None
        };

        let num_to_skip = if out_opts.sorters.is_empty() {
            out_opts.offset
        } else {
            None
        };

        // the real evaluation
        let (result_store, early_return) = tx.stratified_magic_evaluate(
            &compiled,
            store_lifetimes,
            total_num_to_take,
            num_to_skip,
            poison,
        )?;

        // deal with assertions
        if let Some(assertion) = &out_opts.assertion {
            match assertion {
                QueryAssertion::AssertNone(span) => {
                    if let Some(tuple) = result_store.all_iter().next() {
                        #[derive(Debug, Error, Diagnostic)]
                        #[error(
                            "The query is asserted to return no result, but a tuple {0:?} is found"
                        )]
                        #[diagnostic(code(eval::assert_none_failure))]
                        struct AssertNoneFailure(Tuple, #[label] SourceSpan);
                        bail!(AssertNoneFailure(tuple.into_tuple(), *span))
                    }
                }
                QueryAssertion::AssertSome(span) => {
                    if result_store.all_iter().next().is_none() {
                        #[derive(Debug, Error, Diagnostic)]
                        #[error("The query is asserted to return some results, but returned none")]
                        #[diagnostic(code(eval::assert_some_failure))]
                        struct AssertSomeFailure(#[label] SourceSpan);
                        bail!(AssertSomeFailure(*span))
                    }
                }
            }
        }

        if !out_opts.sorters.is_empty() {
            // sort outputs if required
            let sorted_result =
                tx.sort_and_collect(result_store, &out_opts.sorters, &entry_head_or_default)?;
            let sorted_iter = if let Some(offset) = out_opts.offset {
                Left(sorted_result.into_iter().skip(offset))
            } else {
                Right(sorted_result.into_iter())
            };
            let sorted_iter = if let Some(limit) = out_opts.limit {
                Left(sorted_iter.take(limit))
            } else {
                Right(sorted_iter)
            };
            if let Some((meta, relation_op)) = &out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        sorted_iter,
                        *relation_op,
                        meta,
                        &entry_head_or_default,
                        cur_vld,
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                Ok((
                    NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![DataValue::from(OK_STR)]],
                    },
                    clean_ups,
                ))
            } else {
                // not sorting outputs
                let rows: Vec<Tuple> = sorted_iter.collect_vec();
                Ok((
                    NamedRows {
                        headers: entry_head_or_default
                            .iter()
                            .map(|s| s.to_string())
                            .collect_vec(),
                        rows,
                    },
                    clean_ups,
                ))
            }
        } else {
            let scan = if early_return {
                Right(Left(
                    result_store.early_returned_iter().map(|t| t.into_tuple()),
                ))
            } else if out_opts.limit.is_some() || out_opts.offset.is_some() {
                let limit = out_opts.limit.unwrap_or(usize::MAX);
                let offset = out_opts.offset.unwrap_or(0);
                Right(Right(
                    result_store
                        .all_iter()
                        .skip(offset)
                        .take(limit)
                        .map(|t| t.into_tuple()),
                ))
            } else {
                Left(result_store.all_iter().map(|t| t.into_tuple()))
            };

            if let Some((meta, relation_op)) = &out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        scan,
                        *relation_op,
                        meta,
                        &entry_head_or_default,
                        cur_vld,
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                Ok((
                    NamedRows {
                        headers: vec![STATUS_STR.to_string()],
                        rows: vec![vec![DataValue::from(OK_STR)]],
                    },
                    clean_ups,
                ))
            } else {
                let rows: Vec<Tuple> = scan.collect_vec();

                Ok((
                    NamedRows {
                        headers: entry_head_or_default
                            .iter()
                            .map(|s| s.to_string())
                            .collect_vec(),
                        rows,
                    },
                    clean_ups,
                ))
            }
        }
    }
    pub(crate) fn list_running(&self) -> Result<NamedRows> {
        let rows = self
            .running_queries
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                vec![
                    DataValue::from(*k as i64),
                    DataValue::from(format!("{:?}", v.started_at)),
                ]
            })
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
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| DataValue::from(val))
                    .collect_vec()
            })
            .collect_vec();
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
        let lower = vec![DataValue::from("")].encode_as_key(RelationId::SYSTEM);
        let upper =
            vec![DataValue::from(String::from(LARGEST_UTF_CHAR))].encode_as_key(RelationId::SYSTEM);
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
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| DataValue::from(val))
                    .collect_vec()
            })
            .collect_vec();
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
pub struct Poison(pub(crate) Arc<AtomicBool>);

impl Poison {
    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Running query is killed before completion")]
        #[diagnostic(code(eval::killed))]
        #[diagnostic(help("A query may be killed by timeout, or explicit command"))]
        struct ProcessKilled;

        if self.0.load(Ordering::Relaxed) {
            bail!(ProcessKilled)
        }
        Ok(())
    }
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn set_timeout(&self, _secs: f64) -> Result<()> {
        bail!("Cannot set timeout when threading is disallowed");
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn set_timeout(&self, secs: f64) -> Result<()> {
        let pill = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
            pill.0.store(true, Ordering::Relaxed);
        });
        Ok(())
    }
}
