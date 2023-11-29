/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::path::Path;
#[allow(unused_imports)]
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(unused_imports)]
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use crossbeam::sync::ShardedLock;
use either::{Left, Right};
use itertools::Itertools;
use miette::Report;
#[allow(unused_imports)]
use miette::{bail, ensure, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
use serde_json::json;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::functions::current_validity;
use crate::data::json::JsonValue;
use crate::data::program::{InputProgram, QueryAssertion, RelationOp, ReturnMutation};
use crate::data::relation::ColumnDef;
use crate::data::tuple::{Tuple, TupleT};
use crate::data::value::{DataValue, ValidityTs, LARGEST_UTF_CHAR};
use crate::fixed_rule::DEFAULT_FIXED_RULES;
use crate::fts::TokenizerCache;
use crate::parse::sys::SysOp;
use crate::parse::{parse_expressions, parse_script, CozoScript, SourceSpan};
use crate::query::compile::{CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::query::ra::{
    FilteredRA, FtsSearchRA, HnswSearchRA, InnerJoin, LshSearchRA, NegJoin, RelAlgebra, ReorderRA,
    StoredRA, StoredWithValidityRA, TempStoreRA, UnificationRA,
};
#[allow(unused_imports)]
use crate::runtime::callback::{
    CallbackCollector, CallbackDeclaration, CallbackOp, EventCallbackRegistry,
};
use crate::runtime::relation::{
    extend_tuple_from_v, AccessLevel, InsufficientAccessLevel, RelationHandle, RelationId,
};
use crate::runtime::transact::SessionTx;
use crate::storage::temp::TempStorage;
use crate::storage::Storage;
use crate::{decode_tuple_from_kv, FixedRule, Symbol};

pub(crate) struct RunningQueryHandle {
    pub(crate) started_at: f64,
    pub(crate) poison: Poison,
}

pub(crate) struct RunningQueryCleanup {
    pub(crate) id: u64,
    pub(crate) running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
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

/// Whether a script is mutable or immutable.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ScriptMutability {
    /// The script is mutable.
    Mutable,
    /// The script is immutable.
    Immutable,
}

/// The database object of Cozo.
#[derive(Clone)]
pub struct Db<S> {
    pub(crate) db: S,
    temp_db: TempStorage,
    relation_store_id: Arc<AtomicU64>,
    pub(crate) queries_count: Arc<AtomicU64>,
    pub(crate) running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
    pub(crate) fixed_rules: Arc<ShardedLock<BTreeMap<String, Arc<Box<dyn FixedRule>>>>>,
    pub(crate) tokenizers: Arc<TokenizerCache>,
    #[cfg(not(target_arch = "wasm32"))]
    callback_count: Arc<AtomicU32>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) event_callbacks: Arc<ShardedLock<EventCallbackRegistry>>,
    relation_locks: Arc<ShardedLock<BTreeMap<SmartString<LazyCompact>, Arc<ShardedLock<()>>>>>,
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

#[derive(Debug, Error, Diagnostic)]
#[error("Cannot import data into relation {0} as it is an index")]
#[diagnostic(code(tx::import_into_index))]
pub(crate) struct ImportIntoIndex(pub(crate) String);

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug, Clone, Default)]
/// Rows in a relation, together with headers for the fields.
pub struct NamedRows {
    /// The headers
    pub headers: Vec<String>,
    /// The rows
    pub rows: Vec<Tuple>,
    /// Contains the next named rows, if exists
    pub next: Option<Box<NamedRows>>,
}

impl IntoIterator for NamedRows {
    type Item = Tuple;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl NamedRows {
    /// create a named rows with the given headers and rows
    pub fn new(headers: Vec<String>, rows: Vec<Tuple>) -> Self {
        Self {
            headers,
            rows,
            next: None,
        }
    }

    /// If there are more named rows after the current one
    pub fn has_more(&self) -> bool {
        self.next.is_some()
    }

    /// convert a chain of named rows to individual named rows
    pub fn flatten(self) -> Vec<Self> {
        let mut collected = vec![];
        let mut current = self;
        loop {
            let nxt = current.next.take();
            collected.push(current);
            if let Some(n) = nxt {
                current = *n;
            } else {
                break;
            }
        }
        collected
    }

    /// Convert to a JSON object
    pub fn into_json(self) -> JsonValue {
        let nxt = match self.next {
            None => json!(null),
            Some(more) => more.into_json(),
        };
        let rows = self
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(JsonValue::from).collect::<JsonValue>())
            .collect::<JsonValue>();
        json!({
            "headers": self.headers,
            "rows": rows,
            "next": nxt,
        })
    }
    /// Make named rows from JSON
    pub fn from_json(value: &JsonValue) -> Result<Self> {
        let headers = value
            .get("headers")
            .ok_or_else(|| miette!("NamedRows requires 'headers' field"))?;
        let headers = headers
            .as_array()
            .ok_or_else(|| miette!("'headers' field must be an array"))?;
        let headers = headers
            .iter()
            .map(|h| -> Result<String> {
                let h = h
                    .as_str()
                    .ok_or_else(|| miette!("'headers' field must be an array of strings"))?;
                Ok(h.to_string())
            })
            .try_collect()?;
        let rows = value
            .get("rows")
            .ok_or_else(|| miette!("NamedRows requires 'rows' field"))?;
        let rows = rows
            .as_array()
            .ok_or_else(|| miette!("'rows' field must be an array"))?;
        let rows = rows
            .iter()
            .map(|row| -> Result<Vec<DataValue>> {
                let row = row
                    .as_array()
                    .ok_or_else(|| miette!("'rows' field must be an array of arrays"))?;
                Ok(row.iter().map(DataValue::from).collect_vec())
            })
            .try_collect()?;
        Ok(Self {
            headers,
            rows,
            next: None,
        })
    }
}

const STATUS_STR: &str = "status";
const OK_STR: &str = "OK";

/// Commands to be sent to a multi-transaction
#[derive(Eq, PartialEq, Debug)]
pub enum TransactionPayload {
    /// Commit the current transaction
    Commit,
    /// Abort the current transaction
    Abort,
    /// Run a query inside the transaction
    Query((String, BTreeMap<String, DataValue>)),
}

impl<'s, S: Storage<'s>> Db<S> {
    /// Create a new database object with the given storage.
    /// You must call [`initialize`](Self::initialize) immediately after creation.
    /// Due to lifetime restrictions we are not able to call that for you automatically.
    pub fn new(storage: S) -> Result<Self> {
        let ret = Self {
            db: storage,
            temp_db: Default::default(),
            relation_store_id: Default::default(),
            queries_count: Default::default(),
            running_queries: Default::default(),
            fixed_rules: Arc::new(ShardedLock::new(DEFAULT_FIXED_RULES.clone())),
            tokenizers: Arc::new(Default::default()),
            #[cfg(not(target_arch = "wasm32"))]
            callback_count: Default::default(),
            // callback_receiver: Arc::new(receiver),
            #[cfg(not(target_arch = "wasm32"))]
            event_callbacks: Default::default(),
            relation_locks: Default::default(),
        };
        Ok(ret)
    }

    /// Must be called after creation of the database to initialize the runtime state.
    pub fn initialize(&'s self) -> Result<()> {
        self.load_last_ids()?;
        Ok(())
    }

    /// Run a multi-transaction. A command should be sent to `payloads`, and the result should be
    /// retrieved from `results`. A transaction ends when it receives a `Commit` or `Abort`,
    /// or when a query is not successful. After a transaction ends, sending / receiving from
    /// the channels will fail.
    ///
    /// Write transactions _may_ block other reads, but we guarantee that this does not happen
    /// for the RocksDB backend.
    pub fn run_multi_transaction(
        &'s self,
        is_write: bool,
        payloads: Receiver<TransactionPayload>,
        results: Sender<Result<NamedRows>>,
    ) {
        let tx = if is_write {
            self.transact_write()
        } else {
            self.transact()
        };
        let mut cleanups: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let mut tx = match tx {
            Ok(tx) => tx,
            Err(err) => {
                let _ = results.send(Err(err));
                return;
            }
        };

        let ts = current_validity();
        let callback_targets = self.current_callback_targets();
        let mut callback_collector = BTreeMap::new();
        let mut write_locks = BTreeMap::new();

        for payload in payloads {
            match payload {
                TransactionPayload::Commit => {
                    for (lower, upper) in cleanups {
                        if let Err(err) = tx.store_tx.del_range_from_persisted(&lower, &upper) {
                            eprintln!("{err:?}")
                        }
                    }

                    let _ = results.send(tx.commit_tx().map(|_| NamedRows::default()));
                    #[cfg(not(target_arch = "wasm32"))]
                    if !callback_collector.is_empty() {
                        self.send_callbacks(callback_collector)
                    }

                    break;
                }
                TransactionPayload::Abort => {
                    let _ = results.send(Ok(NamedRows::default()));
                    break;
                }
                TransactionPayload::Query((script, params)) => {
                    let p =
                        match parse_script(&script, &params, &self.fixed_rules.read().unwrap(), ts)
                        {
                            Ok(p) => p,
                            Err(err) => {
                                if results.send(Err(err)).is_err() {
                                    break;
                                } else {
                                    continue;
                                }
                            }
                        };

                    let p = match p.get_single_program() {
                        Ok(p) => p,
                        Err(err) => {
                            if results.send(Err(err)).is_err() {
                                break;
                            } else {
                                continue;
                            }
                        }
                    };
                    if let Some(write_lock_name) = p.needs_write_lock() {
                        match write_locks.entry(write_lock_name) {
                            Entry::Vacant(e) => {
                                let lock = self
                                    .obtain_relation_locks(iter::once(e.key()))
                                    .pop()
                                    .unwrap();
                                e.insert(lock);
                            }
                            Entry::Occupied(_) => {}
                        }
                    }

                    let res = self.execute_single_program(
                        p,
                        &mut tx,
                        &mut cleanups,
                        ts,
                        &callback_targets,
                        &mut callback_collector,
                    );
                    if results.send(res).is_err() {
                        break;
                    }
                }
            }
        }
    }

    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    pub fn run_script(
        &'s self,
        payload: &str,
        params: BTreeMap<String, DataValue>,
        mutability: ScriptMutability,
    ) -> Result<NamedRows> {
        let cur_vld = current_validity();
        self.do_run_script(
            payload,
            &params,
            cur_vld,
            mutability == ScriptMutability::Immutable,
        )
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    pub fn run_script_read_only(
        &'s self,
        payload: &str,
        params: BTreeMap<String, DataValue>,
    ) -> Result<NamedRows> {
        let cur_vld = current_validity();
        self.do_run_script(payload, &params, cur_vld, true)
    }

    /// Export relations to JSON data.
    ///
    /// `relations` contains names of the stored relations to export.
    pub fn export_relations<I, T>(&'s self, relations: I) -> Result<BTreeMap<String, NamedRows>>
    where
        T: AsRef<str>,
        I: Iterator<Item = T>,
    {
        let tx = self.transact()?;
        let mut ret: BTreeMap<String, NamedRows> = BTreeMap::new();
        for rel in relations {
            let handle = tx.get_relation(rel.as_ref(), false)?;
            let size_hint = handle.metadata.keys.len() + handle.metadata.non_keys.len();

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
                let tuple = decode_tuple_from_kv(&k, &v, Some(size_hint));
                rows.push(tuple);
            }
            let headers = cols.iter().map(|col| col.to_string()).collect_vec();
            ret.insert(rel.as_ref().to_string(), NamedRows::new(headers, rows));
        }
        Ok(ret)
    }
    /// Import relations. The argument `data` accepts data in the shape of
    /// what was returned by [Self::export_relations].
    /// The target stored relations must already exist in the database.
    /// Any associated indices will be updated.
    ///
    /// Note that triggers and callbacks are _not_ run for the relations, if any exists.
    /// If you need to activate triggers or callbacks, use queries with parameters.
    pub fn import_relations(&'s self, data: BTreeMap<String, NamedRows>) -> Result<()> {
        #[derive(Debug, Diagnostic, Error)]
        #[error("cannot import data for relation '{0}': {1}")]
        #[diagnostic(code(import::bad_data))]
        struct BadDataForRelation(String, JsonValue);

        let rel_names = data.keys().map(SmartString::from).collect_vec();
        let locks = self.obtain_relation_locks(rel_names.iter());
        let _guards = locks.iter().map(|l| l.read().unwrap()).collect_vec();

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
            if relation.contains(':') {
                bail!(ImportIntoIndex(relation.to_string()))
            }
            let handle = tx.get_relation(relation, false)?;
            let has_indices = !handle.indices.is_empty();

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
                if has_indices {
                    if let Some(existing) = tx.store_tx.get(&k_store, false)? {
                        let mut old = keys.clone();
                        extend_tuple_from_v(&mut old, &existing);
                        if is_delete || old != row {
                            for (idx_rel, extractor) in handle.indices.values() {
                                let idx_tup =
                                    extractor.iter().map(|i| old[*i].clone()).collect_vec();
                                let encoded =
                                    idx_rel.encode_key_for_store(&idx_tup, Default::default())?;
                                tx.store_tx.del(&encoded)?;
                            }
                        }
                    }
                }
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
                    if has_indices {
                        let mut kv = keys;
                        kv.extend(vals);
                        for (idx_rel, extractor) in handle.indices.values() {
                            let idx_tup = extractor.iter().map(|i| kv[*i].clone()).collect_vec();
                            let encoded =
                                idx_rel.encode_key_for_store(&idx_tup, Default::default())?;
                            tx.store_tx.put(&encoded, &[])?;
                        }
                    }
                }
            }
        }
        tx.commit_tx()?;
        Ok(())
    }
    /// Backup the running database into an Sqlite file
    #[allow(unused_variables)]
    pub fn backup_db(&'s self, out_file: impl AsRef<Path>) -> Result<()> {
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
    pub fn restore_backup(&'s self, in_file: impl AsRef<Path>) -> Result<()> {
        #[cfg(feature = "storage-sqlite")]
        {
            let sqlite_db = crate::new_cozo_sqlite(in_file)?;
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
    /// The target stored relations must already exist in the database, and it must not
    /// have any associated indices. If you want to import into relations with indices,
    /// use [Db::import_relations].
    ///
    /// Note that triggers and callbacks are _not_ run for the relations, if any exists.
    /// If you need to activate triggers or callbacks, use queries with parameters.
    #[allow(unused_variables)]
    pub fn import_from_backup(
        &'s self,
        in_file: impl AsRef<Path>,
        relations: &[String],
    ) -> Result<()> {
        #[cfg(not(feature = "storage-sqlite"))]
        bail!("backup requires the 'storage-sqlite' feature to be enabled");

        #[cfg(feature = "storage-sqlite")]
        {
            let rel_names = relations.iter().map(SmartString::from).collect_vec();
            let locks = self.obtain_relation_locks(rel_names.iter());
            let _guards = locks.iter().map(|l| l.read().unwrap()).collect_vec();

            let source_db = crate::new_cozo_sqlite(in_file)?;
            let mut src_tx = source_db.transact()?;
            let mut dst_tx = self.transact_write()?;

            for relation in relations {
                if relation.contains(':') {
                    bail!(ImportIntoIndex(relation.to_string()))
                }
                let src_handle = src_tx.get_relation(relation, false)?;
                let dst_handle = dst_tx.get_relation(relation, false)?;

                if !dst_handle.indices.is_empty() {
                    #[derive(Debug, Error, Diagnostic)]
                    #[error("Cannot import data into relation {0} from backup as the relation has indices")]
                    #[diagnostic(code(tx::bare_import_with_indices))]
                    #[diagnostic(help("Use `import_relations()` instead"))]
                    pub(crate) struct RestoreIntoRelWithIndices(pub(crate) String);

                    bail!(RestoreIntoRelWithIndices(dst_handle.name.to_string()))
                }

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
    /// Register a custom fixed rule implementation.
    pub fn register_fixed_rule<R>(&self, name: String, rule_impl: R) -> Result<()>
    where
        R: FixedRule + 'static,
    {
        match self.fixed_rules.write().unwrap().entry(name) {
            Entry::Vacant(ent) => {
                ent.insert(Arc::new(Box::new(rule_impl)));
                Ok(())
            }
            Entry::Occupied(ent) => {
                bail!(
                    "A fixed rule with the name {} is already registered",
                    ent.key()
                )
            }
        }
    }

    /// Unregister a custom fixed rule implementation.
    pub fn unregister_fixed_rule(&self, name: &str) -> Result<bool> {
        if DEFAULT_FIXED_RULES.contains_key(name) {
            bail!("Cannot unregister builtin fixed rule {}", name);
        }
        Ok(self.fixed_rules.write().unwrap().remove(name).is_some())
    }

    /// Register callback channel to receive changes when the requested relation are successfully committed.
    /// The returned ID can be used to unregister the callback channel.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn register_callback(
        &self,
        relation: &str,
        capacity: Option<usize>,
    ) -> (u32, Receiver<(CallbackOp, NamedRows, NamedRows)>) {
        let (sender, receiver) = if let Some(c) = capacity {
            bounded(c)
        } else {
            unbounded()
        };
        let cb = CallbackDeclaration {
            dependent: SmartString::from(relation),
            sender,
        };

        let mut guard = self.event_callbacks.write().unwrap();
        let new_id = self.callback_count.fetch_add(1, Ordering::SeqCst);
        guard
            .1
            .entry(SmartString::from(relation))
            .or_default()
            .insert(new_id);

        guard.0.insert(new_id, cb);
        (new_id, receiver)
    }

    /// Unregister callbacks/channels to run when changes to relations are committed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn unregister_callback(&self, id: u32) -> bool {
        let mut guard = self.event_callbacks.write().unwrap();
        let ret = guard.0.remove(&id);
        if let Some(cb) = &ret {
            guard.1.get_mut(&cb.dependent).unwrap().remove(&id);

            if guard.1.get(&cb.dependent).unwrap().is_empty() {
                guard.1.remove(&cb.dependent);
            }
        }
        ret.is_some()
    }

    pub(crate) fn obtain_relation_locks<'a, T: Iterator<Item = &'a SmartString<LazyCompact>>>(
        &'s self,
        rels: T,
    ) -> Vec<Arc<ShardedLock<()>>> {
        let mut collected = vec![];
        let mut pending = vec![];
        {
            let locks = self.relation_locks.read().unwrap();
            for rel in rels {
                match locks.get(rel) {
                    None => {
                        pending.push(rel);
                    }
                    Some(lock) => collected.push(lock.clone()),
                }
            }
        }
        if !pending.is_empty() {
            let mut locks = self.relation_locks.write().unwrap();
            for rel in pending {
                let lock = locks.entry(rel.clone()).or_default().clone();
                collected.push(lock);
            }
        }
        collected
    }

    fn compact_relation(&'s self) -> Result<()> {
        let l = Tuple::default().encode_as_key(RelationId(0));
        let u = vec![DataValue::Bot].encode_as_key(RelationId(u64::MAX));
        self.db.range_compact(&l, &u)?;
        Ok(())
    }

    fn load_last_ids(&'s self) -> Result<()> {
        let mut tx = self.transact_write()?;
        self.relation_store_id
            .store(tx.init_storage()?.0, Ordering::Release);
        tx.commit_tx()?;
        Ok(())
    }
    pub(crate) fn transact(&'s self) -> Result<SessionTx<'_>> {
        let ret = SessionTx {
            store_tx: Box::new(self.db.transact(false)?),
            temp_store_tx: self.temp_db.transact(true)?,
            relation_store_id: self.relation_store_id.clone(),
            temp_store_id: Default::default(),
            tokenizers: self.tokenizers.clone(),
        };
        Ok(ret)
    }
    pub(crate) fn transact_write(&'s self) -> Result<SessionTx<'_>> {
        let ret = SessionTx {
            store_tx: Box::new(self.db.transact(true)?),
            temp_store_tx: self.temp_db.transact(true)?,
            relation_store_id: self.relation_store_id.clone(),
            temp_store_id: Default::default(),
            tokenizers: self.tokenizers.clone(),
        };
        Ok(ret)
    }

    pub(crate) fn execute_single_program(
        &'s self,
        p: InputProgram,
        tx: &mut SessionTx<'_>,
        cleanups: &mut Vec<(Vec<u8>, Vec<u8>)>,
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
    ) -> Result<NamedRows> {
        #[allow(unused_variables)]
        let sleep_opt = p.out_opts.sleep;
        let (q_res, q_cleanups) =
            self.run_query(tx, p, cur_vld, callback_targets, callback_collector, true)?;
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
        read_only: bool,
    ) -> Result<NamedRows> {
        match parse_script(
            payload,
            param_pool,
            &self.fixed_rules.read().unwrap(),
            cur_vld,
        )? {
            CozoScript::Single(p) => self.execute_single(cur_vld, p, read_only),
            CozoScript::Imperative(ps) => self.execute_imperative(cur_vld, &ps, read_only),
            CozoScript::Sys(op) => self.run_sys_op(op, read_only),
        }
    }

    fn execute_single(
        &'s self,
        cur_vld: ValidityTs,
        p: InputProgram,
        read_only: bool,
    ) -> Result<NamedRows, Report> {
        let mut callback_collector = BTreeMap::new();
        let write_lock_names = p.needs_write_lock();
        let is_write = write_lock_names.is_some();
        if read_only && is_write {
            bail!("write lock required for read-only query");
        }
        let write_lock = self.obtain_relation_locks(write_lock_names.iter());
        let _write_lock_guards = if is_write {
            Some(write_lock[0].read().unwrap())
        } else {
            None
        };
        let callback_targets = if is_write {
            self.current_callback_targets()
        } else {
            Default::default()
        };
        let mut cleanups = vec![];
        let res;
        {
            let mut tx = if is_write {
                self.transact_write()?
            } else {
                self.transact()?
            };

            res = self.execute_single_program(
                p,
                &mut tx,
                &mut cleanups,
                cur_vld,
                &callback_targets,
                &mut callback_collector,
            )?;

            for (lower, upper) in cleanups {
                tx.store_tx.del_range_from_persisted(&lower, &upper)?;
            }

            tx.commit_tx()?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if !callback_collector.is_empty() {
            self.send_callbacks(callback_collector)
        }

        Ok(res)
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
                                    RelAlgebra::HnswSearch(HnswSearchRA {
                                        hnsw_search, ..
                                    }) => (
                                        "hnsw_index",
                                        json!(format!(":{}", hnsw_search.query.name)),
                                        json!(hnsw_search.query.name),
                                        json!(hnsw_search
                                            .filter
                                            .iter()
                                            .map(|f| f.to_string())
                                            .collect_vec()),
                                    ),
                                    RelAlgebra::FtsSearch(FtsSearchRA { fts_search, .. }) => (
                                        "fts_index",
                                        json!(format!(":{}", fts_search.query.name)),
                                        json!(fts_search.query.name),
                                        json!(fts_search
                                            .filter
                                            .iter()
                                            .map(|f| f.to_string())
                                            .collect_vec()),
                                    ),
                                    RelAlgebra::LshSearch(LshSearchRA { lsh_search, .. }) => (
                                        "lsh_index",
                                        json!(format!(":{}", lsh_search.query.name)),
                                        json!(lsh_search.query.name),
                                        json!(lsh_search
                                            .filter
                                            .iter()
                                            .map(|f| f.to_string())
                                            .collect_vec()),
                                    ),
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

        Ok(NamedRows::new(headers, rows))
    }
    pub(crate) fn run_sys_op_with_tx(
        &'s self,
        tx: &mut SessionTx<'_>,
        op: &SysOp,
        read_only: bool,
        skip_locking: bool,
    ) -> Result<NamedRows> {
        match op {
            SysOp::Explain(prog) => {
                let (normalized_program, _) = prog.clone().into_normalized_program(tx)?;
                let (stratified_program, _) = normalized_program.into_stratified_program()?;
                let program = stratified_program.magic_sets_rewrite(tx)?;
                let compiled = tx.stratified_magic_compile(program)?;
                self.explain_compiled(&compiled)
            }
            SysOp::Compact => {
                if read_only {
                    bail!("Cannot compact in read-only mode");
                }
                self.compact_relation()?;
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::ListRelations => self.list_relations(tx),
            SysOp::ListFixedRules => {
                let rules = self.fixed_rules.read().unwrap();
                Ok(NamedRows::new(
                    vec!["rule".to_string()],
                    rules
                        .keys()
                        .map(|k| vec![DataValue::from(k as &str)])
                        .collect_vec(),
                ))
            }
            SysOp::RemoveRelation(rel_names) => {
                if read_only {
                    bail!("Cannot remove relations in read-only mode");
                }
                let rel_name_strs = rel_names.iter().map(|n| &n.name);
                let locks = if skip_locking {
                    vec![]
                } else {
                    self.obtain_relation_locks(rel_name_strs)
                };
                let _guards = locks.iter().map(|l| l.read().unwrap()).collect_vec();
                let mut bounds = vec![];
                for rs in rel_names {
                    let bound = tx.destroy_relation(rs)?;
                    if !rs.is_temp_store_name() {
                        bounds.extend(bound);
                    }
                }
                for (lower, upper) in bounds {
                    tx.store_tx.del_range_from_persisted(&lower, &upper)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::DescribeRelation(rel_name, description) => {
                tx.describe_relation(rel_name, description)?;
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::CreateIndex(rel_name, idx_name, cols) => {
                if read_only {
                    bail!("Cannot create index in read-only mode");
                }
                if skip_locking {
                    tx.create_index(rel_name, idx_name, cols)?;
                } else {
                    let lock = self
                        .obtain_relation_locks(iter::once(&rel_name.name))
                        .pop()
                        .unwrap();
                    let _guard = lock.write().unwrap();
                    tx.create_index(rel_name, idx_name, cols)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::CreateVectorIndex(config) => {
                if read_only {
                    bail!("Cannot create vector index in read-only mode");
                }
                if skip_locking {
                    tx.create_hnsw_index(config)?;
                } else {
                    let lock = self
                        .obtain_relation_locks(iter::once(&config.base_relation))
                        .pop()
                        .unwrap();
                    let _guard = lock.write().unwrap();
                    tx.create_hnsw_index(config)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::CreateFtsIndex(config) => {
                if read_only {
                    bail!("Cannot create fts index in read-only mode");
                }
                if skip_locking {
                    tx.create_fts_index(config)?;
                } else {
                    let lock = self
                        .obtain_relation_locks(iter::once(&config.base_relation))
                        .pop()
                        .unwrap();
                    let _guard = lock.write().unwrap();
                    tx.create_fts_index(config)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::CreateMinHashLshIndex(config) => {
                if read_only {
                    bail!("Cannot create minhash lsh index in read-only mode");
                }
                if skip_locking {
                    tx.create_minhash_lsh_index(config)?;
                } else {
                    let lock = self
                        .obtain_relation_locks(iter::once(&config.base_relation))
                        .pop()
                        .unwrap();
                    let _guard = lock.write().unwrap();
                    tx.create_minhash_lsh_index(config)?;
                }

                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::RemoveIndex(rel_name, idx_name) => {
                if read_only {
                    bail!("Cannot remove index in read-only mode");
                }
                let bounds = if skip_locking {
                    tx.remove_index(rel_name, idx_name)?
                } else {
                    let lock = self
                        .obtain_relation_locks(iter::once(&rel_name.name))
                        .pop()
                        .unwrap();
                    let _guard = lock.read().unwrap();
                    tx.remove_index(rel_name, idx_name)?
                };

                for (lower, upper) in bounds {
                    tx.store_tx.del_range_from_persisted(&lower, &upper)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::ListColumns(rs) => self.list_columns(tx, rs),
            SysOp::ListIndices(rs) => self.list_indices(tx, rs),
            SysOp::RenameRelation(rename_pairs) => {
                if read_only {
                    bail!("Cannot rename relations in read-only mode");
                }
                let rel_names = rename_pairs.iter().flat_map(|(f, t)| [&f.name, &t.name]);
                let locks = if skip_locking {
                    vec![]
                } else {
                    self.obtain_relation_locks(rel_names)
                };
                let _guards = locks.iter().map(|l| l.read().unwrap()).collect_vec();
                for (old, new) in rename_pairs {
                    tx.rename_relation(old, new)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::ListRunning => self.list_running(),
            SysOp::KillRunning(id) => {
                let queries = self.running_queries.lock().unwrap();
                Ok(match queries.get(id) {
                    None => NamedRows::new(
                        vec![STATUS_STR.to_string()],
                        vec![vec![DataValue::from("NOT_FOUND")]],
                    ),
                    Some(handle) => {
                        handle.poison.0.store(true, Ordering::Relaxed);
                        NamedRows::new(
                            vec![STATUS_STR.to_string()],
                            vec![vec![DataValue::from("KILLING")]],
                        )
                    }
                })
            }
            SysOp::ShowTrigger(name) => {
                let rel = tx.get_relation(name, false)?;
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
                    .map(|row| row.into_iter().map(DataValue::from).collect_vec())
                    .collect_vec();
                Ok(NamedRows::new(
                    vec!["type".to_string(), "idx".to_string(), "trigger".to_string()],
                    rows,
                ))
            }
            SysOp::SetTriggers(name, puts, rms, replaces) => {
                if read_only {
                    bail!("Cannot set triggers in read-only mode");
                }
                tx.set_relation_triggers(name, puts, rms, replaces)?;
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
            SysOp::SetAccessLevel(names, level) => {
                if read_only {
                    bail!("Cannot set access level in read-only mode");
                }
                for name in names {
                    tx.set_access_level(name, *level)?;
                }
                Ok(NamedRows::new(
                    vec![STATUS_STR.to_string()],
                    vec![vec![DataValue::from(OK_STR)]],
                ))
            }
        }
    }
    fn run_sys_op(&'s self, op: SysOp, read_only: bool) -> Result<NamedRows> {
        let mut tx = if read_only {
            self.transact()?
        } else {
            self.transact_write()?
        };
        let res = self.run_sys_op_with_tx(&mut tx, &op, read_only, false)?;
        tx.commit_tx()?;
        Ok(res)
    }
    /// This is the entry to query evaluation
    pub(crate) fn run_query(
        &self,
        tx: &mut SessionTx<'_>,
        input_program: InputProgram,
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        top_level: bool,
    ) -> Result<(NamedRows, Vec<(Vec<u8>, Vec<u8>)>)> {
        // cleanups contain stored relations that should be deleted at the end of query
        let mut clean_ups = vec![];

        // Some checks in case the query specifies mutation
        if let Some((meta, op, _)) = &input_program.out_opts.store_relation {
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

                existing.ensure_compatible(
                    meta,
                    *op == RelationOp::Rm || *op == RelationOp::Delete || *op == RelationOp::Update,
                )?;
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
        let since_the_epoch = seconds_since_the_epoch()?;

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
            if let Some((meta, relation_op, returning)) = &out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        sorted_iter,
                        *relation_op,
                        meta,
                        &entry_head_or_default,
                        cur_vld,
                        callback_targets,
                        callback_collector,
                        top_level,
                        if *returning == ReturnMutation::Returning {
                            &meta.name.name
                        } else {
                            ""
                        },
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                let returned_rows =
                    tx.get_returning_rows(callback_collector, &meta.name, returning)?;
                Ok((returned_rows, clean_ups))
            } else {
                // not sorting outputs
                let rows: Vec<Tuple> = sorted_iter.collect_vec();
                Ok((
                    NamedRows::new(
                        entry_head_or_default
                            .iter()
                            .map(|s| s.to_string())
                            .collect_vec(),
                        rows,
                    ),
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

            if let Some((meta, relation_op, returning)) = &out_opts.store_relation {
                let to_clear = tx
                    .execute_relation(
                        self,
                        scan,
                        *relation_op,
                        meta,
                        &entry_head_or_default,
                        cur_vld,
                        callback_targets,
                        callback_collector,
                        top_level,
                        if *returning == ReturnMutation::Returning {
                            &meta.name.name
                        } else {
                            ""
                        },
                    )
                    .wrap_err_with(|| format!("when executing against relation '{}'", meta.name))?;
                clean_ups.extend(to_clear);
                let returned_rows =
                    tx.get_returning_rows(callback_collector, &meta.name, returning)?;

                Ok((returned_rows, clean_ups))
            } else {
                let rows: Vec<Tuple> = scan.collect_vec();

                Ok((
                    NamedRows::new(
                        entry_head_or_default
                            .iter()
                            .map(|s| s.to_string())
                            .collect_vec(),
                        rows,
                    ),
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
        Ok(NamedRows::new(
            vec!["id".to_string(), "started_at".to_string()],
            rows,
        ))
    }
    fn list_indices(&'s self, tx: &SessionTx<'_>, name: &str) -> Result<NamedRows> {
        let handle = tx.get_relation(name, false)?;
        let mut rows = vec![];
        for (name, (rel, cols)) in &handle.indices {
            rows.push(vec![
                json!(name),
                json!("normal"),
                json!([rel.name]),
                json!({ "indices": cols }),
            ]);
        }
        for (name, (rel, manifest)) in &handle.hnsw_indices {
            rows.push(vec![
                json!(name),
                json!("hnsw"),
                json!([rel.name]),
                json!({
                    "vec_dim": manifest.vec_dim,
                    "dtype": manifest.dtype,
                    "vec_fields": manifest.vec_fields,
                    "distance": manifest.distance,
                    "ef_construction": manifest.ef_construction,
                    "m_neighbours": manifest.m_neighbours,
                    "m_max": manifest.m_max,
                    "m_max0": manifest.m_max0,
                    "level_multiplier": manifest.level_multiplier,
                    "extend_candidates": manifest.extend_candidates,
                    "keep_pruned_connections": manifest.keep_pruned_connections,
                }),
            ]);
        }
        for (name, (rel, manifest)) in &handle.fts_indices {
            rows.push(vec![
                json!(name),
                json!("fts"),
                json!([rel.name]),
                json!({
                    "extractor": manifest.extractor,
                    "tokenizer": manifest.tokenizer,
                    "tokenizer_filters": manifest.filters,
                }),
            ]);
        }
        for (name, (rel, inv_rel, manifest)) in &handle.lsh_indices {
            rows.push(vec![
                json!(name),
                json!("lsh"),
                json!([rel.name, inv_rel.name]),
                json!({
                    "extractor": manifest.extractor,
                    "tokenizer": manifest.tokenizer,
                    "tokenizer_filters": manifest.filters,
                    "n_gram": manifest.n_gram,
                    "num_perm": manifest.num_perm,
                    "n_bands": manifest.n_bands,
                    "n_rows_in_band": manifest.n_rows_in_band,
                    "threshold": manifest.threshold,
                }),
            ]);
        }
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(DataValue::from).collect_vec())
            .collect_vec();
        Ok(NamedRows::new(
            vec![
                "name".to_string(),
                "type".to_string(),
                "relations".to_string(),
                "config".to_string(),
            ],
            rows,
        ))
    }
    fn list_columns(&'s self, tx: &SessionTx<'_>, name: &str) -> Result<NamedRows> {
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
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(DataValue::from).collect_vec())
            .collect_vec();
        Ok(NamedRows::new(
            vec![
                "column".to_string(),
                "is_key".to_string(),
                "index".to_string(),
                "type".to_string(),
                "has_default".to_string(),
            ],
            rows,
        ))
    }
    fn list_relations(&'s self, tx: &SessionTx<'_>) -> Result<NamedRows> {
        let lower = vec![DataValue::from("")].encode_as_key(RelationId::SYSTEM);
        let upper =
            vec![DataValue::from(String::from(LARGEST_UTF_CHAR))].encode_as_key(RelationId::SYSTEM);
        let mut rows: Vec<Vec<JsonValue>> = vec![];
        for kv_res in tx.store_tx.range_scan(&lower, &upper) {
            let (k_slice, v_slice) = kv_res?;
            if upper <= k_slice {
                break;
            }
            let meta = RelationHandle::decode(&v_slice)?;
            let n_keys = meta.metadata.keys.len();
            let n_dependents = meta.metadata.non_keys.len();
            let arity = n_keys + n_dependents;
            let name = meta.name;
            let access_level = if name.contains(':') {
                "index".to_string()
            } else {
                meta.access_level.to_string()
            };
            rows.push(vec![
                json!(name),
                json!(arity),
                json!(access_level),
                json!(n_keys),
                json!(n_dependents),
                json!(meta.put_triggers.len()),
                json!(meta.rm_triggers.len()),
                json!(meta.replace_triggers.len()),
                json!(meta.description),
            ]);
        }
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(DataValue::from).collect_vec())
            .collect_vec();
        Ok(NamedRows::new(
            vec![
                "name".to_string(),
                "arity".to_string(),
                "access_level".to_string(),
                "n_keys".to_string(),
                "n_non_keys".to_string(),
                "n_put_triggers".to_string(),
                "n_rm_triggers".to_string(),
                "n_replace_triggers".to_string(),
                "description".to_string(),
            ],
            rows,
        ))
    }
}

/// Evaluate a string expression in the context of a set of parameters and variables
pub fn evaluate_expressions(
    src: &str,
    params: &BTreeMap<String, DataValue>,
    vars: &BTreeMap<String, DataValue>,
) -> Result<DataValue> {
    _evaluate_expressions(src, params, vars).map_err(|err| {
        if err.source().is_none() {
            err.with_source_code(format!("{src} "))
        } else {
            err
        }
    })
}

/// Get the variables referenced in a string expression
pub fn get_variables(src: &str, params: &BTreeMap<String, DataValue>) -> Result<BTreeSet<String>> {
    _get_variables(src, params).map_err(|err| {
        if err.source().is_none() {
            err.with_source_code(format!("{src} "))
        } else {
            err
        }
    })
}

fn _evaluate_expressions(
    src: &str,
    params: &BTreeMap<String, DataValue>,
    vars: &BTreeMap<String, DataValue>,
) -> Result<DataValue> {
    let mut expr = parse_expressions(src, params)?;
    let mut ctx = vec![];
    let mut binding_map = BTreeMap::new();
    for (i, (k, v)) in vars.iter().enumerate() {
        ctx.push(v.clone());
        binding_map.insert(Symbol::new(k, Default::default()), i);
    }
    expr.fill_binding_indices(&binding_map)?;
    expr.eval(&ctx)
}

fn _get_variables(src: &str, params: &BTreeMap<String, DataValue>) -> Result<BTreeSet<String>> {
    let expr = parse_expressions(src, params)?;
    expr.get_variables()
}

/// Used for user-initiated termination of running queries
#[derive(Clone, Default)]
pub struct Poison(pub(crate) Arc<AtomicBool>);

impl Poison {
    /// Will return `Err` if user has initiated termination.
    #[inline(always)]
    pub fn check(&self) -> Result<()> {
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

pub(crate) fn seconds_since_the_epoch() -> Result<f64> {
    #[cfg(not(target_arch = "wasm32"))]
    let now = SystemTime::now();
    #[cfg(not(target_arch = "wasm32"))]
    return Ok(now
        .duration_since(UNIX_EPOCH)
        .into_diagnostic()?
        .as_secs_f64());

    #[cfg(target_arch = "wasm32")]
    Ok(js_sys::Date::now())
}
