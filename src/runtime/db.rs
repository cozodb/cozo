use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, thread};

use either::{Left, Right};
use itertools::Itertools;
use log::debug;
use miette::{bail, ensure, miette, IntoDiagnostic, Result, WrapErr};
use serde_json::json;
use smartstring::SmartString;

use cozorocks::CfHandle::{Pri, Snd};
use cozorocks::{DbBuilder, DbIter, RocksDb};

use crate::data::compare::{rusty_cmp, DB_KEY_PREFIX_LEN};
use crate::data::encode::{largest_key, smallest_key};
use crate::data::id::{TxId, Validity};
use crate::data::json::JsonValue;
use crate::data::program::{InputProgram, RelationOp};
use crate::data::symb::Symbol;
use crate::data::tuple::{rusty_scratch_cmp, EncodedTuple, Tuple, SCRATCH_DB_KEY_PREFIX_LEN};
use crate::data::value::{DataValue, LARGEST_UTF_CHAR};
use crate::parse::schema::AttrTxItem;
use crate::parse::sys::{CompactTarget, SysOp};
use crate::parse::tx::TripleTx;
use crate::parse::{parse_script, CozoScript};
use crate::runtime::relation::{RelationId, RelationMetadata};
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

struct RunningQueryHandle {
    started_at: Validity,
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

pub struct Db {
    db: RocksDb,
    last_attr_id: Arc<AtomicU64>,
    last_ent_id: Arc<AtomicU64>,
    last_tx_id: Arc<AtomicU64>,
    relation_store_id: Arc<AtomicU64>,
    n_sessions: Arc<AtomicUsize>,
    queries_count: Arc<AtomicU64>,
    running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
    session_id: usize,
}

impl Debug for Db {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Db<session {}, attrs {:?}, entities {:?}, txs {:?}, sessions: {:?}>",
            self.session_id, self.last_tx_id, self.last_ent_id, self.last_tx_id, self.n_sessions
        )
    }
}

impl Db {
    pub fn build(builder: DbBuilder<'_>) -> Result<Self> {
        let path = builder.opts.db_path;
        fs::create_dir_all(path).into_diagnostic()?;
        let path_buf = PathBuf::from(path);
        let mut store_path = path_buf.clone();
        store_path.push("data");
        let db_builder = builder
            .pri_use_capped_prefix_extractor(true, DB_KEY_PREFIX_LEN)
            .pri_use_custom_comparator("cozo_rusty_cmp", rusty_cmp, false)
            .snd_use_capped_prefix_extractor(true, SCRATCH_DB_KEY_PREFIX_LEN)
            .snd_use_custom_comparator("cozo_rusty_scratch_cmp", rusty_scratch_cmp, false)
            .path(store_path.to_str().unwrap());

        let db = db_builder.build()?;

        let ret = Self {
            db,
            last_attr_id: Arc::new(Default::default()),
            last_ent_id: Arc::new(Default::default()),
            last_tx_id: Arc::new(Default::default()),
            relation_store_id: Arc::new(Default::default()),
            n_sessions: Arc::new(Default::default()),
            queries_count: Arc::new(Default::default()),
            running_queries: Arc::new(Mutex::new(Default::default())),
            session_id: Default::default(),
        };
        ret.load_last_ids()?;
        Ok(ret)
    }

    pub fn compact_triple_store(&self) -> Result<()> {
        let l = smallest_key();
        let u = largest_key();
        self.db.range_compact(&l, &u, Pri)?;
        Ok(())
    }

    pub fn compact_relation(&self) -> Result<()> {
        let l = Tuple::default().encode_as_key(RelationId(0));
        let u = Tuple(vec![DataValue::Bot]).encode_as_key(RelationId(u64::MAX));
        self.db.range_compact(&l, &u, Snd)?;
        Ok(())
    }

    pub fn new_session(&self) -> Result<Self> {
        let old_count = self.n_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(Self {
            db: self.db.clone(),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            relation_store_id: self.relation_store_id.clone(),
            n_sessions: self.n_sessions.clone(),
            queries_count: self.queries_count.clone(),
            running_queries: self.running_queries.clone(),
            session_id: old_count + 1,
        })
    }

    fn load_last_ids(&self) -> Result<()> {
        let tx = self.transact()?;
        self.last_tx_id
            .store(tx.load_last_tx_id()?.0, Ordering::Release);
        self.last_attr_id
            .store(tx.load_last_attr_id()?.0, Ordering::Release);
        self.last_ent_id
            .store(tx.load_last_entity_id()?.0, Ordering::Release);
        self.relation_store_id
            .store(tx.load_last_relation_store_id()?.0, Ordering::Release);
        Ok(())
    }
    pub fn transact(&self) -> Result<SessionTx> {
        let ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            mem_store_id: Default::default(),
            relation_store_id: self.relation_store_id.clone(),
            w_tx_id: None,
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            eid_by_attr_val_cache: Default::default(),
        };
        Ok(ret)
    }
    pub fn transact_write(&self) -> Result<SessionTx> {
        let last_tx_id = self.last_tx_id.fetch_add(1, Ordering::AcqRel);
        let cur_tx_id = TxId(last_tx_id + 1);

        let ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            mem_store_id: Default::default(),
            relation_store_id: self.relation_store_id.clone(),
            w_tx_id: Some(cur_tx_id),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            eid_by_attr_val_cache: Default::default(),
        };
        Ok(ret)
    }
    pub fn total_iter(&self) -> DbIter {
        let mut it = self.db.transact().start().iterator(Pri).start();
        it.seek_to_start();
        it
    }
    fn transact_triples(&self, payloads: TripleTx) -> Result<JsonValue> {
        let mut tx = self.transact_write()?;
        for before_prog in payloads.before {
            self.run_query(&mut tx, before_prog)
                .wrap_err("Triple store transaction failed as a pre-condition failed")?;
        }
        let res: JsonValue = tx
            .tx_triples(payloads.quintuples)?
            .iter()
            .map(|(eid, size)| json!([eid.0, size]))
            .collect();
        for after_prog in payloads.after {
            self.run_query(&mut tx, after_prog)
                .wrap_err("Triple store transaction failed as a post-condition failed")?;
        }
        let tx_id = tx.get_write_tx_id()?;
        tx.commit_tx("", false)?;
        Ok(json!({
            "tx_id": tx_id,
            "results": res
        }))
    }
    fn transact_attributes(&self, attrs: Vec<AttrTxItem>) -> Result<JsonValue> {
        let mut tx = self.transact_write()?;
        let res: JsonValue = tx
            .tx_attrs(attrs)?
            .iter()
            .map(|(op, aid)| json!([aid.0, op.to_string()]))
            .collect();
        let tx_id = tx.get_write_tx_id()?;
        tx.commit_tx("", false)?;
        Ok(json!({
            "tx_id": tx_id,
            "results": res
        }))
    }
    pub fn current_schema(&self) -> Result<JsonValue> {
        let mut tx = self.transact()?;
        let rows: Vec<_> = tx
            .all_attrs()
            .map_ok(|v| {
                vec![
                    json!(v.id.0),
                    json!(v.name),
                    json!(v.val_type.to_string()),
                    json!(v.cardinality.to_string()),
                    json!(v.indexing.to_string()),
                    json!(v.with_history),
                ]
            })
            .try_collect()?;
        Ok(
            json!({"rows": rows, "headers": ["id", "name", "type", "cardinality", "index", "history"]}),
        )
    }
    pub fn run_script(
        &self,
        payload: &str,
        params: &BTreeMap<String, JsonValue>,
    ) -> Result<JsonValue> {
        let param_pool = params
            .iter()
            .map(|(k, v)| (k.clone(), DataValue::from(v)))
            .collect();
        match parse_script(payload, &param_pool)
            .map_err(|e| e.with_source_code(payload.to_string()))?
        {
            CozoScript::Query(p) => {
                let (mut tx, is_write) = if p.out_opts.store_relation.is_some() {
                    (self.transact_write()?, true)
                } else {
                    (self.transact()?, false)
                };
                let (res, cleanups) = self.run_query(&mut tx, p)?;
                if is_write {
                    tx.commit_tx("", false)?;
                } else {
                    ensure!(cleanups.is_empty(), "non-empty cleanups on read-only tx");
                }
                for (lower, upper) in cleanups {
                    self.db.range_del(&lower, &upper, Snd)?;
                }
                Ok(res)
            }
            CozoScript::Tx(tx) => self.transact_triples(tx),
            CozoScript::Schema(schema) => self.transact_attributes(schema),
            CozoScript::Sys(op) => self.run_sys_op(op),
        }
    }
    pub fn run_json_query(&self, _payload: &JsonValue) -> Result<JsonValue> {
        todo!()
    }
    fn run_sys_op(&self, op: SysOp) -> Result<JsonValue> {
        match op {
            SysOp::Compact(opts) => {
                for opt in opts {
                    match opt {
                        CompactTarget::Triples => {
                            self.compact_triple_store()?;
                        }
                        CompactTarget::Relations => {
                            self.compact_relation()?;
                        }
                    }
                }
                Ok(json!({"status": "OK"}))
            }
            SysOp::ListSchema => self.current_schema(),
            SysOp::ListRelations => self.list_relations(),
            SysOp::RemoveRelations(rs) => {
                for r in rs.iter() {
                    self.remove_relation(&r.0)?;
                }
                Ok(json!({"status": "OK"}))
            }
            SysOp::ListRunning => self.list_running(),
            SysOp::KillRunning(id) => {
                let queries = self.running_queries.lock().unwrap();
                Ok(match queries.get(&id) {
                    None => {
                        json!({"status": "NOT_FOUND"})
                    }
                    Some(handle) => {
                        handle.poison.0.store(true, Ordering::Relaxed);
                        json!({"status": "KILLING"})
                    }
                })
            }
        }
    }
    fn run_query(
        &self,
        tx: &mut SessionTx,
        input_program: InputProgram,
    ) -> Result<(JsonValue, Vec<(Vec<u8>, Vec<u8>)>)> {
        let mut clean_ups = vec![];
        if let Some((meta, op)) = &input_program.out_opts.store_relation {
            if *op == RelationOp::Create {
                ensure!(
                    !tx.relation_exists(&meta.name)?,
                    "relation '{}' exists but is required not to be",
                    meta.name
                )
            } else if *op != RelationOp::Rederive {
                ensure!(
                    tx.relation_exists(&meta.name)?,
                    "relation '{}' does not exist but is required to be",
                    meta.name
                )
            }
        };
        let default_vld = Validity::current();
        let program = input_program
            .to_normalized_program(&tx, default_vld)?
            .stratify()?
            .magic_sets_rewrite(&tx, default_vld)?;
        debug!("{:#?}", program);
        let (compiled, stores) =
            tx.stratified_magic_compile(&program, &input_program.const_rules)?;

        let poison = Poison::default();
        if let Some(secs) = input_program.out_opts.timeout {
            poison.set_timeout(secs);
        }
        let id = self.queries_count.fetch_add(1, Ordering::AcqRel);
        let handle = RunningQueryHandle {
            started_at: Validity::current(),
            poison: poison.clone(),
        };
        self.running_queries.lock().unwrap().insert(id, handle);
        let _guard = RunningQueryCleanup {
            id,
            running_queries: self.running_queries.clone(),
        };

        let result = tx.stratified_magic_evaluate(
            &compiled,
            &stores,
            if input_program.out_opts.sorters.is_empty() {
                input_program.out_opts.num_to_take()
            } else {
                None
            },
            poison,
        )?;
        let json_headers = match input_program.get_entry_head() {
            None => JsonValue::Null,
            Some(headers) => headers.iter().map(|v| json!(v.0)).collect(),
        };
        if !input_program.out_opts.sorters.is_empty() {
            let entry_head = input_program
                .get_entry_head()
                .ok_or_else(|| miette!("program entry head must be defined for sorters to work"))?
                .to_vec();
            let sorted_result =
                tx.sort_and_collect(result, &input_program.out_opts.sorters, &entry_head)?;
            let sorted_iter = if let Some(offset) = input_program.out_opts.offset {
                Left(sorted_result.scan_sorted().skip(offset))
            } else {
                Right(sorted_result.scan_sorted())
            };
            let sorted_iter = if let Some(limit) = input_program.out_opts.limit {
                Left(sorted_iter.take(limit))
            } else {
                Right(sorted_iter)
            };
            if let Some((meta, relation_op)) = input_program.out_opts.store_relation {
                let to_clear = tx.execute_relation(sorted_iter, relation_op, &meta)?;
                if let Some(c) = to_clear {
                    clean_ups.push(c);
                }
                Ok((json!({"relation": "OK"}), clean_ups))
            } else {
                let ret: Vec<_> = tx.run_pull_on_query_results(
                    sorted_iter,
                    input_program.get_entry_head(),
                    &input_program.out_opts.out_spec,
                    default_vld,
                )?;
                Ok((json!({ "rows": ret, "headers": json_headers }), clean_ups))
            }
        } else {
            let scan = if input_program.out_opts.limit.is_some()
                || input_program.out_opts.offset.is_some()
            {
                let limit = input_program.out_opts.limit.unwrap_or(usize::MAX);
                let offset = input_program.out_opts.offset.unwrap_or(0);
                Right(result.scan_all().skip(offset).take(limit))
            } else {
                Left(result.scan_all())
            };

            if let Some((meta, relation_op)) = input_program.out_opts.store_relation {
                let to_clear = tx.execute_relation(scan, relation_op, &meta)?;
                if let Some(c) = to_clear {
                    clean_ups.push(c);
                }
                Ok((json!({"relation": "OK"}), clean_ups))
            } else {
                let ret: Vec<_> = tx.run_pull_on_query_results(
                    scan,
                    input_program.get_entry_head(),
                    &input_program.out_opts.out_spec,
                    default_vld,
                )?;
                Ok((json!({ "rows": ret, "headers": json_headers }), clean_ups))
            }
        }
    }
    pub fn remove_relation(&self, name: &str) -> Result<()> {
        let name = Symbol::from(name);
        let mut tx = self.transact_write()?;
        let (lower, upper) = tx.destroy_relation(&name)?;
        self.db.range_del(&lower, &upper, Snd)?;
        Ok(())
    }
    pub fn list_running(&self) -> Result<JsonValue> {
        let res = self
            .running_queries
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| json!([k, format!("{:?}", v.started_at)]))
            .collect_vec();
        Ok(json!({"rows": res, "headers": ["?id", "?started_at"]}))
    }
    pub fn put_meta_kv(&self, k: &[&str], v: &[u8]) -> Result<()> {
        let mut ks = vec![DataValue::Guard];
        for el in k {
            ks.push(DataValue::Str(SmartString::from(*el)));
        }
        let key = Tuple(ks).encode_as_key(RelationId::SYSTEM);
        let mut vtx = self.db.transact().start();
        vtx.put(&key, v, Snd)?;
        vtx.commit()?;
        Ok(())
    }
    pub fn remove_meta_kv(&self, k: &[&str]) -> Result<()> {
        let mut ks = vec![DataValue::Guard];
        for el in k {
            ks.push(DataValue::Str(SmartString::from(*el)));
        }
        let key = Tuple(ks).encode_as_key(RelationId::SYSTEM);
        let mut vtx = self.db.transact().start();
        vtx.del(&key, Snd)?;
        vtx.commit()?;
        Ok(())
    }
    pub fn get_meta_kv(&self, k: &[&str]) -> Result<Option<Vec<u8>>> {
        let mut ks = vec![DataValue::Guard];
        for el in k {
            ks.push(DataValue::Str(SmartString::from(*el)));
        }
        let key = Tuple(ks).encode_as_key(RelationId::SYSTEM);
        let vtx = self.db.transact().start();
        Ok(match vtx.get(&key, false, Snd)? {
            None => None,
            Some(slice) => Some(slice.to_vec()),
        })
    }
    pub fn meta_range_scan(
        &self,
        prefix: &[&str],
    ) -> impl Iterator<Item = Result<(Vec<String>, Vec<u8>)>> {
        let mut lower_bound = Tuple(vec![DataValue::Guard]);
        for p in prefix {
            lower_bound.0.push(DataValue::Str(SmartString::from(*p)));
        }
        let upper_bound = Tuple(vec![DataValue::Bot]);
        let mut it = self
            .db
            .transact()
            .start()
            .iterator(Snd)
            .upper_bound(&upper_bound.encode_as_key(RelationId::SYSTEM))
            .start();
        it.seek(&lower_bound.encode_as_key(RelationId::SYSTEM));

        struct CustomIter {
            it: DbIter,
            started: bool,
        }

        impl CustomIter {
            fn next_inner(&mut self) -> Result<Option<(Vec<String>, Vec<u8>)>> {
                if self.started {
                    self.it.next()
                } else {
                    self.started = true;
                }
                match self.it.pair()? {
                    None => Ok(None),
                    Some((k_slice, v_slice)) => {
                        let encoded = EncodedTuple(k_slice).decode()?;
                        let ks: Vec<_> = encoded
                            .0
                            .into_iter()
                            .skip(1)
                            .map(|v| {
                                v.get_string()
                                    .map(|s| s.to_string())
                                    .ok_or_else(|| miette!("bad key in meta store"))
                            })
                            .try_collect()?;
                        Ok(Some((ks, v_slice.to_vec())))
                    }
                }
            }
        }

        impl Iterator for CustomIter {
            type Item = Result<(Vec<String>, Vec<u8>)>;

            fn next(&mut self) -> Option<Self::Item> {
                swap_option_result(self.next_inner())
            }
        }

        CustomIter { it, started: false }
    }
    pub fn list_relations(&self) -> Result<JsonValue> {
        let lower =
            Tuple(vec![DataValue::Str(SmartString::from(""))]).encode_as_key(RelationId::SYSTEM);
        let upper = Tuple(vec![DataValue::Str(SmartString::from(String::from(
            LARGEST_UTF_CHAR,
        )))])
        .encode_as_key(RelationId::SYSTEM);
        let mut it = self
            .db
            .transact()
            .start()
            .iterator(Snd)
            .upper_bound(&upper)
            .start();
        it.seek(&lower);
        let mut collected = vec![];
        while let Some(v_slice) = it.val()? {
            let meta: RelationMetadata = rmp_serde::from_slice(v_slice).into_diagnostic()?;
            let name = meta.name.0;
            let arity = meta.arity;
            collected.push(json!([name, arity]));
            it.next();
        }
        Ok(json!({"rows": collected, "headers": ["name", "arity"]}))
    }
}

#[derive(Clone, Default)]
pub(crate) struct Poison(pub(crate) Arc<AtomicBool>);

impl Poison {
    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        if self.0.load(Ordering::Relaxed) {
            bail!("killed")
        }
        Ok(())
    }
    pub(crate) fn set_timeout(&self, secs: u64) {
        let pill = self.0.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(secs));
            pill.store(true, Ordering::Relaxed);
        });
    }
}
