use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use anyhow::Result;
use itertools::Itertools;
use serde_json::json;

use cozorocks::{DbBuilder, DbIter, RocksDb};

use crate::AttrTxItem;
use crate::data::compare::{DB_KEY_PREFIX_LEN, rusty_cmp};
use crate::data::encode::{
    decode_ea_key, decode_value_from_key, decode_value_from_val, encode_eav_key, StorageTag,
};
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::triple::StoreOp;
use crate::data::value::Value;
use crate::runtime::transact::SessionTx;

pub struct Db {
    db: RocksDb,
    last_attr_id: Arc<AtomicU64>,
    last_ent_id: Arc<AtomicU64>,
    last_tx_id: Arc<AtomicU64>,
    n_sessions: Arc<AtomicUsize>,
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
    pub fn build(builder: DbBuilder) -> Result<Self> {
        let db = builder
            .use_bloom_filter(true, 10., true)
            .use_capped_prefix_extractor(true, DB_KEY_PREFIX_LEN)
            .use_custom_comparator("cozo_rusty_cmp", rusty_cmp, false)
            .build()?;
        let ret = Self {
            db,
            last_attr_id: Arc::new(Default::default()),
            last_ent_id: Arc::new(Default::default()),
            last_tx_id: Arc::new(Default::default()),
            n_sessions: Arc::new(Default::default()),
            session_id: Default::default(),
        };
        ret.load_last_ids()?;
        Ok(ret)
    }

    pub fn new_session(&self) -> Result<Self> {
        let old_count = self.n_sessions.fetch_add(1, Ordering::AcqRel);

        Ok(Self {
            db: self.db.clone(),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            n_sessions: self.n_sessions.clone(),
            session_id: old_count + 1,
        })
    }

    fn load_last_ids(&self) -> Result<()> {
        let mut tx = self.transact()?;
        self.last_tx_id
            .store(tx.load_last_tx_id()?.0, Ordering::Release);
        self.last_attr_id
            .store(tx.load_last_attr_id()?.0, Ordering::Release);
        self.last_ent_id
            .store(tx.load_last_entity_id()?.0, Ordering::Release);
        Ok(())
    }
    pub fn transact(&self) -> Result<SessionTx> {
        let ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            w_tx_id: None,
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            temp_entity_to_perm: Default::default(),
            eid_by_attr_val_cache: Default::default(),
            touched_eids: Default::default(),
        };
        Ok(ret)
    }
    pub fn transact_write(&self) -> Result<SessionTx> {
        let last_tx_id = self.last_tx_id.fetch_add(1, Ordering::AcqRel);
        let cur_tx_id = TxId(last_tx_id + 1);

        let ret = SessionTx {
            tx: self.db.transact().set_snapshot(true).start(),
            w_tx_id: Some(cur_tx_id),
            last_attr_id: self.last_attr_id.clone(),
            last_ent_id: self.last_ent_id.clone(),
            last_tx_id: self.last_tx_id.clone(),
            attr_by_id_cache: Default::default(),
            attr_by_kw_cache: Default::default(),
            temp_entity_to_perm: Default::default(),
            eid_by_attr_val_cache: Default::default(),
            touched_eids: Default::default(),
        };
        Ok(ret)
    }
    pub fn total_iter(&self) -> DbIter {
        let mut it = self.db.transact().start().iterator().start();
        it.seek_to_start();
        it
    }
    pub fn transact_triples(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let mut tx = self.transact_write()?;
        let (payloads, comment) = tx.parse_tx_requests(payload)?;
        let res: serde_json::Value = tx
            .tx_triples(payloads)?
            .iter()
            .map(|(eid, size)| json!([eid.0, size]))
            .collect();
        let tx_id = tx.get_write_tx_id()?;
        tx.commit_tx(&comment, false)?;
        Ok(json!({
            "tx_id": tx_id,
            "results": res
        }))
    }
    pub fn transact_attributes(&self, payload: &serde_json::Value) -> Result<serde_json::Value> {
        let (attrs, comment) = AttrTxItem::parse_request(payload)?;
        let mut tx = self.transact_write()?;
        let res: serde_json::Value = tx
            .tx_attrs(attrs)?
            .iter()
            .map(|(op, aid)| json!([aid.0, op.to_string()]))
            .collect();
        let tx_id = tx.get_write_tx_id()?;
        tx.commit_tx(&comment, false)?;
        Ok(json!({
            "tx_id": tx_id,
            "results": res
        }))
    }
    pub fn current_schema(&self) -> Result<serde_json::Value> {
        let mut tx = self.transact()?;
        tx.all_attrs().map_ok(|v| v.to_json()).try_collect()
    }
    pub fn entities_at(&self, vld: Option<Validity>) -> Result<serde_json::Value> {
        let vld = vld.unwrap_or_else(Validity::current);
        let mut tx = self.transact()?;
        let mut current = encode_eav_key(
            EntityId::MIN_PERM,
            AttrId::MIN_PERM,
            &Value::Null,
            Validity::MAX,
        );
        let upper_bound = encode_eav_key(
            EntityId::MAX_PERM,
            AttrId::MAX_PERM,
            &Value::Bottom,
            Validity::MIN,
        );
        let mut it = tx
            .tx
            .iterator()
            .upper_bound(&upper_bound)
            .total_order_seek(true)
            .start();
        let mut collected: BTreeMap<EntityId, serde_json::Value> = BTreeMap::default();
        it.seek(&current);
        while let Some((k_slice, v_slice)) = it.pair()? {
            debug_assert_eq!(
                StorageTag::try_from(k_slice[0])?,
                StorageTag::TripleEntityAttrValue
            );
            let (e_found, a_found, vld_found) = decode_ea_key(k_slice)?;
            current.copy_from_slice(k_slice);

            if vld_found > vld {
                current.encoded_entity_amend_validity(vld);
                it.seek(&current);
                continue;
            }
            let op = StoreOp::try_from(v_slice[0])?;
            if op.is_retract() {
                current.encoded_entity_amend_validity_to_inf_past();
                it.seek(&current);
                continue;
            }
            let attr = tx.attr_by_id(a_found)?;
            if attr.is_none() {
                current.encoded_entity_amend_validity_to_inf_past();
                it.seek(&current);
                continue;
            }
            let attr = attr.unwrap();
            let value = if attr.cardinality.is_one() {
                decode_value_from_val(v_slice)?
            } else {
                decode_value_from_key(k_slice)?
            };
            let json_for_entry = collected.entry(e_found).or_insert_with(|| json!({}));
            let map_for_entry = json_for_entry.as_object_mut().unwrap();
            map_for_entry.insert("_id".to_string(), e_found.0.into());
            if attr.cardinality.is_many() {
                let arr = map_for_entry
                    .entry(attr.keyword.to_string_no_prefix())
                    .or_insert_with(|| json!([]));
                let arr = arr.as_array_mut().unwrap();
                arr.push(value.into());
            } else {
                map_for_entry.insert(attr.keyword.to_string_no_prefix(), value.into());
            }
            current.encoded_entity_amend_validity_to_inf_past();
            it.seek(&current);
        }
        let collected = collected.into_iter().map(|(_, v)| v).collect_vec();
        Ok(json!(collected))
    }
}
