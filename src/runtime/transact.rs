use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;

use cozorocks::{DbIter, RocksDb, Tx};

use crate::data::attr::Attribute;
use crate::data::encode::{
    encode_sentinel_attr_by_id, encode_sentinel_entity_attr, encode_tx, EncodedVec,
};
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::program::MagicSymbol;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::{DerivedRelStore, DerivedRelStoreId};
use crate::runtime::view::ViewRelId;

pub struct SessionTx {
    pub(crate) tx: Tx,
    pub(crate) view_db: RocksDb,
    pub(crate) view_store_id: Arc<AtomicU64>,
    pub(crate) mem_store_id: Arc<AtomicU32>,
    pub(crate) w_tx_id: Option<TxId>,
    pub(crate) last_attr_id: Arc<AtomicU64>,
    pub(crate) last_ent_id: Arc<AtomicU64>,
    pub(crate) last_tx_id: Arc<AtomicU64>,
    pub(crate) attr_by_id_cache: BTreeMap<AttrId, Option<Attribute>>,
    pub(crate) attr_by_kw_cache: BTreeMap<Symbol, Option<Attribute>>,
    pub(crate) temp_entity_to_perm: BTreeMap<EntityId, EntityId>,
    pub(crate) eid_by_attr_val_cache:
        BTreeMap<DataValue, BTreeMap<(AttrId, Validity), Option<EntityId>>>,
    // "touched" requires the id to exist prior to the transaction, and something related to it has changed
    pub(crate) touched_eids: BTreeSet<EntityId>,
}

#[derive(
    Clone, PartialEq, Ord, PartialOrd, Eq, Debug, serde_derive::Deserialize, serde_derive::Serialize,
)]
pub(crate) struct TxLog {
    #[serde(rename = "i")]
    pub(crate) id: TxId,
    #[serde(rename = "c")]
    pub(crate) comment: String,
    #[serde(rename = "t")]
    pub(crate) timestamp: Validity,
}

impl TxLog {
    pub(crate) fn new(id: TxId, comment: &str) -> Self {
        let timestamp = Validity::current();
        Self {
            id,
            comment: comment.to_string(),
            timestamp,
        }
    }
    pub(crate) fn encode(&self) -> EncodedVec<64> {
        let mut store = SmallVec::<[u8; 64]>::new();
        self.serialize(&mut Serializer::new(&mut store)).unwrap();
        EncodedVec { inner: store }
    }
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
}

impl SessionTx {
    pub(crate) fn new_rule_store(&self, rule_name: MagicSymbol, arity: usize) -> DerivedRelStore {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        DerivedRelStore::new(DerivedRelStoreId(old_count), rule_name, arity)
    }

    pub(crate) fn new_temp_store(&self) -> DerivedRelStore {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        DerivedRelStore::new(
            DerivedRelStoreId(old_count),
            MagicSymbol::Muggle {
                inner: Symbol::from(""),
            },
            0,
        )
    }

    pub(crate) fn clear_cache(&mut self) {
        self.attr_by_id_cache.clear();
        self.attr_by_kw_cache.clear();
        self.temp_entity_to_perm.clear();
        self.touched_eids.clear();
        self.eid_by_attr_val_cache.clear();
    }

    pub(crate) fn load_last_entity_id(&mut self) -> Result<EntityId> {
        let e_lower = encode_sentinel_entity_attr(EntityId::MIN_PERM, AttrId::MIN_PERM);
        let e_upper = encode_sentinel_entity_attr(EntityId::MAX_PERM, AttrId::MIN_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);

        Ok(match it.key()? {
            None => EntityId::MAX_TEMP,
            Some(data) => EntityId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_attr_id(&mut self) -> Result<AttrId> {
        let e_lower = encode_sentinel_attr_by_id(AttrId::MIN_PERM);
        let e_upper = encode_sentinel_attr_by_id(AttrId::MAX_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);
        Ok(match it.key()? {
            None => AttrId::MAX_TEMP,
            Some(data) => AttrId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_view_store_id(&mut self) -> Result<ViewRelId> {
        let tuple = Tuple(vec![DataValue::Null]);
        let t_encoded = tuple.encode_as_key(ViewRelId::SYSTEM);
        let vtx = self.view_db.transact().start();
        let found = vtx.get(&t_encoded, false)?;
        match found {
            None => Ok(ViewRelId::SYSTEM),
            Some(slice) => ViewRelId::raw_decode(&slice),
        }
    }

    pub(crate) fn load_last_tx_id(&mut self) -> Result<TxId> {
        let e_lower = encode_tx(TxId::MAX_USER);
        let e_upper = encode_tx(TxId::MAX_SYS);
        let it = self.bounded_scan_first(&e_lower, &e_upper);
        Ok(match it.key()? {
            None => TxId::MAX_SYS,
            Some(data) => TxId::from_bytes(data),
        })
    }

    pub fn commit_tx(&mut self, comment: &str, refresh: bool) -> Result<()> {
        let tx_id = self.get_write_tx_id()?;
        let encoded = encode_tx(tx_id);

        let log = TxLog::new(tx_id, comment);
        self.tx.put(&encoded, &log.encode())?;
        self.tx.commit()?;
        if refresh {
            let new_tx_id = TxId(self.last_tx_id.fetch_add(1, Ordering::AcqRel) + 1);
            self.tx.set_snapshot();
            self.w_tx_id = Some(new_tx_id);
            self.clear_cache();
        }
        Ok(())
    }

    pub(crate) fn get_write_tx_id(&self) -> Result<TxId> {
        self.w_tx_id
            .ok_or_else(|| anyhow!("attempting to write in read-only transaction"))
    }
    pub(crate) fn bounded_scan(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        self.tx
            .iterator()
            .lower_bound(lower)
            .lower_bound(upper)
            .start()
    }
    pub(crate) fn bounded_scan_first(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        // this is tricky, must be written like this!
        let mut it = self.tx.iterator().upper_bound(upper).start();
        it.seek(lower);
        it
    }

    pub(crate) fn bounded_scan_last(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        // this is tricky, must be written like this!
        let mut it = self
            .tx
            .iterator()
            .lower_bound(lower)
            .upper_bound(upper)
            .start();
        it.seek_to_end();
        it
    }
}
