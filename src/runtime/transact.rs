use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use log::error;
use miette::{Diagnostic, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use cozorocks::CfHandle::{Pri, Snd};
use cozorocks::{DbIter, Tx};

use crate::data::attr::Attribute;
use crate::data::encode::{
    encode_sentinel_attr_by_id, encode_sentinel_entity_attr, encode_tx, EncodedVec,
};
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::program::MagicSymbol;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::derived::{DerivedRelStore, DerivedRelStoreId};
use crate::runtime::relation::RelationId;

pub struct SessionTx {
    pub(crate) tx: Tx,
    pub(crate) relation_store_id: Arc<AtomicU64>,
    pub(crate) mem_store_id: Arc<AtomicU32>,
    pub(crate) w_tx_id: Option<TxId>,
    pub(crate) last_attr_id: Arc<AtomicU64>,
    pub(crate) last_ent_id: Arc<AtomicU64>,
    pub(crate) last_tx_id: Arc<AtomicU64>,
    pub(crate) attr_by_id_cache: RefCell<BTreeMap<AttrId, Option<Attribute>>>,
    pub(crate) attr_by_kw_cache: RefCell<BTreeMap<SmartString<LazyCompact>, Option<Attribute>>>,
    pub(crate) eid_by_attr_val_cache:
        RefCell<BTreeMap<DataValue, BTreeMap<(AttrId, Validity), Option<EntityId>>>>,
    // "touched" requires the id to exist prior to the transaction, and something related to it has changed
}

#[derive(
    Clone, PartialEq, Ord, PartialOrd, Eq, Debug, serde_derive::Deserialize, serde_derive::Serialize,
)]
pub(crate) struct TxLog {
    pub(crate) id: TxId,
    pub(crate) comment: String,
    pub(crate) ts: Validity,
}

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
#[error("Cannot deserialize tx log")]
#[diagnostic(code(deser::tx_log))]
#[diagnostic(help("This could indicate a bug. Consider file a bug report."))]
pub(crate) struct TxLogDeserError;

impl TxLog {
    pub(crate) fn new(id: TxId, comment: &str) -> Self {
        let timestamp = Validity::current();
        Self {
            id,
            comment: comment.to_string(),
            ts: timestamp,
        }
    }
    pub(crate) fn encode(&self) -> EncodedVec<64> {
        let mut store = SmallVec::<[u8; 64]>::new();
        self.serialize(&mut Serializer::new(&mut store)).unwrap();
        EncodedVec { inner: store }
    }
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data).map_err(|err| {
            error!(
                "Cannot deserialize tx log from bytes: {:x?}, {:?}",
                data, err
            );
            TxLogDeserError
        })?)
    }
}

impl SessionTx {
    pub(crate) fn new_rule_store(&self, rule_name: MagicSymbol, arity: usize) -> DerivedRelStore {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        DerivedRelStore::new(DerivedRelStoreId(old_count), rule_name, arity)
    }

    pub(crate) fn new_temp_store(&self, span: SourceSpan) -> DerivedRelStore {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        DerivedRelStore::new(
            DerivedRelStoreId(old_count),
            MagicSymbol::Muggle {
                inner: Symbol::new("", span),
            },
            0,
        )
    }

    pub(crate) fn clear_cache(&self) {
        self.attr_by_id_cache.borrow_mut().clear();
        self.attr_by_kw_cache.borrow_mut().clear();
        self.eid_by_attr_val_cache.borrow_mut().clear();
    }

    pub(crate) fn load_last_entity_id(&self) -> Result<EntityId> {
        let e_lower = encode_sentinel_entity_attr(EntityId::MIN_PERM, AttrId::MIN_PERM);
        let e_upper = encode_sentinel_entity_attr(EntityId::MAX_PERM, AttrId::MIN_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);

        Ok(match it.key()? {
            None => EntityId::MAX_TEMP,
            Some(data) => EntityId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_attr_id(&self) -> Result<AttrId> {
        let e_lower = encode_sentinel_attr_by_id(AttrId::MIN_PERM);
        let e_upper = encode_sentinel_attr_by_id(AttrId::MAX_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);
        Ok(match it.key()? {
            None => AttrId::MAX_TEMP,
            Some(data) => AttrId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_relation_store_id(&self) -> Result<RelationId> {
        let tuple = Tuple(vec![DataValue::Null]);
        let t_encoded = tuple.encode_as_key(RelationId::SYSTEM);
        let found = self.tx.get(&t_encoded, false, Snd)?;
        Ok(match found {
            None => RelationId::SYSTEM,
            Some(slice) => RelationId::raw_decode(&slice),
        })
    }

    pub(crate) fn load_last_tx_id(&self) -> Result<TxId> {
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
        self.tx.put(&encoded, &log.encode(), Pri)?;
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
        #[derive(Error, Diagnostic, Debug)]
        #[error("Attempting to write in read-only mode")]
        #[diagnostic(code(query::readonly_violation))]
        #[diagnostic(help("This indicates a bug. Please report it."))]
        struct WriteInReadOnlyModeError;

        Ok(self.w_tx_id.ok_or_else(|| WriteInReadOnlyModeError)?)
    }
    pub(crate) fn bounded_scan_first(&self, lower: &[u8], upper: &[u8]) -> DbIter {
        // this is tricky, must be written like this!
        let mut it = self.tx.iterator(Pri).upper_bound(upper).start();
        it.seek(lower);
        it
    }

    pub(crate) fn bounded_scan_last(&self, lower: &[u8], upper: &[u8]) -> DbIter {
        // this is tricky, must be written like this!
        let mut it = self
            .tx
            .iterator(Pri)
            .lower_bound(lower)
            .upper_bound(upper)
            .start();
        it.seek_to_end();
        it
    }
}
