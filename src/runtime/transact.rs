use crate::data::encode::{encode_tx, encode_unique_attr_by_id, encode_unique_entity};
use crate::data::id::{AttrId, EntityId, TxId};
use anyhow::Result;
use cozorocks::{DbIter, Tx};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub(crate) struct SessionTx {
    pub(crate) tx: Tx,
    pub(crate) r_tx_id: TxId,
    pub(crate) w_tx_id: Option<TxId>,
    pub(crate) last_attr_id: Arc<AtomicU64>,
    pub(crate) last_ent_id: Arc<AtomicU64>,
    pub(crate) last_tx_id: Arc<AtomicU64>,
}

impl SessionTx {
    pub(crate) fn load_last_entity_id(&mut self) -> Result<EntityId> {
        let e_lower = encode_unique_entity(EntityId::MIN_PERM);
        let e_upper = encode_unique_entity(EntityId::MAX_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);

        Ok(match it.key()? {
            None => EntityId::MAX_TEMP,
            Some(data) => EntityId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_attr_id(&mut self) -> Result<AttrId> {
        let e_lower = encode_unique_attr_by_id(AttrId::MIN_PERM);
        let e_upper = encode_unique_attr_by_id(AttrId::MAX_PERM);
        let it = self.bounded_scan_last(&e_lower, &e_upper);
        Ok(match it.key()? {
            None => AttrId::MAX_TEMP,
            Some(data) => AttrId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_tx_id(&mut self) -> Result<TxId> {
        let e_lower = encode_tx(TxId::MAX_USER);
        let e_upper = encode_tx(TxId::MIN_USER);
        let it = self.bounded_scan_first(&e_lower, &e_upper);
        Ok(match it.key()? {
            None => TxId::MAX_SYS,
            Some(data) => TxId::from_bytes(data),
        })
    }

    pub(crate) fn commit_tx(&mut self, message: &str, refresh: bool) -> Result<()> {
        let tx_id = self.get_write_tx_id()?;
        let encoded = encode_tx(tx_id);
        self.tx.put(&encoded, message.as_bytes())?;
        self.tx.commit()?;
        if refresh {
            let new_tx_id = TxId(self.last_tx_id.fetch_add(1, Ordering::AcqRel) + 1);
            self.tx.set_snapshot();
            self.r_tx_id = new_tx_id;
            self.w_tx_id = Some(new_tx_id);
        }
        Ok(())
    }

    pub(crate) fn get_write_tx_id(&self) -> std::result::Result<TxId, TransactError> {
        self.w_tx_id.ok_or(TransactError::WriteInReadOnly)
    }
    pub(crate) fn bounded_scan(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        self.tx
            .iterator()
            .lower_bound(&lower)
            .lower_bound(&upper)
            .start()
    }
    pub(crate) fn bounded_scan_first(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        let mut it = self.tx.iterator().upper_bound(upper).start();
        it.seek(lower);
        it
    }

    pub(crate) fn bounded_scan_last(&mut self, lower: &[u8], upper: &[u8]) -> DbIter {
        let mut it = self.tx.iterator().lower_bound(lower).start();
        it.seek_back(upper);
        it
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransactError {
    #[error("attribute conflict for {0:?}: {1}")]
    AttrConflict(AttrId, String),
    #[error("attempt to write in read-only transaction")]
    WriteInReadOnly,
}
