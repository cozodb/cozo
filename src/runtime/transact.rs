use crate::data::encode::{encode_tx, encode_unique_attr_by_id, encode_unique_entity};
use crate::data::id::{AttrId, EntityId, TxId};
use anyhow::Result;
use cozorocks::Tx;

pub(crate) struct SessionTx {
    pub(crate) tx: Tx,
}

impl SessionTx {
    pub(crate) fn load_last_entity_id(&mut self) -> Result<EntityId> {
        let e_lower = encode_unique_entity(EntityId::MIN_PERM);
        let e_upper = encode_unique_entity(EntityId::MAX_PERM);

        let mut it = self
            .tx
            .iterator()
            .lower_bound(&e_lower)
            .lower_bound(&e_upper)
            .start();

        it.seek_to_end();
        Ok(match it.key()? {
            None => EntityId::MAX_TEMP,
            Some(data) => EntityId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_attr_id(&mut self) -> Result<AttrId> {
        let e_lower = encode_unique_attr_by_id(AttrId::MIN_PERM);
        let e_upper = encode_unique_attr_by_id(AttrId::MAX_PERM);

        let mut it = self
            .tx
            .iterator()
            .lower_bound(&e_lower)
            .lower_bound(&e_upper)
            .start();

        it.seek_to_end();
        Ok(match it.key()? {
            None => AttrId::MAX_TEMP,
            Some(data) => AttrId::from_bytes(data),
        })
    }

    pub(crate) fn load_last_tx_id(&mut self) -> Result<TxId> {
        let e_lower = encode_tx(TxId::MAX_USER);
        let e_upper = encode_tx(TxId::MIN_USER);

        let mut it = self
            .tx
            .iterator()
            .lower_bound(&e_lower)
            .lower_bound(&e_upper)
            .start();

        it.seek_to_start();
        Ok(match it.key()? {
            None => TxId::MAX_SYS,
            Some(data) => TxId::from_bytes(data),
        })
    }
}
