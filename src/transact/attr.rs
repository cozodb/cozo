use crate::data::attr::Attribute;
use crate::data::encode::{
    decode_attr_key_by_id, decode_attr_key_by_kw, encode_attr_by_id, encode_attr_by_kw,
    encode_unique_attr_by_id, encode_unique_attr_by_kw,
};
use crate::data::id::{AttrId,  TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::runtime::transact::{SessionTx, TransactError};
use anyhow::Result;
use std::sync::atomic::{ Ordering};

impl SessionTx {
    pub(crate) fn attr_by_id(&mut self, aid: AttrId) -> Result<Option<Attribute>> {
        let anchor = encode_attr_by_id(aid, self.r_tx_id, StoreOp::Retract);
        let upper = encode_attr_by_id(aid, TxId::MAX_SYS, StoreOp::Assert);
        let it = self.bounded_scan_first(&anchor, &upper);
        Ok(match it.pair()? {
            None => None,
            Some((k, v)) => {
                let (_, _, op) = decode_attr_key_by_id(k)?;
                if op.is_retract() {
                    None
                } else {
                    Some(Attribute::decode(v)?)
                }
            }
        })
    }

    pub(crate) fn attr_by_kw(&mut self, kw: &Keyword) -> Result<Option<Attribute>> {
        let anchor = encode_attr_by_kw(&kw, self.r_tx_id, StoreOp::Retract);
        let upper = encode_attr_by_kw(&kw, TxId::MAX_SYS, StoreOp::Assert);
        let it = self.bounded_scan_first(&anchor, &upper);
        Ok(match it.pair()? {
            None => None,
            Some((k, v)) => {
                let (_, _, op) = decode_attr_key_by_kw(k)?;
                if op.is_retract() {
                    None
                } else {
                    Some(Attribute::decode(v)?)
                }
            }
        })
    }

    pub(crate) fn all_attrs(&mut self) -> Result<Vec<Attribute>> {
        todo!()
    }

    /// conflict if new attribute has same name as existing one
    pub(crate) fn new_attr(&mut self, mut attr: Attribute) -> Result<()> {
        if self.attr_by_kw(&attr.alias)?.is_some() {
            return Err(TransactError::AttrConflict(
                attr.id,
                format!(
                    "new attribute conflicts with existing one for alias {}",
                    attr.alias
                ),
            )
            .into());
        }
        attr.id = AttrId(self.last_attr_id.fetch_add(1, Ordering::AcqRel) + 1);
        self.put_attr(&attr)
    }

    /// conflict if asserted attribute has name change, and the name change conflicts with an existing attr,
    /// or if the attr_id doesn't already exist (or retracted),
    /// or if changing immutable properties (cardinality, val_type, indexing)
    pub(crate) fn assert_attr(&mut self, attr: Attribute) -> Result<()> {
        let existing = self.attr_by_id(attr.id)?.ok_or_else(|| {
            TransactError::AttrConflict(attr.id, "expected attributed not found".to_string())
        })?;
        let tx_id = self.get_write_tx_id()?;
        if existing.alias != attr.alias {
            if self.attr_by_kw(&attr.alias)?.is_some() {
                return Err(TransactError::AttrConflict(
                    attr.id,
                    format!("alias conflict: {}", attr.alias),
                )
                .into());
            }
            let kw_encoded = encode_attr_by_kw(&attr.alias, tx_id, StoreOp::Retract);
            self.tx.put(&kw_encoded, &[])?;
            let kw_signal = encode_unique_attr_by_kw(&attr.alias);
            self.tx.put(&kw_signal, &tx_id.bytes())?;
        }
        self.put_attr(&attr)
    }

    fn put_attr(&mut self, attr: &Attribute) -> Result<()> {
        let attr_data = attr.encode();
        let tx_id = self.get_write_tx_id()?;
        let id_encoded = encode_attr_by_id(attr.id, tx_id, StoreOp::Assert);
        self.tx.put(&id_encoded, &attr_data)?;
        let kw_encoded = encode_attr_by_kw(&attr.alias, tx_id, StoreOp::Assert);
        self.tx.put(&kw_encoded, &attr_data)?;
        self.put_attr_guard(&attr)?;
        Ok(())
    }

    /// conflict if retracted attr_id doesn't already exist, or is already retracted
    pub(crate) fn retract_attr(&mut self, aid: AttrId) -> Result<()> {
        match self.attr_by_id(aid)? {
            None => Err(TransactError::AttrConflict(
                aid,
                "attempting to retract non-existing attribute".to_string(),
            )
            .into()),
            Some(attr) => {
                let tx_id = self.get_write_tx_id()?;
                let id_encoded = encode_attr_by_id(aid, tx_id, StoreOp::Retract);
                self.tx.put(&id_encoded, &[])?;
                let kw_encoded = encode_attr_by_kw(&attr.alias, tx_id, StoreOp::Retract);
                self.tx.put(&kw_encoded, &[])?;
                self.put_attr_guard(&attr)?;
                Ok(())
            }
        }
    }

    fn put_attr_guard(&mut self, attr: &Attribute) -> Result<()> {
        let tx_id = self.get_write_tx_id()?;
        let tx_id_bytes = tx_id.bytes();
        let id_signal = encode_unique_attr_by_id(attr.id);
        self.tx.put(&id_signal, &tx_id_bytes)?;
        let kw_signal = encode_unique_attr_by_kw(&attr.alias);
        self.tx.put(&kw_signal, &tx_id_bytes)?;
        Ok(())
    }
}
