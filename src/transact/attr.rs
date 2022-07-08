use crate::data::attr::Attribute;
use crate::data::encode::{
    decode_attr_key_by_id, decode_attr_key_by_kw, encode_attr_by_id, encode_attr_by_kw,
    encode_unique_attr_by_id, encode_unique_attr_by_kw, StorageTag,
};
use crate::data::id::{AttrId, TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::runtime::transact::{SessionTx, TransactError};
use crate::utils::swap_option_result;
use anyhow::Result;
use cozorocks::{DbIter, IterBuilder};
use std::sync::atomic::Ordering;

impl SessionTx {
    pub(crate) fn attr_by_id(&mut self, aid: AttrId) -> Result<Option<Attribute>> {
        if let Some(res) = self.attr_by_id_cache.get(&aid) {
            return Ok(res.clone());
        }

        let anchor = encode_attr_by_id(aid, self.r_tx_id);
        let upper = encode_attr_by_id(aid, TxId::ZERO);
        let it = self.bounded_scan_first(&anchor, &upper);
        Ok(match it.pair()? {
            None => {
                self.attr_by_id_cache.insert(aid, None);
                None
            }
            Some((k, v)) => {
                debug_assert_eq!(k[0], StorageTag::AttrById as u8);
                let (_, _) = decode_attr_key_by_id(k)?;
                let op = StoreOp::try_from(v[0])?;
                if op.is_retract() {
                    self.attr_by_id_cache.insert(aid, None);
                    None
                } else {
                    let attr = Attribute::decode(&v[1..])?;
                    self.attr_by_id_cache.insert(attr.id, Some(attr.clone()));
                    self.attr_by_kw_cache
                        .insert(attr.keyword.clone(), Some(attr.clone()));
                    Some(attr)
                }
            }
        })
    }

    pub(crate) fn attr_by_kw(&mut self, kw: &Keyword) -> Result<Option<Attribute>> {
        if let Some(res) = self.attr_by_kw_cache.get(kw) {
            return Ok(res.clone());
        }

        let anchor = encode_attr_by_kw(kw, self.r_tx_id);
        let upper = encode_attr_by_kw(kw, TxId::ZERO);
        let it = self.bounded_scan_first(&anchor, &upper);
        Ok(match it.pair()? {
            None => {
                self.attr_by_kw_cache.insert(kw.clone(), None);
                None
            }
            Some((k, v)) => {
                let (_, _) = decode_attr_key_by_kw(k)?;
                let op = StoreOp::try_from(v[0])?;
                if op.is_retract() {
                    self.attr_by_kw_cache.insert(kw.clone(), None);
                    None
                } else {
                    let attr = Attribute::decode(&v[1..])?;
                    self.attr_by_id_cache.insert(attr.id, Some(attr.clone()));
                    self.attr_by_kw_cache
                        .insert(attr.keyword.clone(), Some(attr.clone()));
                    Some(attr)
                }
            }
        })
    }

    pub(crate) fn all_attrs(&mut self) -> impl Iterator<Item = Result<Attribute>> {
        AttrIter::new(self.tx.iterator(), self.r_tx_id)
    }

    /// conflict if new attribute has same name as existing one
    pub(crate) fn new_attr(&mut self, mut attr: Attribute) -> Result<()> {
        if attr.cardinality.is_many() && attr.indexing.is_unique_index() {
            return Err(TransactError::AttrConsistency(
                attr.id,
                "cardinality cannot be 'many' for unique or identity attributes".to_string(),
            )
            .into());
        }

        if self.attr_by_kw(&attr.keyword)?.is_some() {
            return Err(TransactError::AttrConflict(
                attr.id,
                format!(
                    "new attribute conflicts with existing one for alias {}",
                    attr.keyword
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
    pub(crate) fn amend_attr(&mut self, attr: Attribute) -> Result<()> {
        let existing = self.attr_by_id(attr.id)?.ok_or_else(|| {
            TransactError::AttrConflict(attr.id, "expected attributed not found".to_string())
        })?;
        let tx_id = self.get_write_tx_id()?;
        if existing.keyword != attr.keyword {
            if self.attr_by_kw(&attr.keyword)?.is_some() {
                return Err(TransactError::AttrConflict(
                    attr.id,
                    format!("alias conflict: {}", attr.keyword),
                )
                .into());
            }
            if existing.val_type != attr.val_type
                || existing.cardinality != attr.cardinality
                || existing.indexing != attr.indexing
                || existing.with_history != attr.with_history
            {
                return Err(TransactError::ChangingImmutableProperty(attr.id).into());
            }
            let kw_encoded = encode_attr_by_kw(&existing.keyword, tx_id);
            self.tx.put(&kw_encoded, &[StoreOp::Retract as u8])?;
            let kw_signal = encode_unique_attr_by_kw(&existing.keyword);
            self.tx
                .put(&kw_signal, &tx_id.bytes_with_op(StoreOp::Retract))?;
        }
        self.put_attr(&attr)
    }

    fn put_attr(&mut self, attr: &Attribute) -> Result<()> {
        let attr_data = attr.encode_with_op(StoreOp::Assert);
        let tx_id = self.get_write_tx_id()?;
        let id_encoded = encode_attr_by_id(attr.id, tx_id);
        self.tx.put(&id_encoded, &attr_data)?;
        let kw_encoded = encode_attr_by_kw(&attr.keyword, tx_id);
        self.tx.put(&kw_encoded, &attr_data)?;
        self.put_attr_guard(attr, StoreOp::Assert)?;
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
                let id_encoded = encode_attr_by_id(aid, tx_id);
                self.tx.put(&id_encoded, &[])?;
                let kw_encoded = encode_attr_by_kw(&attr.keyword, tx_id);
                self.tx.put(&kw_encoded, &[StoreOp::Retract as u8])?;
                self.put_attr_guard(&attr, StoreOp::Retract)?;
                Ok(())
            }
        }
    }

    fn put_attr_guard(&mut self, attr: &Attribute, op: StoreOp) -> Result<()> {
        let tx_id = self.get_write_tx_id()?;
        let tx_id_bytes = tx_id.bytes_with_op(op);
        let id_signal = encode_unique_attr_by_id(attr.id);
        self.tx.put(&id_signal, &tx_id_bytes)?;
        let kw_signal = encode_unique_attr_by_kw(&attr.keyword);
        self.tx.put(&kw_signal, &tx_id_bytes)?;
        Ok(())
    }
}

struct AttrIter {
    it: DbIter,
    tx_bound: TxId,
    last_found: Option<AttrId>,
}

impl AttrIter {
    fn new(builder: IterBuilder, tx_bound: TxId) -> Self {
        let upper_bound = encode_attr_by_id(AttrId::MAX_PERM, TxId::ZERO);
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            tx_bound,
            last_found: None,
        }
    }

    fn next_inner(&mut self) -> Result<Option<Attribute>> {
        loop {
            let id_to_seek = match self.last_found {
                None => AttrId::MIN_PERM,
                Some(id) => AttrId(id.0 + 1),
            };
            let encoded = encode_attr_by_id(id_to_seek, self.tx_bound);
            self.it.seek(&encoded);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k, v)) => {
                    debug_assert_eq!(k[0], StorageTag::AttrById as u8);
                    let (found_aid, found_tid) = decode_attr_key_by_id(k)?;
                    let found_op = StoreOp::try_from(v[0])?;
                    if found_tid > self.tx_bound {
                        self.last_found = Some(AttrId(found_aid.0 - 1));
                        continue;
                    }
                    self.last_found = Some(AttrId(found_aid.0));
                    if found_op.is_retract() {
                        continue;
                    }
                    return Ok(Some(Attribute::decode(&v[1..])?));
                }
            }
        }
    }
}

impl Iterator for AttrIter {
    type Item = Result<Attribute>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
