use std::sync::atomic::Ordering;

use anyhow::Result;

use cozorocks::{DbIter, IterBuilder};

use crate::AttrTxItem;
use crate::data::attr::Attribute;
use crate::data::encode::{
    encode_attr_by_id, encode_sentinel_attr_by_id, encode_sentinel_attr_by_kw, VEC_SIZE_8,
};
use crate::data::id::AttrId;
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::runtime::transact::{SessionTx, TransactError};
use crate::utils::swap_option_result;

impl SessionTx {
    pub fn tx_attrs(&mut self, payloads: Vec<AttrTxItem>) -> Result<Vec<(StoreOp, AttrId)>> {
        let mut ret = Vec::with_capacity(payloads.len());
        for item in payloads {
            let id = item.attr.id;
            let kw = item.attr.keyword.clone();
            if item.op.is_retract() {
                if item.attr.id.is_perm() {
                    ret.push((item.op, self.retract_attr(item.attr.id)?));
                } else {
                    ret.push((item.op, self.retract_attr_by_kw(&item.attr.keyword)?));
                }
            } else if item.attr.id.is_perm() {
                ret.push((item.op, self.amend_attr(item.attr)?));
            } else {
                ret.push((item.op, self.new_attr(item.attr)?));
            }
            self.attr_by_id_cache.remove(&id);
            self.attr_by_kw_cache.remove(&kw);
        }
        Ok(ret)
    }

    pub(crate) fn attr_by_id(&mut self, aid: AttrId) -> Result<Option<Attribute>> {
        if let Some(res) = self.attr_by_id_cache.get(&aid) {
            return Ok(res.clone());
        }

        let anchor = encode_sentinel_attr_by_id(aid);
        Ok(match self.tx.get(&anchor, false)? {
            None => {
                self.attr_by_id_cache.insert(aid, None);
                None
            }
            Some(v_slice) => {
                let data = v_slice.as_ref();
                let op = StoreOp::try_from(data[0])?;
                let attr = Attribute::decode(&data[VEC_SIZE_8..])?;
                if op.is_retract() {
                    self.attr_by_id_cache.insert(attr.id, None);
                    self.attr_by_kw_cache.insert(attr.keyword.clone(), None);
                    None
                } else {
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

        let anchor = encode_sentinel_attr_by_kw(kw);
        Ok(match self.tx.get(&anchor, false)? {
            None => {
                self.attr_by_kw_cache.insert(kw.clone(), None);
                None
            }
            Some(v_slice) => {
                let data = v_slice.as_ref();
                let op = StoreOp::try_from(data[0])?;
                debug_assert!(data.len() > 8);
                let attr = Attribute::decode(&data[VEC_SIZE_8..])?;
                if op.is_retract() {
                    self.attr_by_id_cache.insert(attr.id, None);
                    self.attr_by_kw_cache.insert(kw.clone(), None);
                    None
                } else {
                    self.attr_by_id_cache.insert(attr.id, Some(attr.clone()));
                    self.attr_by_kw_cache
                        .insert(attr.keyword.clone(), Some(attr.clone()));
                    Some(attr)
                }
            }
        })
    }

    pub(crate) fn all_attrs(&mut self) -> impl Iterator<Item=Result<Attribute>> {
        AttrIter::new(self.tx.iterator())
    }

    /// conflict if new attribute has same name as existing one
    pub(crate) fn new_attr(&mut self, mut attr: Attribute) -> Result<AttrId> {
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
        self.put_attr(&attr, StoreOp::Assert)
    }

    /// conflict if asserted attribute has name change, and the name change conflicts with an existing attr,
    /// or if the attr_id doesn't already exist (or retracted),
    /// or if changing immutable properties (cardinality, val_type, indexing)
    pub(crate) fn amend_attr(&mut self, attr: Attribute) -> Result<AttrId> {
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
            let kw_sentinel = encode_sentinel_attr_by_kw(&existing.keyword);
            let attr_data = existing.encode_with_op_and_tx(StoreOp::Retract, tx_id);
            self.tx.put(&kw_sentinel, &attr_data)?;
        }
        self.put_attr(&attr, StoreOp::Assert)
    }

    fn put_attr(&mut self, attr: &Attribute, op: StoreOp) -> Result<AttrId> {
        let tx_id = self.get_write_tx_id()?;
        let attr_data = attr.encode_with_op_and_tx(op, tx_id);
        let id_encoded = encode_attr_by_id(attr.id, tx_id);
        self.tx.put(&id_encoded, &attr_data)?;
        let id_sentinel = encode_sentinel_attr_by_id(attr.id);
        self.tx.put(&id_sentinel, &attr_data)?;
        let kw_sentinel = encode_sentinel_attr_by_kw(&attr.keyword);
        self.tx.put(&kw_sentinel, &attr_data)?;
        Ok(attr.id)
    }

    /// conflict if retracted attr_id doesn't already exist, or is already retracted
    pub(crate) fn retract_attr(&mut self, aid: AttrId) -> Result<AttrId> {
        match self.attr_by_id(aid)? {
            None => Err(TransactError::AttrConflict(
                aid,
                "attempting to retract non-existing attribute".to_string(),
            )
                .into()),
            Some(attr) => {
                self.put_attr(&attr, StoreOp::Retract)?;
                Ok(attr.id)
            }
        }
    }

    pub(crate) fn retract_attr_by_kw(&mut self, kw: &Keyword) -> Result<AttrId> {
        let attr = self
            .attr_by_kw(kw)?
            .ok_or_else(|| TransactError::AttrNotFoundKw(kw.clone()))?;
        self.retract_attr(attr.id)
    }
}

struct AttrIter {
    it: DbIter,
    started: bool,
}

impl AttrIter {
    fn new(builder: IterBuilder) -> Self {
        let upper_bound = encode_sentinel_attr_by_id(AttrId::MAX_PERM);
        let it = builder.upper_bound(&upper_bound).start();
        Self { it, started: false }
    }

    fn next_inner(&mut self) -> Result<Option<Attribute>> {
        if !self.started {
            let lower_bound = encode_sentinel_attr_by_id(AttrId::MIN_PERM);
            self.it.seek(&lower_bound);
            self.started = true;
        } else {
            self.it.next();
        }
        loop {
            match self.it.val()? {
                None => return Ok(None),
                Some(v) => {
                    let found_op = StoreOp::try_from(v[0])?;
                    if found_op.is_retract() {
                        self.it.next();
                        continue;
                    }
                    return Ok(Some(Attribute::decode(&v[VEC_SIZE_8..])?));
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
