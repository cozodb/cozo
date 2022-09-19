use std::cmp::Ordering::Greater;
use std::sync::atomic::Ordering;

use miette::{bail, ensure, Diagnostic, Result};
use smartstring::SmartString;
use thiserror::Error;

use cozorocks::CfHandle::Pri;
use cozorocks::{DbIter, IterBuilder};

use crate::data::attr::Attribute;
use crate::data::compare::compare_triple_store_key;
use crate::data::encode::{
    encode_attr_by_id, encode_sentinel_attr_by_id, encode_sentinel_attr_by_name, VEC_SIZE_8,
};
use crate::data::id::AttrId;
use crate::data::triple::StoreOp;
use crate::EncodedVec;
use crate::parse::schema::AttrTxItem;
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

#[derive(Debug, Error, Diagnostic)]
#[error("Attribute name {0} conflicts with an existing name")]
#[diagnostic(code(eval::attr_name_conflict))]
struct AttrNameConflict(String);

impl SessionTx {
    pub(crate) fn tx_attrs(&mut self, payloads: Vec<AttrTxItem>) -> Result<Vec<(StoreOp, AttrId)>> {
        let mut ret = Vec::with_capacity(payloads.len());
        for item in payloads {
            let id = item.attr.id;
            let kw = item.attr.name.clone();
            if item.op.is_retract() {
                if item.attr.id.is_perm() {
                    ret.push((item.op, self.retract_attr(item.attr.id)?));
                } else {
                    ret.push((item.op, self.retract_attr_by_name(&item.attr.name)?));
                }
            } else if item.attr.id.is_perm() {
                ret.push((item.op, self.amend_attr(item.attr)?));
            } else {
                ret.push((item.op, self.new_attr(item.attr)?));
            }
            self.attr_by_id_cache.borrow_mut().remove(&id);
            self.attr_by_kw_cache.borrow_mut().remove(&kw);
        }
        Ok(ret)
    }

    pub(crate) fn attr_by_id(&self, aid: AttrId) -> Result<Option<Attribute>> {
        if let Some(res) = self.attr_by_id_cache.borrow().get(&aid) {
            return Ok(res.clone());
        }

        let anchor = encode_sentinel_attr_by_id(aid);
        Ok(match self.tx.get(&anchor, false, Pri)? {
            None => {
                self.attr_by_id_cache.borrow_mut().insert(aid, None);
                None
            }
            Some(v_slice) => {
                let data = v_slice.as_ref();
                let op = StoreOp::try_from(data[0])?;
                let attr = Attribute::decode(&data[VEC_SIZE_8..])?;
                if op.is_retract() {
                    self.attr_by_id_cache.borrow_mut().insert(attr.id, None);
                    self.attr_by_kw_cache
                        .borrow_mut()
                        .insert(attr.name, None);
                    None
                } else {
                    self.attr_by_id_cache
                        .borrow_mut()
                        .insert(attr.id, Some(attr.clone()));
                    self.attr_by_kw_cache
                        .borrow_mut()
                        .insert(attr.name.clone(), Some(attr.clone()));
                    Some(attr)
                }
            }
        })
    }

    pub(crate) fn attr_by_name(&self, name: &str) -> Result<Option<Attribute>> {
        if let Some(res) = self.attr_by_kw_cache.borrow().get(name) {
            return Ok(res.clone());
        }

        let anchor = encode_sentinel_attr_by_name(name);
        Ok(match self.tx.get(&anchor, false, Pri)? {
            None => {
                self.attr_by_kw_cache
                    .borrow_mut()
                    .insert(SmartString::from(name), None);
                None
            }
            Some(v_slice) => {
                let data = v_slice.as_ref();
                let op = StoreOp::try_from(data[0])?;
                debug_assert!(data.len() > 8);
                let attr = Attribute::decode(&data[VEC_SIZE_8..])?;
                if op.is_retract() {
                    self.attr_by_id_cache.borrow_mut().insert(attr.id, None);
                    self.attr_by_kw_cache
                        .borrow_mut()
                        .insert(SmartString::from(name), None);
                    None
                } else {
                    self.attr_by_id_cache
                        .borrow_mut()
                        .insert(attr.id, Some(attr.clone()));
                    self.attr_by_kw_cache
                        .borrow_mut()
                        .insert(attr.name.clone(), Some(attr.clone()));
                    Some(attr)
                }
            }
        })
    }

    pub(crate) fn all_attrs(&mut self) -> impl Iterator<Item = Result<Attribute>> {
        AttrIter::new(self.tx.iterator(Pri))
    }

    /// conflict if new attribute has same name as existing one
    pub(crate) fn new_attr(&mut self, mut attr: Attribute) -> Result<AttrId> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Attribute {0} cannot both be uniquely indexed and have cardinality 'many'")]
        #[diagnostic(code(eval::card_unique_conflict))]
        struct CardUniqueConflict(String);
        ensure!(
            !attr.cardinality.is_many() || !attr.indexing.is_unique_index(),
            CardUniqueConflict(attr.name.to_string())
        );

        ensure!(
            self.attr_by_name(&attr.name)?.is_none(),
            AttrNameConflict(attr.name.to_string())
        );

        attr.id = AttrId(self.last_attr_id.fetch_add(1, Ordering::AcqRel) + 1);
        self.put_attr(&attr, StoreOp::Assert)
    }

    /// conflict if asserted attribute has name change, and the name change conflicts with an existing attr,
    /// or if the attr_id doesn't already exist (or retracted),
    /// or if changing immutable properties (cardinality, val_type, indexing)
    pub(crate) fn amend_attr(&mut self, attr: Attribute) -> Result<AttrId> {
        let existing = self
            .attr_by_id(attr.id)?
            .ok_or_else(|| AttrNotFoundError(attr.id.0.to_string()))?;

        #[derive(Debug, Error, Diagnostic)]
        #[error("Attempting to change immutable properties of existing attribute {0}")]
        #[diagnostic(code(eval::change_immutable_attr_prop))]
        #[diagnostic(help(
        "Currently the following are immutable: cardinality, index, history, typing"
        ))]
        struct ChangingImmutablePropertiesOfAttrError(String);

        ensure!(
                existing.val_type == attr.val_type
                    && existing.cardinality == attr.cardinality
                    && existing.indexing == attr.indexing
                    && existing.with_history == attr.with_history,
                ChangingImmutablePropertiesOfAttrError(attr.name.to_string())
            );

        let tx_id = self.get_write_tx_id()?;
        if existing.name != attr.name {
            ensure!(
                self.attr_by_name(&attr.name)?.is_none(),
                AttrNameConflict(attr.name.to_string())
            );
            let kw_sentinel = encode_sentinel_attr_by_name(&existing.name);
            let attr_data = existing.encode_with_op_and_tx(StoreOp::Retract, tx_id);
            self.tx.put(&kw_sentinel, &attr_data, Pri)?;
        }
        self.put_attr(&attr, StoreOp::Assert)
    }

    fn put_attr(&mut self, attr: &Attribute, op: StoreOp) -> Result<AttrId> {
        let tx_id = self.get_write_tx_id()?;
        let attr_data = attr.encode_with_op_and_tx(op, tx_id);
        let id_encoded = encode_attr_by_id(attr.id, tx_id);
        self.tx.put(&id_encoded, &attr_data, Pri)?;
        let id_sentinel = encode_sentinel_attr_by_id(attr.id);
        self.tx.put(&id_sentinel, &attr_data, Pri)?;
        let kw_sentinel = encode_sentinel_attr_by_name(&attr.name);
        self.tx.put(&kw_sentinel, &attr_data, Pri)?;
        Ok(attr.id)
    }

    /// conflict if retracted attr_id doesn't already exist, or is already retracted
    pub(crate) fn retract_attr(&mut self, aid: AttrId) -> Result<AttrId> {
        match self.attr_by_id(aid)? {
            None => bail!(AttrNotFoundError(aid.0.to_string())),
            Some(attr) => {
                self.put_attr(&attr, StoreOp::Retract)?;
                Ok(attr.id)
            }
        }
    }

    pub(crate) fn retract_attr_by_name(&mut self, kw: &str) -> Result<AttrId> {
        let attr = self
            .attr_by_name(kw)?
            .ok_or_else(|| AttrNotFoundError(kw.to_string()))?;
        self.retract_attr(attr.id)
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Required attribute identified by {0} not found")]
#[diagnostic(code(eval::attr_not_found))]
pub(crate) struct AttrNotFoundError(pub(crate) String);

struct AttrIter {
    it: DbIter,
    started: bool,
    upper_bound: EncodedVec<8>
}

impl AttrIter {
    fn new(builder: IterBuilder) -> Self {
        let upper_bound = encode_sentinel_attr_by_id(AttrId::MAX_PERM);
        let it = builder.upper_bound(&upper_bound).start();
        Self { it, started: false, upper_bound }
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
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v)) => {
                    if compare_triple_store_key(&self.upper_bound, k_slice) != Greater {
                        return Ok(None);
                    }

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
