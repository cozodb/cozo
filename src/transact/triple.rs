use crate::data::attr::{Attribute, AttributeTyping};
use crate::data::compare::{compare_key, rusty_cmp};
use crate::data::encode::{
    decode_ae_key, decode_ea_key, decode_vae_key, decode_value, decode_value_from_key,
    decode_value_from_val, encode_aev_key, encode_ave_key, encode_ave_key_for_unique_v,
    encode_eav_key, encode_unique_attr_val, encode_unique_entity_attr, encode_vae_key, EncodedVec,
    LARGE_VEC_SIZE,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::tx_triple::{Quintuple, TxAction};
use crate::data::value::{StaticValue, Value, INLINE_VAL_SIZE_LIMIT};
use crate::runtime::transact::{SessionTx, TransactError};
use crate::utils::swap_option_result;
use anyhow::Result;
use cozorocks::{DbIter, IterBuilder};
use std::sync::atomic::Ordering;

#[derive(Debug, thiserror::Error)]
enum TripleError {
    #[error("use of temp entity id: {0:?}")]
    TempEid(EntityId),
    #[error("use of non-existent entity: {0:?}")]
    EidNotFound(EntityId),
    #[error("unique constraint violated: {0} {1}")]
    UniqueConstraintViolated(Keyword, String),
    #[error("triple not found for {0:?} {1:?} {2:?}")]
    TripleEANotFound(EntityId, AttrId, Validity),
}

impl SessionTx {
    pub fn tx_triples(&mut self, payloads: Vec<Quintuple>) -> Result<Vec<(EntityId, isize)>> {
        let mut ret = Vec::with_capacity(payloads.len());
        for payload in payloads {
            match payload.action {
                TxAction::Put => {
                    let attr = self.attr_by_id(payload.triple.attr)?.unwrap();
                    if payload.triple.id.is_perm() {
                        ret.push((
                            self.amend_triple(
                                payload.triple.id,
                                &attr,
                                &payload.triple.value,
                                payload.validity,
                            )?,
                            1,
                        ));
                    } else {
                        ret.push((
                            self.new_triple(
                                payload.triple.id,
                                &attr,
                                &payload.triple.value,
                                payload.validity,
                            )?,
                            1,
                        ));
                    }
                }
                TxAction::Retract => {
                    let attr = self.attr_by_id(payload.triple.attr)?.unwrap();
                    ret.push((
                        self.retract_triple(
                            payload.triple.id,
                            &attr,
                            &payload.triple.value,
                            payload.validity,
                        )?,
                        -1,
                    ));
                }
                TxAction::RetractAllEA => {
                    let attr = self.attr_by_id(payload.triple.attr)?.unwrap();
                    ret.push((
                        payload.triple.id,
                        self.retract_triples_for_attr(payload.triple.id, &attr, payload.validity)?,
                    ));
                }
                TxAction::RetractAllE => {
                    ret.push((
                        payload.triple.id,
                        self.retract_entity(payload.triple.id, payload.validity)?,
                    ));
                }
                TxAction::Ensure => {
                    let attr = self.attr_by_id(payload.triple.attr)?.unwrap();
                    self.ensure_triple(
                        payload.triple.id,
                        &attr,
                        &payload.triple.value,
                        payload.validity,
                    )?;
                    ret.push((payload.triple.id, 0));
                }
            }
        }
        Ok(ret)
    }
    pub(crate) fn ensure_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
    ) -> Result<()> {
        let aid = attr.id;
        let signal = encode_unique_entity_attr(eid, aid);
        let gen_err = || TransactError::RequiredTripleNotFound(eid, aid);
        self.tx.get(&signal, true)?.ok_or_else(gen_err)?;
        let v_in_key = if attr.cardinality.is_one() {
            &Value::Bottom
        } else {
            v
        };
        let eav_encoded = encode_eav_key(eid, attr.id, v_in_key, vld);
        let eav_encoded_upper = encode_eav_key(eid, attr.id, v_in_key, Validity::MIN);
        let it = self.bounded_scan_first(&eav_encoded, &eav_encoded_upper);
        let (k_slice, v_slice) = it.pair()?.ok_or_else(gen_err)?;
        if StoreOp::try_from(v_slice[0])?.is_retract() {
            return Err(gen_err().into());
        }
        let stored_v = if attr.cardinality.is_one() {
            decode_value_from_val(v_slice)?
        } else {
            decode_value_from_key(k_slice)?
        };
        if stored_v != *v {
            return Err(TransactError::PreconditionFailed(
                eid,
                aid,
                format!("{:?}", v),
                format!("{:?}", stored_v),
            )
            .into());
        }
        Ok(())
    }
    pub(crate) fn put_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
        op: StoreOp,
    ) -> Result<EntityId> {
        let tx_id = self.get_write_tx_id()?;
        let vld_in_key = if attr.with_history {
            vld
        } else {
            Validity::NO_HISTORY
        };
        // elide value in key for eav and aev if cardinality is one
        let (v_in_key, v_in_val) = if attr.cardinality.is_one() {
            (
                &Value::Bottom,
                if op.is_assert() { v } else { &Value::Bottom },
            )
        } else {
            (v, &Value::Bottom)
        };
        let eav_encoded = encode_eav_key(eid, attr.id, v_in_key, vld_in_key);
        let val_encoded = v_in_val.encode_with_op_and_tx(op, tx_id);
        self.tx.put(&eav_encoded, &val_encoded)?;

        // elide value in data for aev if it is big
        let val_encoded = if val_encoded.len() > INLINE_VAL_SIZE_LIMIT {
            Value::Bottom.encode_with_op_and_tx(op, tx_id)
        } else {
            val_encoded
        };

        let aev_encoded = encode_aev_key(attr.id, eid, v_in_key, vld_in_key);
        self.tx.put(&aev_encoded, &val_encoded)?;

        // vae for ref types
        if attr.val_type.is_ref_type() {
            let vae_encoded = encode_vae_key(v.get_entity_id()?, attr.id, eid, vld_in_key);
            self.tx.put(
                &vae_encoded,
                &Value::Bottom.encode_with_op_and_tx(op, tx_id),
            )?;
        }

        // ave for indexing
        if attr.indexing.should_index() {
            // elide e for unique index
            let e_in_key = if attr.indexing.is_unique_index() {
                EntityId(0)
            } else {
                eid
            };
            let ave_encoded = encode_ave_key(attr.id, v, e_in_key, vld_in_key);
            // checking of unique constraints
            if attr.indexing.is_unique_index() {
                let (current_ave_encoded, vld_in_key) = if attr.with_history {
                    (ave_encoded.clone(), vld)
                } else {
                    (
                        encode_ave_key(attr.id, v, e_in_key, Validity::NO_HISTORY),
                        Validity::NO_HISTORY,
                    )
                };
                // back scan
                if attr.with_history {
                    for item in self.triple_av_before_scan(attr.id, v, vld_in_key) {
                        let (_, _, found_eid) = item?;
                        if found_eid != eid {
                            return Err(TripleError::UniqueConstraintViolated(
                                attr.keyword.clone(),
                                format!("{:?}", v),
                            )
                            .into());
                        }
                    }
                }

                for item in self.triple_av_after_scan(attr.id, v, vld_in_key) {
                    let (_, _, found_eid) = item?;
                    if found_eid != eid {
                        return Err(TripleError::UniqueConstraintViolated(
                            attr.keyword.clone(),
                            format!("{:?}", v),
                        )
                        .into());
                    }
                }
            }
            let e_in_val_encoded = Value::EnId(eid).encode_with_op_and_tx(op, tx_id);
            self.tx.put(&ave_encoded, &e_in_val_encoded)?;

            self.tx.put(
                &encode_unique_attr_val(attr.id, v),
                &tx_id.bytes_with_op(op),
            )?;
        }

        self.tx.put(
            &encode_unique_entity_attr(eid, attr.id),
            &tx_id.bytes_with_op(op),
        )?;

        Ok(eid)
    }

    pub(crate) fn new_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
    ) -> Result<EntityId> {
        // invariant: in the preparation step, any identity attr should already be resolved to
        // an existing eid, if there is one
        let eid = if eid.is_perm() {
            eid
        } else {
            match self.temp_entity_to_perm.get(&eid) {
                Some(id) => *id,
                None => {
                    let new_eid = EntityId(self.last_ent_id.fetch_add(1, Ordering::AcqRel) + 1);
                    self.temp_entity_to_perm.insert(eid, new_eid);
                    new_eid
                }
            }
        };
        if attr.val_type.is_ref_type() {
            let v_eid = v.get_entity_id()?;
            if !v_eid.is_perm() {
                let perm_v_eid = match self.temp_entity_to_perm.get(&v_eid) {
                    Some(id) => *id,
                    None => {
                        let new_eid = EntityId(self.last_ent_id.fetch_add(1, Ordering::AcqRel) + 1);
                        self.temp_entity_to_perm.insert(v_eid, new_eid);
                        new_eid
                    }
                };
                let new_v = Value::EnId(perm_v_eid);
                return self.put_triple(eid, attr, &new_v, vld, StoreOp::Assert);
            }
        }
        self.put_triple(eid, attr, v, vld, StoreOp::Assert)
    }

    pub(crate) fn amend_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
    ) -> Result<EntityId> {
        if !eid.is_perm() {
            return Err(TripleError::TempEid(eid).into());
        }
        // checking that the eid actually exists should be done in the preprocessing step
        self.put_triple(eid, attr, v, vld, StoreOp::Retract)
    }

    pub(crate) fn retract_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
    ) -> Result<EntityId> {
        self.put_triple(eid, attr, v, vld, StoreOp::Retract)?;
        if attr.val_type == AttributeTyping::Component {
            let eid_v = v.get_entity_id()?;
            self.retract_entity(eid_v, vld)?;
        }
        Ok(eid)
    }
    pub(crate) fn retract_triples_for_attr(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        vld: Validity,
    ) -> Result<isize> {
        let lower_bound = encode_eav_key(eid, attr.id, &Value::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, attr.id, &Value::Bottom, Validity::MIN);
        self.batch_retract_triple(lower_bound, upper_bound, vld)
    }
    pub(crate) fn retract_entity(&mut self, eid: EntityId, vld: Validity) -> Result<isize> {
        let lower_bound = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, Validity::MAX);
        self.batch_retract_triple(lower_bound, upper_bound, vld)
    }
    fn batch_retract_triple(
        &mut self,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        vld: Validity,
    ) -> Result<isize> {
        let mut it = self.bounded_scan(&lower_bound, &upper_bound);
        let mut current = lower_bound.clone();
        current.encoded_entity_amend_validity(vld);
        let mut counter = 0;
        loop {
            it.seek(&current);
            match it.pair()? {
                None => return Ok(counter),
                Some((k_slice, v_slice)) => {
                    let op = StoreOp::try_from(v_slice[0])?;
                    let (cur_eid, cur_aid, cur_vld) = decode_ea_key(k_slice)?;
                    if cur_vld > vld {
                        current.encoded_entity_amend_validity(vld);
                        continue;
                    }
                    let cur_v = decode_value_from_key(k_slice)?;
                    if op.is_assert() {
                        let cur_attr = self
                            .attr_by_id(cur_aid)?
                            .ok_or(TransactError::AttrNotFound(cur_aid))?;
                        self.retract_triple(cur_eid, &cur_attr, &cur_v, vld)?;
                        counter -= 1;
                    }
                    current = encode_eav_key(cur_eid, cur_aid, &cur_v, Validity::MIN);
                }
            }
        }
    }
    pub(crate) fn eid_by_unique_av(
        &mut self,
        attr: &Attribute,
        v: &Value,
        vld: Validity,
    ) -> Result<Option<EntityId>> {
        if let Some(inner) = self.eid_by_attr_val_cache.get(v) {
            if let Some(found) = inner.get(&(attr.id, vld)) {
                return Ok(*found);
            }
        }

        let lower = encode_ave_key_for_unique_v(attr.id, v, vld);
        let upper = encode_ave_key_for_unique_v(attr.id, v, Validity::MIN);
        Ok(
            if let Some((k_slice, v_slice)) = self.bounded_scan_first(&lower, &upper).pair()? {
                if StoreOp::try_from(v_slice[0])?.is_assert() {
                    let (_, eid, _) = decode_ae_key(k_slice)?;
                    let ret = Some(eid);
                    self.eid_by_attr_val_cache
                        .entry(v.to_static())
                        .or_default()
                        .insert((attr.id, vld), ret);
                    ret
                } else {
                    self.eid_by_attr_val_cache
                        .entry(v.to_static())
                        .or_default()
                        .insert((attr.id, vld), None);
                    None
                }
            } else {
                None
            },
        )
    }
    pub(crate) fn restore_bottom_value(
        &mut self,
        eid: EntityId,
        aid: AttrId,
        vld: Validity,
    ) -> Result<StaticValue> {
        let encoded = encode_eav_key(eid, aid, &Value::Bottom, vld);
        let res = self
            .tx
            .get(&encoded, false)?
            .ok_or(TripleError::TripleEANotFound(eid, aid, vld))?;
        Ok(decode_value(&res.as_ref()[1..])?.to_static())
    }
    pub(crate) fn triple_ea_scan(
        &mut self,
        eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue, Validity, StoreOp)>> {
        let lower = encode_eav_key(eid, aid, &Value::Null, Validity::MAX);
        let upper = encode_eav_key(eid, aid, &Value::Bottom, Validity::MIN);
        TripleEntityAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_ea_before_scan(
        &mut self,
        eid: EntityId,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue)>> {
        let lower = encode_eav_key(eid, aid, &Value::Null, Validity::MAX);
        let upper = encode_eav_key(eid, aid, &Value::Bottom, Validity::MIN);
        TripleEntityAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_ae_scan(
        &mut self,
        aid: AttrId,
        eid: EntityId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue, Validity, StoreOp)>> {
        let lower = encode_aev_key(aid, eid, &Value::Null, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &Value::Bottom, Validity::MIN);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_ae_before_scan(
        &mut self,
        aid: AttrId,
        eid: EntityId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue)>> {
        let lower = encode_aev_key(aid, eid, &Value::Null, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &Value::Bottom, Validity::MIN);
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_av_scan(
        &mut self,
        aid: AttrId,
        v: &Value,
    ) -> impl Iterator<Item = Result<(AttrId, StaticValue, EntityId, Validity, StoreOp)>> {
        let lower = encode_ave_key(aid, v, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_av_before_scan(
        &mut self,
        aid: AttrId,
        v: &Value,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, StaticValue, EntityId)>> {
        let lower = encode_ave_key(aid, v, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_av_after_scan(
        &mut self,
        aid: AttrId,
        v: &Value,
        after: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, StaticValue, EntityId)>> {
        let lower = encode_ave_key(aid, v, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueAfterIter::new(self.tx.iterator(), lower, upper, after)
    }
    pub(crate) fn triple_vref_a_scan(
        &mut self,
        v_eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId, Validity, StoreOp)>> {
        let lower = encode_vae_key(v_eid, aid, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_vae_key(v_eid, aid, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_vref_a_before_scan(
        &mut self,
        v_eid: EntityId,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId)>> {
        let lower = encode_vae_key(v_eid, aid, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_vae_key(v_eid, aid, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_e_scan(
        &mut self,
        eid: EntityId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue, Validity, StoreOp)>> {
        let lower = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, Validity::MIN);
        TripleEntityAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_e_before_scan(
        &mut self,
        eid: EntityId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue)>> {
        let lower = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, Validity::MIN);
        TripleEntityAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_a_scan(
        &mut self,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue, Validity, StoreOp)>> {
        let lower = encode_aev_key(aid, EntityId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &Value::Bottom, Validity::MIN);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_a_before_scan(
        &mut self,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue)>> {
        let lower = encode_aev_key(aid, EntityId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &Value::Bottom, Validity::MIN);
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_a_scan_all(
        &mut self,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue, Validity, StoreOp)>> {
        let lower = encode_aev_key(
            AttrId::MIN_PERM,
            EntityId::MIN_PERM,
            &Value::Null,
            Validity::MAX,
        );
        let upper = encode_aev_key(
            AttrId::MAX_PERM,
            EntityId::MAX_PERM,
            &Value::Bottom,
            Validity::MIN,
        );
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_a_before_scan_all(
        &mut self,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue)>> {
        let lower = encode_aev_key(
            AttrId::MIN_PERM,
            EntityId::MIN_PERM,
            &Value::Null,
            Validity::MAX,
        );
        let upper = encode_aev_key(
            AttrId::MAX_PERM,
            EntityId::MAX_PERM,
            &Value::Bottom,
            Validity::MIN,
        );
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_vref_scan(
        &mut self,
        v_eid: EntityId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId, Validity, StoreOp)>> {
        let lower = encode_vae_key(v_eid, AttrId::MIN_PERM, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_vae_key(v_eid, AttrId::MAX_PERM, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_vref_before_scan(
        &mut self,
        v_eid: EntityId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId)>> {
        let lower = encode_vae_key(v_eid, AttrId::MIN_PERM, EntityId::MIN_PERM, Validity::MAX);
        let upper = encode_vae_key(v_eid, AttrId::MAX_PERM, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
}

enum LatestTripleExistence {
    Asserted,
    Retracted,
    NotFound,
}

// normal version

struct TripleEntityAttrIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
}

impl TripleEntityAttrIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, StaticValue, Validity, StoreOp)>> {
        self.it.seek(&self.current);
        return match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (eid, aid, tid) = decode_ea_key(k_slice)?;
                let v = decode_value_from_key(k_slice)?;
                self.current.copy_from_slice(k_slice);
                self.current.encoded_entity_amend_validity_to_first();
                let op = StoreOp::try_from(v_slice[0])?;
                Ok(Some((eid, aid, v.to_static(), tid, op)))
            }
        };
    }
}

impl Iterator for TripleEntityAttrIter {
    type Item = Result<(EntityId, AttrId, StaticValue, Validity, StoreOp)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// before version

struct TripleEntityAttrBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    before: Validity,
}

impl TripleEntityAttrBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            before,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, StaticValue)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (eid, aid, tid) = decode_ea_key(k_slice)?;
                    if tid > self.before {
                        self.current.encoded_entity_amend_validity(self.before);
                        continue;
                    }
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((eid, aid, v.to_static())));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleEntityAttrBeforeIter {
    type Item = Result<(EntityId, AttrId, StaticValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleAttrEntityIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
}

impl TripleAttrEntityIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, StaticValue, Validity, StoreOp)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, eid, v.to_static(), tid, op)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrEntityIter {
    type Item = Result<(AttrId, EntityId, StaticValue, Validity, StoreOp)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// before version

struct TripleAttrEntityBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    before: Validity,
}

impl TripleAttrEntityBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            before,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, StaticValue)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    if tid > self.before {
                        self.current.encoded_entity_amend_validity(self.before);
                        continue;
                    }
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, eid, v.to_static())));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrEntityBeforeIter {
    type Item = Result<(AttrId, EntityId, StaticValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleAttrValueIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
}

impl TripleAttrValueIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, StaticValue, EntityId, Validity, StoreOp)>> {
        self.it.seek(&self.current);
        return match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (aid, eid, tid) = decode_ae_key(k_slice)?;
                let v = decode_value_from_key(k_slice)?;
                self.current.copy_from_slice(k_slice);
                self.current.encoded_entity_amend_validity_to_first();
                let op = StoreOp::try_from(v_slice[0])?;
                Ok(Some((aid, v.to_static(), eid, tid, op)))
            }
        };
    }
}

impl Iterator for TripleAttrValueIter {
    type Item = Result<(AttrId, StaticValue, EntityId, Validity, StoreOp)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// before version

struct TripleAttrValueBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    before: Validity,
}

impl TripleAttrValueBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            before,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, StaticValue, EntityId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    if tid > self.before {
                        self.current.encoded_entity_amend_validity(self.before);
                        continue;
                    }
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, v.to_static(), eid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueBeforeIter {
    type Item = Result<(AttrId, StaticValue, EntityId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// after version

struct TripleAttrValueAfterIter {
    it: DbIter,
    lower_bound: EncodedVec<LARGE_VEC_SIZE>,
    current: EncodedVec<LARGE_VEC_SIZE>,
    after: Validity,
}

impl TripleAttrValueAfterIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        after: Validity,
    ) -> Self {
        let it = builder.lower_bound(&lower_bound).start();
        Self {
            it,
            lower_bound,
            current: upper_bound,
            after,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, StaticValue, EntityId)>> {
        loop {
            self.it.seek_back(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    if compare_key(k_slice, &self.lower_bound) == std::cmp::Ordering::Less {
                        return Ok(None);
                    }
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    if tid < self.after {
                        self.current.encoded_entity_amend_validity(self.after);
                        continue;
                    }
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_last();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, v.to_static(), eid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueAfterIter {
    type Item = Result<(AttrId, StaticValue, EntityId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleValueRefAttrIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
}

impl TripleValueRefAttrIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, EntityId, Validity, StoreOp)>> {
        self.it.seek(&self.current);
        return match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (v_eid, aid, eid, tid) = decode_vae_key(k_slice)?;
                self.current.copy_from_slice(k_slice);
                self.current.encoded_entity_amend_validity_to_first();
                let op = StoreOp::try_from(v_slice[0])?;
                Ok(Some((v_eid, aid, eid, tid, op)))
            }
        };
    }
}

impl Iterator for TripleValueRefAttrIter {
    type Item = Result<(EntityId, AttrId, EntityId, Validity, StoreOp)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// Before version

struct TripleValueRefAttrBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    before: Validity,
}

impl TripleValueRefAttrBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            before,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, EntityId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (v_eid, aid, eid, tid) = decode_vae_key(k_slice)?;
                    if tid > self.before {
                        self.current.encoded_entity_amend_validity(self.before);
                        continue;
                    }
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((v_eid, aid, eid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleValueRefAttrBeforeIter {
    type Item = Result<(EntityId, AttrId, EntityId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
