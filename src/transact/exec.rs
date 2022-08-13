use std::sync::atomic::Ordering;

use anyhow::{anyhow, ensure, Result};

use cozorocks::{DbIter, IterBuilder};

use crate::data::attr::{Attribute, AttributeTyping};
use crate::data::compare::compare_key;
use crate::data::encode::{
    decode_ae_key, decode_ea_key, decode_vae_key, decode_value, decode_value_from_key,
    decode_value_from_val, encode_aev_key, encode_ave_key, encode_ave_key_for_unique_v,
    encode_eav_key, encode_sentinel_attr_val, encode_sentinel_entity_attr, encode_vae_key,
    EncodedVec, LARGE_VEC_SIZE,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::triple::StoreOp;
use crate::data::value::{DataValue, INLINE_VAL_SIZE_LIMIT};
use crate::parse::triple::{Quintuple, TxAction};
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

impl SessionTx {
    pub(crate) fn tx_triples(
        &mut self,
        payloads: Vec<Quintuple>,
    ) -> Result<Vec<(EntityId, isize)>> {
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
        v: &DataValue,
        vld: Validity,
    ) -> Result<()> {
        let aid = attr.id;
        let sentinel = encode_sentinel_entity_attr(eid, aid);
        let gen_err = || anyhow!("required triple not found for {:?}, {:?}", eid, aid);
        self.tx.get(&sentinel, true)?.ok_or_else(gen_err)?;
        let v_in_key = if attr.cardinality.is_one() {
            &DataValue::Guard
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
        ensure!(
            stored_v == *v,
            "precondition check failed: wanted {:?}, got {:?}",
            v,
            stored_v
        );
        Ok(())
    }
    pub(crate) fn write_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &DataValue,
        vld: Validity,
        op: StoreOp,
    ) -> Result<EntityId> {
        let tx_id = self.get_write_tx_id()?;
        let vld_in_key = if attr.with_history {
            vld
        } else {
            Validity::NO_HISTORY
        };
        let real_delete = op.is_retract() && !attr.with_history;
        // elide value in key for eav and aev if cardinality is one
        let (v_in_key, v_in_val) = if attr.cardinality.is_one() {
            (
                &DataValue::Guard,
                if op.is_assert() { v } else { &DataValue::Guard },
            )
        } else {
            (v, &DataValue::Guard)
        };
        let val_encoded = v_in_val.encode_with_op_and_tx(op, tx_id);

        let aev_encoded = encode_aev_key(attr.id, eid, v_in_key, vld_in_key);
        if real_delete {
            self.tx.del(&aev_encoded)?;
        } else {
            self.tx.put(&aev_encoded, &val_encoded)?;
        }
        // elide value in data for aev if it is big
        let val_encoded = if val_encoded.len() > INLINE_VAL_SIZE_LIMIT {
            DataValue::Guard.encode_with_op_and_tx(op, tx_id)
        } else {
            val_encoded
        };

        let eav_encoded = encode_eav_key(eid, attr.id, v_in_key, vld_in_key);
        if real_delete {
            self.tx.del(&eav_encoded)?;
        } else {
            self.tx.put(&eav_encoded, &val_encoded)?;
        }

        // vae for ref types
        if attr.val_type.is_ref_type() {
            let vae_encoded = encode_vae_key(v.get_entity_id()?, attr.id, eid, vld_in_key);
            if real_delete {
                self.tx.del(&vae_encoded)?;
            } else {
                self.tx.put(
                    &vae_encoded,
                    &DataValue::Guard.encode_with_op_and_tx(op, tx_id),
                )?;
            }
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
                let vld_in_key = if attr.with_history {
                    vld
                } else {
                    Validity::NO_HISTORY
                };
                if attr.with_history {
                    // back scan in time
                    for item in self.triple_av_before_scan(attr.id, v, vld_in_key) {
                        let (_, _, found_eid) = item?;
                        ensure!(
                            found_eid == eid,
                            "unique constraint violated for attr {} with value {:?}",
                            attr.name,
                            v
                        );
                    }
                    // fwd scan in time
                    for item in self.triple_av_after_scan(attr.id, v, vld_in_key) {
                        let (_, _, found_eid) = item?;
                        ensure!(
                            found_eid == eid,
                            "unique constraint violated for attr {} with value {:?}",
                            attr.name,
                            v
                        );
                    }
                } else if let Some(v_slice) = self.tx.get(&ave_encoded, false)? {
                    let found_eid = decode_value_from_val(&v_slice)?.get_entity_id()?;
                    ensure!(
                        found_eid == eid,
                        "unique constraint violated for attr {} with value {:?}",
                        attr.name,
                        v
                    );
                }
            }
            let e_in_val_encoded = eid.to_value().encode_with_op_and_tx(op, tx_id);
            if real_delete {
                self.tx.del(&ave_encoded)?;
            } else {
                self.tx.put(&ave_encoded, &e_in_val_encoded)?;
            }

            self.tx.put(
                &encode_sentinel_attr_val(attr.id, v),
                &tx_id.bytes_with_op(op),
            )?;
        }

        self.tx.put(
            &encode_sentinel_entity_attr(eid, attr.id),
            &tx_id.bytes_with_op(op),
        )?;

        Ok(eid)
    }

    pub(crate) fn new_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &DataValue,
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
                let new_v = perm_v_eid.to_value();
                return self.write_triple(eid, attr, &new_v, vld, StoreOp::Assert);
            }
        }
        self.write_triple(eid, attr, v, vld, StoreOp::Assert)
    }

    pub(crate) fn amend_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &DataValue,
        vld: Validity,
    ) -> Result<EntityId> {
        ensure!(eid.is_perm(), "temp id not allowed here: {:?}", eid);
        // checking that the eid actually exists should be done in the preprocessing step
        self.write_triple(eid, attr, v, vld, StoreOp::Retract)
    }

    pub(crate) fn retract_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &DataValue,
        vld: Validity,
    ) -> Result<EntityId> {
        self.write_triple(eid, attr, v, vld, StoreOp::Retract)?;
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
        let lower_bound = encode_eav_key(eid, attr.id, &DataValue::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, attr.id, &DataValue::Bottom, Validity::MIN);
        self.batch_retract_triple(lower_bound, upper_bound, vld)
    }
    pub(crate) fn retract_entity(&mut self, eid: EntityId, vld: Validity) -> Result<isize> {
        let lower_bound = encode_eav_key(eid, AttrId::MIN_PERM, &DataValue::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &DataValue::Bottom, Validity::MAX);
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
                            .ok_or_else(|| anyhow!("attribute not found for {:?}", cur_aid))?;
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
        v: &DataValue,
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
            if let Some(v_slice) = self.bounded_scan_first(&lower, &upper).val()? {
                if StoreOp::try_from(v_slice[0])?.is_assert() {
                    // let (_, mut eid, _) = decode_ae_key(k_slice)?;
                    // if eid.is_zero() {
                    let eid = decode_value(&v_slice[8..])?.get_entity_id()?;
                    // }
                    let ret = Some(eid);
                    self.eid_by_attr_val_cache
                        .entry(v.clone())
                        .or_default()
                        .insert((attr.id, vld), ret);
                    ret
                } else {
                    self.eid_by_attr_val_cache
                        .entry(v.clone())
                        .or_default()
                        .insert((attr.id, vld), None);
                    None
                }
            } else {
                None
            },
        )
    }
    pub(crate) fn triple_ea_scan(
        &self,
        eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, DataValue)>> {
        let lower = encode_eav_key(eid, aid, &DataValue::Null, Validity::MAX);
        let upper = encode_eav_key(eid, aid, &DataValue::Bottom, Validity::MIN);
        println!("zz");
        for x in TripleEntityAttrIter::new(self.tx.iterator(), lower.clone(), upper.clone()) {
            dbg!(x.unwrap());
        }
        for y in TripleEntityAttrBeforeIter::new(
            self.tx.iterator(),
            lower.clone(),
            upper.clone(),
            Validity::current(),
        ) {
            dbg!(y.unwrap());
        }

        TripleEntityAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_ea_before_scan(
        &self,
        eid: EntityId,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, DataValue)>> {
        let lower = encode_eav_key(eid, aid, &DataValue::Null, Validity::MAX);
        let upper = encode_eav_key(eid, aid, &DataValue::Bottom, Validity::MIN);
        TripleEntityAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn eav_exists(
        &self,
        eid: EntityId,
        aid: AttrId,
        val: &DataValue,
        vld: Validity,
    ) -> Result<bool> {
        for result in self.triple_ea_before_scan(eid, aid, vld) {
            let (_, _, found_val) = result?;
            if found_val == *val {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn triple_av_scan(
        &self,
        aid: AttrId,
        v: &DataValue,
    ) -> impl Iterator<Item = Result<(AttrId, DataValue, EntityId)>> {
        let lower = encode_ave_key(aid, v, EntityId::ZERO, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_av_before_scan(
        &self,
        aid: AttrId,
        v: &DataValue,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, DataValue, EntityId)>> {
        let lower = encode_ave_key(aid, v, EntityId::ZERO, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_av_after_scan(
        &self,
        aid: AttrId,
        v: &DataValue,
        after: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, DataValue, EntityId)>> {
        let lower = encode_ave_key(aid, v, EntityId::ZERO, Validity::MAX);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueAfterIter::new(self.tx.iterator(), lower, upper, after)
    }
    pub(crate) fn triple_vref_a_scan(
        &self,
        v_eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId)>> {
        let lower = encode_vae_key(v_eid, aid, EntityId::ZERO, Validity::MAX);
        let upper = encode_vae_key(v_eid, aid, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_vref_a_before_scan(
        &self,
        v_eid: EntityId,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId)>> {
        let lower = encode_vae_key(v_eid, aid, EntityId::ZERO, Validity::MAX);
        let upper = encode_vae_key(v_eid, aid, EntityId::MAX_PERM, Validity::MIN);
        TripleValueRefAttrBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_a_scan(
        &self,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, EntityId::ZERO, &DataValue::Null, Validity::MAX);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &DataValue::Bottom, Validity::MIN);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_a_before_scan(
        &self,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, EntityId::ZERO, &DataValue::Null, Validity::MAX);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &DataValue::Bottom, Validity::MIN);
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
}

// normal version

struct TripleEntityAttrIter {
    it: DbIter,
    started: bool,
}

impl TripleEntityAttrIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self { it, started: false }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, DataValue)>> {
        if !self.started {
            self.started = true;
        } else {
            self.it.next();
        }
        return match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (eid, aid, _tid) = decode_ea_key(k_slice)?;
                let mut v = decode_value_from_key(k_slice)?;
                if v == DataValue::Guard {
                    v = decode_value_from_val(v_slice)?;
                }
                Ok(Some((eid, aid, v)))
            }
        };
    }
}

impl Iterator for TripleEntityAttrIter {
    type Item = Result<(EntityId, AttrId, DataValue)>;

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
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, DataValue)>> {
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
                    let mut v = decode_value_from_key(k_slice)?;
                    if v == DataValue::Guard {
                        v = decode_value_from_val(v_slice)?;
                    }
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_inf_past();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((eid, aid, v)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleEntityAttrBeforeIter {
    type Item = Result<(EntityId, AttrId, DataValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleAttrEntityIter {
    it: DbIter,
    started: bool,
}

impl TripleAttrEntityIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self { it, started: false }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, DataValue)>> {
        if self.started {
            self.it.next()
        } else {
            self.started = true;
        }
        match self.it.key()? {
            None => Ok(None),
            Some(k_slice) => {
                let (aid, eid, _tid) = decode_ae_key(k_slice)?;
                let v = decode_value_from_key(k_slice)?;
                Ok(Some((aid, eid, v)))
            }
        }
    }
}

impl Iterator for TripleAttrEntityIter {
    type Item = Result<(AttrId, EntityId, DataValue)>;

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
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, DataValue)>> {
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
                    let mut v = decode_value_from_key(k_slice)?;
                    if v == DataValue::Guard {
                        v = decode_value_from_val(v_slice)?;
                    }
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_inf_past();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, eid, v)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrEntityBeforeIter {
    type Item = Result<(AttrId, EntityId, DataValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleAttrValueIter {
    it: DbIter,
    started: bool,
}

impl TripleAttrValueIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self { it, started: false }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, DataValue, EntityId)>> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        return match self.it.key()? {
            None => Ok(None),
            Some(k_slice) => {
                let (aid, eid, _tid) = decode_ae_key(k_slice)?;
                let v = decode_value_from_key(k_slice)?;
                Ok(Some((aid, v, eid)))
            }
        };
    }
}

impl Iterator for TripleAttrValueIter {
    type Item = Result<(AttrId, DataValue, EntityId)>;

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
    fn next_inner(&mut self) -> Result<Option<(AttrId, DataValue, EntityId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, mut eid, tid) = decode_ae_key(k_slice)?;
                    if eid.is_placeholder() {
                        eid = decode_value_from_val(v_slice)?.get_entity_id()?;
                    }
                    if tid > self.before {
                        self.current.encoded_entity_amend_validity(self.before);
                        continue;
                    }
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_validity_to_inf_past();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, v, eid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueBeforeIter {
    type Item = Result<(AttrId, DataValue, EntityId)>;

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
    fn next_inner(&mut self) -> Result<Option<(AttrId, DataValue, EntityId)>> {
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
                    self.current.encoded_entity_amend_validity_to_inf_future();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, v, eid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueAfterIter {
    type Item = Result<(AttrId, DataValue, EntityId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleValueRefAttrIter {
    it: DbIter,
    started: bool,
}

impl TripleValueRefAttrIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self { it, started: false }
    }
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, EntityId)>> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        return match self.it.key()? {
            None => Ok(None),
            Some(k_slice) => {
                let (v_eid, aid, eid, _) = decode_vae_key(k_slice)?;
                Ok(Some((v_eid, aid, eid)))
            }
        };
    }
}

impl Iterator for TripleValueRefAttrIter {
    type Item = Result<(EntityId, AttrId, EntityId)>;

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
                    self.current.encoded_entity_amend_validity_to_inf_past();
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
