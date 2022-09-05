use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use miette::{bail, ensure, miette, Result};
use smartstring::{LazyCompact, SmartString};

use cozorocks::{DbIter, IterBuilder};

use crate::data::attr::Attribute;
use crate::data::compare::compare_key;
use crate::data::encode::{
    decode_ae_key, decode_vae_key, decode_value, decode_value_from_key, decode_value_from_val,
    encode_aev_key, encode_ave_key, encode_ave_key_for_unique_v, encode_sentinel_attr_val,
    encode_sentinel_entity_attr, encode_vae_key, EncodedVec, LARGE_VEC_SIZE,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::triple::StoreOp;
use crate::data::value::DataValue;
use crate::parse::tx::{EntityRep, Quintuple, TxAction};
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

impl SessionTx {
    pub(crate) fn tx_triples(
        &mut self,
        mut payloads: Vec<Quintuple>,
    ) -> Result<Vec<(EntityId, isize)>> {
        let default_vld = Validity::current();
        let mut ret = Vec::with_capacity(payloads.len());
        let mut str_temp_to_perm_ids: BTreeMap<SmartString<LazyCompact>, EntityId> =
            BTreeMap::new();
        for payload in &mut payloads {
            if let EntityRep::UserTempId(symb) = &payload.entity {
                ensure!(
                    payload.action == TxAction::Put,
                    "temp id can only be used for put actions"
                );
                if !str_temp_to_perm_ids.contains_key(symb) {
                    let new_eid = EntityId(self.last_ent_id.fetch_add(1, Ordering::AcqRel) + 1);
                    str_temp_to_perm_ids.insert(symb.clone(), new_eid);
                }
            }
        }

        for payload in payloads {
            let vld = payload.validity.unwrap_or(default_vld);
            match payload.action {
                TxAction::Put => {
                    let attr = self
                        .attr_by_name(&payload.attr_name)?
                        .ok_or_else(|| miette!("attribute {} not found", payload.attr_name))?;
                    let val =
                        attr.coerce_value(payload.value, &mut str_temp_to_perm_ids, self, vld)?;
                    match payload.entity {
                        EntityRep::Id(perm) => {
                            ret.push((
                                self.amend_triple(
                                    perm,
                                    &attr,
                                    &val,
                                    payload.validity.unwrap_or(vld),
                                )?,
                                1,
                            ));
                        }

                        EntityRep::UserTempId(tempid) => {
                            let eid = *str_temp_to_perm_ids.get(&tempid).unwrap();

                            ret.push((self.new_triple(eid, &attr, &val, vld)?, 1));
                        }
                        EntityRep::PullByKey(symb, val) => {
                            let attr = self
                                .attr_by_name(&symb)?
                                .ok_or_else(|| miette!("required attribute {} not found", symb))?;

                            let eid =
                                self.eid_by_unique_av(&attr, &val, vld)?.ok_or_else(|| {
                                    miette!("requried entity not found: {}: {:?}", symb, val)
                                })?;

                            ret.push((self.new_triple(eid, &attr, &val, vld)?, 1));
                        }
                    }
                }
                TxAction::Retract => {
                    let attr = self.attr_by_name(&payload.attr_name)?.unwrap();
                    let eid = match payload.entity {
                        EntityRep::Id(id) => id,
                        EntityRep::UserTempId(_) => {
                            bail!("cannot retract with temp id")
                        }
                        EntityRep::PullByKey(symb, val) => {
                            let vld = payload.validity.unwrap_or(default_vld);
                            let attr = self
                                .attr_by_name(&symb)?
                                .ok_or_else(|| miette!("required attribute {} not found", symb))?;

                            let eid =
                                self.eid_by_unique_av(&attr, &val, vld)?.ok_or_else(|| {
                                    miette!("requried entity not found: {}: {:?}", symb, val)
                                })?;
                            eid
                        }
                    };
                    ret.push((
                        self.retract_triple(
                            eid,
                            &attr,
                            &payload.value,
                            payload.validity.unwrap_or(default_vld),
                        )?,
                        -1,
                    ));
                }
            }
        }
        Ok(ret)
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

        // vae for ref types
        if attr.val_type.is_ref_type() {
            let vae_encoded = encode_vae_key(v.get_entity_id()?, attr.id, eid, vld_in_key);
            if real_delete {
                self.tx.del(&vae_encoded)?;
            } else {
                self.tx
                    .put(
                        &vae_encoded,
                        &DataValue::Guard.encode_with_op_and_tx(op, tx_id),
                    )
                    ?;
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
            let e_in_val_encoded = eid.as_datavalue().encode_with_op_and_tx(op, tx_id);
            if real_delete {
                self.tx.del(&ave_encoded)?;
            } else {
                self.tx
                    .put(&ave_encoded, &e_in_val_encoded)
                    ?;
            }

            self.tx
                .put(
                    &encode_sentinel_attr_val(attr.id, v),
                    &tx_id.bytes_with_op(op),
                )
                ?;
        }

        self.tx
            .put(
                &encode_sentinel_entity_attr(eid, attr.id),
                &tx_id.bytes_with_op(op),
            )
            ?;

        Ok(eid)
    }

    pub(crate) fn new_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &DataValue,
        vld: Validity,
    ) -> Result<EntityId> {
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
        Ok(eid)
    }
    pub(crate) fn eid_by_unique_av(
        &self,
        attr: &Attribute,
        v: &DataValue,
        vld: Validity,
    ) -> Result<Option<EntityId>> {
        if let Some(inner) = self.eid_by_attr_val_cache.borrow_mut().get(v) {
            if let Some(found) = inner.get(&(attr.id, vld)) {
                return Ok(*found);
            }
        }

        let lower = encode_ave_key_for_unique_v(attr.id, v, vld);
        let upper = encode_ave_key_for_unique_v(attr.id, v, Validity::MIN);
        Ok(
            if let Some(v_slice) = self
                .bounded_scan_first(&lower, &upper)
                .val()
                ?
            {
                if StoreOp::try_from(v_slice[0])?.is_assert() {
                    let eid = decode_value(&v_slice[8..])?.get_entity_id()?;
                    let ret = Some(eid);
                    self.eid_by_attr_val_cache
                        .borrow_mut()
                        .entry(v.clone())
                        .or_default()
                        .insert((attr.id, vld), ret);
                    ret
                } else {
                    self.eid_by_attr_val_cache
                        .borrow_mut()
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
    pub(crate) fn triple_ae_scan(
        &self,
        aid: AttrId,
        eid: EntityId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, eid, &DataValue::Null, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_ae_range_scan(
        &self,
        aid: AttrId,
        eid: EntityId,
        v_lower: DataValue,
        v_upper: DataValue,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, eid, &v_lower, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityRangeIter::new(self.tx.iterator(), lower, upper, v_upper)
    }
    pub(crate) fn triple_ae_before_scan(
        &self,
        aid: AttrId,
        eid: EntityId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, eid, &DataValue::Null, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
    }
    pub(crate) fn triple_ae_range_before_scan(
        &self,
        aid: AttrId,
        eid: EntityId,
        v_lower: DataValue,
        v_upper: DataValue,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, eid, &v_lower, Validity::MAX);
        let upper = encode_aev_key(aid, eid, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityRangeBeforeIter::new(self.tx.iterator(), lower, upper, v_upper, before)
    }
    pub(crate) fn aev_exists(
        &self,
        aid: AttrId,
        eid: EntityId,
        val: &DataValue,
        vld: Validity,
    ) -> Result<bool> {
        for result in self.triple_ae_before_scan(aid, eid, vld) {
            let (_, _, found_val) = result?;
            if found_val == *val {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn triple_av_range_scan(
        &self,
        aid: AttrId,
        lower: &DataValue,
        upper_inc: &DataValue,
    ) -> impl Iterator<Item = Result<(AttrId, DataValue, EntityId)>> {
        let lower = encode_ave_key(aid, lower, EntityId::ZERO, Validity::MAX);
        let upper = encode_ave_key(aid, &DataValue::Bot, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueRangeIter::new(self.tx.iterator(), lower, upper, upper_inc.clone())
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
    pub(crate) fn triple_av_range_before_scan(
        &self,
        aid: AttrId,
        lower: &DataValue,
        upper_inc: &DataValue,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, DataValue, EntityId)>> {
        let lower = encode_ave_key(aid, lower, EntityId::ZERO, Validity::MAX);
        let upper = encode_ave_key(aid, &DataValue::Bot, EntityId::MAX_PERM, Validity::MIN);
        TripleAttrValueRangeBeforeIter::new(
            self.tx.iterator(),
            lower,
            upper,
            upper_inc.clone(),
            before,
        )
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
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_a_before_scan(
        &self,
        aid: AttrId,
        before: Validity,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, DataValue)>> {
        let lower = encode_aev_key(aid, EntityId::ZERO, &DataValue::Null, Validity::MAX);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &DataValue::Bot, Validity::MIN);
        TripleAttrEntityBeforeIter::new(self.tx.iterator(), lower, upper, before)
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
        match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (aid, eid, _tid) = decode_ae_key(k_slice)?;
                let mut v = decode_value_from_key(k_slice)?;
                if v == DataValue::Guard {
                    v = decode_value_from_val(v_slice)?;
                }
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

struct TripleAttrEntityRangeIter {
    it: DbIter,
    started: bool,
    inc_upper: DataValue,
}

impl TripleAttrEntityRangeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        inc_upper: DataValue,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self {
            it,
            started: false,
            inc_upper,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, DataValue)>> {
        if self.started {
            self.it.next()
        } else {
            self.started = true;
        }
        match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (aid, eid, _tid) = decode_ae_key(k_slice)?;
                let mut v = decode_value_from_key(k_slice)?;
                if v == DataValue::Guard {
                    v = decode_value_from_val(v_slice)?;
                }
                if v > self.inc_upper {
                    return Ok(None);
                }
                Ok(Some((aid, eid, v)))
            }
        }
    }
}

impl Iterator for TripleAttrEntityRangeIter {
    type Item = Result<(AttrId, EntityId, DataValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// before version

struct TripleAttrEntityRangeBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    before: Validity,
    inc_upper: DataValue,
}

impl TripleAttrEntityRangeBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        inc_upper: DataValue,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            before,
            inc_upper,
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
                    if v > self.inc_upper {
                        return Ok(None);
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

impl Iterator for TripleAttrEntityRangeBeforeIter {
    type Item = Result<(AttrId, EntityId, DataValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// normal version

struct TripleAttrValueRangeIter {
    it: DbIter,
    started: bool,
    inc_upper: DataValue,
}

impl TripleAttrValueRangeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        inc_upper: DataValue,
    ) -> Self {
        let mut it = builder.upper_bound(&upper_bound).start();
        it.seek(&lower_bound);
        Self {
            it,
            started: false,
            inc_upper,
        }
    }
    fn next_inner(&mut self) -> Result<Option<(AttrId, DataValue, EntityId)>> {
        if self.started {
            self.it.next();
        } else {
            self.started = true;
        }
        match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (aid, mut eid, _tid) = decode_ae_key(k_slice)?;
                if eid.is_placeholder() {
                    eid = decode_value_from_val(v_slice)?.get_entity_id()?;
                }
                let v = decode_value_from_key(k_slice)?;
                if v > self.inc_upper {
                    Ok(None)
                } else {
                    Ok(Some((aid, v, eid)))
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueRangeIter {
    type Item = Result<(AttrId, DataValue, EntityId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

// before version

struct TripleAttrValueRangeBeforeIter {
    it: DbIter,
    current: EncodedVec<LARGE_VEC_SIZE>,
    inc_upper: DataValue,
    before: Validity,
}

impl TripleAttrValueRangeBeforeIter {
    fn new(
        builder: IterBuilder,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
        inc_upper: DataValue,
        before: Validity,
    ) -> Self {
        let it = builder.upper_bound(&upper_bound).start();
        Self {
            it,
            current: lower_bound,
            inc_upper,
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
                    if v > self.inc_upper {
                        return Ok(None);
                    }
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

impl Iterator for TripleAttrValueRangeBeforeIter {
    type Item = Result<(AttrId, DataValue, EntityId)>;

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
        match self.it.pair()? {
            None => Ok(None),
            Some((k_slice, v_slice)) => {
                let (aid, mut eid, _tid) = decode_ae_key(k_slice)?;
                if eid.is_placeholder() {
                    eid = decode_value_from_val(v_slice)?.get_entity_id()?;
                }
                let v = decode_value_from_key(k_slice)?;
                Ok(Some((aid, v, eid)))
            }
        }
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
        match self.it.key()? {
            None => Ok(None),
            Some(k_slice) => {
                let (v_eid, aid, eid, _) = decode_vae_key(k_slice)?;
                Ok(Some((v_eid, aid, eid)))
            }
        }
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
