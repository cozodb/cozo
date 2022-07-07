use crate::data::attr::{Attribute, AttributeTyping};
use crate::data::encode::{
    decode_ae_key, decode_ea_key, decode_vae_key, decode_value, decode_value_from_key,
    encode_aev_key, encode_ave_key, encode_ave_key_for_unique_v, encode_eav_key,
    encode_unique_attr_val, encode_unique_entity, encode_vae_key, EncodedVec, LARGE_VEC_SIZE,
};
use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
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
    TripleEANotFound(EntityId, AttrId, TxId),
}

impl SessionTx {
    pub(crate) fn put_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
        op: StoreOp,
    ) -> Result<()> {
        let tx_id = self.get_write_tx_id()?;
        let tx_id_in_key = if attr.with_history {
            tx_id
        } else {
            TxId::NO_HISTORY
        };
        // elide value in key for eav and aev if cardinality is one
        let (v_in_key, v_in_val) = if attr.cardinality.is_one() {
            (&Value::Bottom, v)
        } else {
            (v, &Value::Bottom)
        };
        let eav_encoded = encode_eav_key(eid, attr.id, v_in_key, tx_id_in_key);
        let val_encoded = v_in_val.encode_with_op(op);
        self.tx.put(&eav_encoded, &val_encoded)?;

        // elide value in data for aev if it is big
        let val_encoded = if val_encoded.len() > INLINE_VAL_SIZE_LIMIT {
            Value::Bottom.encode()
        } else {
            val_encoded
        };

        let aev_encoded = encode_aev_key(attr.id, eid, v_in_key, tx_id_in_key);
        self.tx.put(&aev_encoded, &val_encoded)?;

        // vae for ref types
        if attr.val_type.is_ref_type() {
            let vae_encoded = encode_vae_key(v.get_entity_id()?, attr.id, eid, tx_id_in_key);
            self.tx.put(&vae_encoded, &[op as u8])?;
        }

        // ave for indexing
        if attr.indexing.should_index() {
            // elide e for unique index
            let e_in_key = if attr.indexing.is_unique_index() {
                EntityId(0)
            } else {
                eid
            };
            let ave_encoded = encode_ave_key(attr.id, v, e_in_key, tx_id_in_key);
            // checking of unique constraints
            if attr.indexing.is_unique_index() {
                let starting = if attr.with_history {
                    ave_encoded.clone()
                } else {
                    encode_ave_key(attr.id, v, e_in_key, tx_id)
                };
                let ave_encoded_bound = encode_ave_key(attr.id, v, e_in_key, TxId::ZERO);
                if let Some((k_slice, v_slice)) = self
                    .bounded_scan_first(&starting, &ave_encoded_bound)
                    .pair()?
                {
                    let (_, _, _) = decode_ae_key(k_slice)?;
                    let found_op = StoreOp::try_from(v_slice[0])?;
                    if found_op.is_assert() {
                        let existing_eid = EntityId::from_bytes(&v_slice[1..]);
                        if existing_eid != eid {
                            return Err(TripleError::UniqueConstraintViolated(
                                attr.keyword.clone(),
                                format!("{:?}", v),
                            )
                            .into());
                        }
                    }
                }
            }
            let e_in_val_encoded = eid.bytes();
            self.tx.put(&ave_encoded, &e_in_val_encoded)?;

            self.tx.put(
                &encode_unique_attr_val(attr.id, v),
                &tx_id.bytes_with_op(op),
            )?;
        }

        self.tx
            .put(&encode_unique_entity(eid), &tx_id.bytes_with_op(op))?;

        Ok(())
    }

    pub(crate) fn new_triple(&mut self, eid: EntityId, attr: &Attribute, v: &Value) -> Result<()> {
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
        self.put_triple(eid, attr, v, StoreOp::Assert)
    }

    pub(crate) fn amend_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
    ) -> Result<()> {
        if !eid.is_perm() {
            return Err(TripleError::TempEid(eid).into());
        }
        // checking that the eid actually exists should be done in the preprocessing step
        self.put_triple(eid, attr, v, StoreOp::Retract)
    }

    pub(crate) fn retract_triple(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
        v: &Value,
    ) -> Result<()> {
        self.put_triple(eid, attr, v, StoreOp::Retract)?;
        if attr.val_type == AttributeTyping::Component {
            let eid_v = v.get_entity_id()?;
            self.retract_entity(eid_v)?;
        }
        Ok(())
    }
    pub(crate) fn retract_triples_for_attr(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
    ) -> Result<()> {
        let lower_bound = encode_eav_key(eid, attr.id, &Value::Null, TxId::MAX_USER);
        let upper_bound = encode_eav_key(eid, attr.id, &Value::Bottom, TxId::ZERO);
        self.batch_retract_triple(lower_bound, upper_bound)
    }
    pub(crate) fn retract_entity(&mut self, eid: EntityId) -> Result<()> {
        match self.latest_entity_existence(eid, true)? {
            LatestTripleExistence::Asserted => {}
            LatestTripleExistence::Retracted => return Ok(()),
            LatestTripleExistence::NotFound => return Err(TripleError::EidNotFound(eid).into()),
        }
        let lower_bound = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, TxId::MAX_USER);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, TxId::ZERO);
        self.batch_retract_triple(lower_bound, upper_bound)
    }
    fn batch_retract_triple(
        &mut self,
        lower_bound: EncodedVec<LARGE_VEC_SIZE>,
        upper_bound: EncodedVec<LARGE_VEC_SIZE>,
    ) -> Result<()> {
        let mut it = self.bounded_scan(&lower_bound, &upper_bound);
        let mut current = lower_bound.clone();
        loop {
            it.seek(&current);
            match it.pair()? {
                None => return Ok(()),
                Some((k_slice, v_slice)) => {
                    let op = StoreOp::try_from(v_slice[0])?;
                    let (cur_eid, cur_aid, _) = decode_ea_key(k_slice)?;
                    let cur_v = decode_value_from_key(k_slice)?;
                    if op.is_assert() {
                        let cur_attr = self
                            .attr_by_id(cur_aid)?
                            .ok_or(TransactError::AttrNotFound(cur_aid))?;
                        self.retract_triple(cur_eid, &cur_attr, &cur_v)?;
                    }
                    current = encode_eav_key(cur_eid, cur_aid, &cur_v, TxId::ZERO);
                }
            }
        }
    }
    fn latest_entity_existence(
        &mut self,
        eid: EntityId,
        for_update: bool,
    ) -> Result<LatestTripleExistence> {
        let encoded = encode_unique_entity(eid);
        Ok(if let Some(v_slice) = self.tx.get(&encoded, for_update)? {
            let op = StoreOp::try_from(v_slice[0])?;
            match op {
                StoreOp::Retract => LatestTripleExistence::Retracted,
                StoreOp::Assert => LatestTripleExistence::Asserted,
            }
        } else {
            LatestTripleExistence::NotFound
        })
    }
    pub(crate) fn eid_by_unique_av(
        &mut self,
        attr: &Attribute,
        v: &Value,
    ) -> Result<Option<EntityId>> {
        if let Some(inner) = self.eid_by_attr_val_cache.get(v) {
            if let Some(found) = inner.get(&attr.id) {
                return Ok(*found);
            }
        }

        let lower = encode_ave_key_for_unique_v(attr.id, v, self.r_tx_id);
        let upper = encode_ave_key_for_unique_v(attr.id, v, TxId::ZERO);
        Ok(
            if let Some((k_slice, v_slice)) = self.bounded_scan_first(&lower, &upper).pair()? {
                if StoreOp::try_from(v_slice[0])?.is_assert() {
                    let (_, eid, _) = decode_ae_key(k_slice)?;
                    let ret = Some(eid);
                    self.eid_by_attr_val_cache
                        .entry(v.to_static())
                        .or_default()
                        .insert(attr.id, ret);
                    ret
                } else {
                    self.eid_by_attr_val_cache
                        .entry(v.to_static())
                        .or_default()
                        .insert(attr.id, None);
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
        txid: TxId,
    ) -> Result<StaticValue> {
        let encoded = encode_eav_key(eid, aid, &Value::Bottom, txid);
        let res = self
            .tx
            .get(&encoded, false)?
            .ok_or(TripleError::TripleEANotFound(eid, aid, txid))?;
        Ok(decode_value(&res.as_ref()[1..])?.to_static())
    }
    pub(crate) fn triple_ea_scan(
        &mut self,
        eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue, TxId)>> {
        let lower = encode_eav_key(eid, aid, &Value::Null, TxId::MAX_USER);
        let upper = encode_eav_key(eid, aid, &Value::Bottom, TxId::ZERO);
        TripleEntityAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_ae_scan(
        &mut self,
        aid: AttrId,
        eid: EntityId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue, TxId)>> {
        let lower = encode_aev_key(aid, eid, &Value::Null, TxId::MAX_USER);
        let upper = encode_aev_key(aid, eid, &Value::Bottom, TxId::ZERO);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_av_scan(
        &mut self,
        aid: AttrId,
        v: &Value,
    ) -> impl Iterator<Item = Result<(AttrId, StaticValue, EntityId, TxId)>> {
        let lower = encode_ave_key(aid, v, EntityId::MIN_PERM, TxId::MAX_USER);
        let upper = encode_ave_key(aid, v, EntityId::MAX_PERM, TxId::ZERO);
        TripleAttrValueIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_vref_a_scan(
        &mut self,
        v_eid: EntityId,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId, TxId)>> {
        let lower = encode_vae_key(v_eid, aid, EntityId::MIN_PERM, TxId::MAX_USER);
        let upper = encode_vae_key(v_eid, aid, EntityId::MAX_PERM, TxId::ZERO);
        TripleValueRefAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_e_scan(
        &mut self,
        eid: EntityId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, StaticValue, TxId)>> {
        let lower = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, TxId::MAX_USER);
        let upper = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, TxId::ZERO);
        TripleEntityAttrIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_a_scan(
        &mut self,
        aid: AttrId,
    ) -> impl Iterator<Item = Result<(AttrId, EntityId, StaticValue, TxId)>> {
        let lower = encode_aev_key(aid, EntityId::MIN_PERM, &Value::Null, TxId::MAX_USER);
        let upper = encode_aev_key(aid, EntityId::MAX_PERM, &Value::Bottom, TxId::ZERO);
        TripleAttrEntityIter::new(self.tx.iterator(), lower, upper)
    }
    pub(crate) fn triple_vref_scan(
        &mut self,
        v_eid: EntityId,
    ) -> impl Iterator<Item = Result<(EntityId, AttrId, EntityId, TxId)>> {
        let lower = encode_vae_key(v_eid, AttrId::MIN_PERM, EntityId::MIN_PERM, TxId::MAX_USER);
        let upper = encode_vae_key(v_eid, AttrId::MAX_PERM, EntityId::MAX_PERM, TxId::ZERO);
        TripleValueRefAttrIter::new(self.tx.iterator(), lower, upper)
    }
}

enum LatestTripleExistence {
    Asserted,
    Retracted,
    NotFound,
}

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
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, StaticValue, TxId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (eid, aid, tid) = decode_ea_key(k_slice)?;
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_tx_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((eid, aid, v.to_static(), tid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleEntityAttrIter {
    type Item = Result<(EntityId, AttrId, StaticValue, TxId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

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
    fn next_inner(&mut self) -> Result<Option<(AttrId, EntityId, StaticValue, TxId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_tx_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, eid, v.to_static(), tid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrEntityIter {
    type Item = Result<(AttrId, EntityId, StaticValue, TxId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

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
    fn next_inner(&mut self) -> Result<Option<(AttrId, StaticValue, EntityId, TxId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (aid, eid, tid) = decode_ae_key(k_slice)?;
                    let v = decode_value_from_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_tx_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((aid, v.to_static(), eid, tid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleAttrValueIter {
    type Item = Result<(AttrId, StaticValue, EntityId, TxId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

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
    fn next_inner(&mut self) -> Result<Option<(EntityId, AttrId, EntityId, TxId)>> {
        loop {
            self.it.seek(&self.current);
            match self.it.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    let (v_eid, aid, eid, tid) = decode_vae_key(k_slice)?;
                    self.current.copy_from_slice(k_slice);
                    self.current.encoded_entity_amend_tx_to_first();
                    let op = StoreOp::try_from(v_slice[0])?;
                    if op.is_assert() {
                        return Ok(Some((v_eid, aid, eid, tid)));
                    }
                }
            }
        }
    }
}

impl Iterator for TripleValueRefAttrIter {
    type Item = Result<(EntityId, AttrId, EntityId, TxId)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
