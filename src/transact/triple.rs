use crate::data::attr::Attribute;
use crate::data::encode::{
    decode_ae_key, encode_aev_key, encode_ave_key, encode_ave_key_for_unique_v, encode_eav_key,
    encode_unique_attr_val, encode_unique_entity, encode_vae_key,
};
use crate::data::id::{EntityId, TxId};
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::{Value, INLINE_VAL_SIZE_LIMIT};
use crate::runtime::transact::SessionTx;
use anyhow::Result;
use std::sync::atomic::Ordering;

#[derive(Debug, thiserror::Error)]
enum TripleError {
    #[error("use of temp entity id: {0:?}")]
    TempEid(EntityId),
    #[error("use of non-existent entity: {0:?}")]
    EidNotFound(EntityId),
    #[error("unique constraint violated: {0} {1}")]
    UniqueConstraintViolated(Keyword, String),
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
        todo!()
    }
    pub(crate) fn retract_triples_for_attr(
        &mut self,
        eid: EntityId,
        attr: &Attribute,
    ) -> Result<()> {
        todo!()
    }
    pub(crate) fn retract_entity(&mut self, eid: EntityId) -> Result<()> {
        todo!()
    }
    pub(crate) fn entity_exists(&mut self, eid: EntityId, for_update: bool) -> Result<bool> {
        let encoded = encode_unique_entity(eid);
        Ok(self.tx.exists(&encoded, for_update)?)
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
    // pub(crate) fn triple_ea_scan(
    //     &mut self,
    //     eid: EntityId,
    //     aid: AttrId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_ae_scan(
    //     &mut self,
    //     aid: AttrId,
    //     eid: EntityId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_av_scan(
    //     &mut self,
    //     aid: AttrId,
    //     eid: EntityId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_va_scan(
    //     &mut self,
    //     aid: AttrId,
    //     eid: EntityId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_e_scan(
    //     &mut self,
    //     eid: EntityId,
    //     aid: AttrId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_a_scan(
    //     &mut self,
    //     aid: AttrId,
    //     eid: EntityId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
    // pub(crate) fn triple_v_scan(
    //     &mut self,
    //     aid: AttrId,
    //     eid: EntityId,
    // ) -> impl Iterator<Item = Result<Value>> {
    //     todo!()
    // }
}
