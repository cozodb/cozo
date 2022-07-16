use crate::data::attr::{Attribute, AttributeCardinality};
use crate::data::encode::{
    decode_ea_key, decode_value_from_key, decode_value_from_val, encode_eav_key, StorageTag,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::Value;
use crate::runtime::transact::SessionTx;
use anyhow::Result;
use serde_json::json;

pub(crate) type PullSpecs = Vec<PullSpec>;

pub(crate) enum PullSpec {
    None,
    PullAll,
    Recurse(Keyword),
    Attr(AttrPullSpec),
}

pub(crate) struct AttrPullSpec {
    pub(crate) attr: Attribute,
    pub(crate) reverse: bool,
    pub(crate) name: Keyword,
    pub(crate) cardinality: AttributeCardinality,
    pub(crate) take: Option<usize>,
    pub(crate) nested: PullSpecs,
}

pub(crate) struct RecursePullSpec {
    pub(crate) parent: Keyword,
    pub(crate) max_depth: Option<usize>,
}

impl SessionTx {
    pub fn pull_all(&mut self, eid: EntityId, vld: Validity) -> Result<JsonValue> {
        let mut current = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, Validity::MIN);

        let mut it = self.tx.iterator().upper_bound(&upper_bound).start();
        let mut collected = json!({});
        it.seek(&current);
        while let Some((k_slice, v_slice)) = it.pair()? {
            debug_assert_eq!(
                StorageTag::try_from(k_slice[0])?,
                StorageTag::TripleEntityAttrValue
            );
            let (_e_found, a_found, vld_found) = decode_ea_key(k_slice)?;
            current.copy_from_slice(k_slice);

            if vld_found > vld {
                current.encoded_entity_amend_validity(vld);
                it.seek(&current);
                continue;
            }
            let op = StoreOp::try_from(v_slice[0])?;
            if op.is_retract() {
                current.encoded_entity_amend_validity_to_inf_past();
                it.seek(&current);
                continue;
            }
            let attr = self.attr_by_id(a_found)?;
            if attr.is_none() {
                current.encoded_entity_amend_validity_to_inf_past();
                it.seek(&current);
                continue;
            }
            let attr = attr.unwrap();
            let value = if attr.cardinality.is_one() {
                decode_value_from_val(v_slice)?
            } else {
                decode_value_from_key(k_slice)?
            };
            let map_for_entry = collected.as_object_mut().unwrap();
            map_for_entry.insert("_id".to_string(), eid.0.into());
            if attr.cardinality.is_many() {
                let arr = map_for_entry
                    .entry(attr.keyword.to_string_no_prefix())
                    .or_insert_with(|| json!([]));
                let arr = arr.as_array_mut().unwrap();
                arr.push(value.into());
            } else {
                map_for_entry.insert(attr.keyword.to_string_no_prefix(), value.into());
            }
            current.encoded_entity_amend_validity_to_inf_past();
            it.seek(&current);
        }
        Ok(json!(collected))
    }
}
