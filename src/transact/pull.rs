use crate::data::attr::{Attribute, AttributeCardinality, AttributeTyping};
use crate::data::encode::{
    decode_ea_key, decode_value_from_key, decode_value_from_val, encode_eav_key, StorageTag,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::keyword::Keyword;
use crate::data::triple::StoreOp;
use crate::data::value::{StaticValue, Value};
use crate::runtime::transact::SessionTx;
use anyhow::Result;
use serde_json::{json, Map};

pub(crate) type PullSpecs = Vec<PullSpec>;

pub(crate) enum PullSpec {
    PullAll,
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
    pub(crate) fn pull(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &PullSpec,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        match spec {
            PullSpec::PullAll => self.pull_all(eid, vld, collector),
            PullSpec::Attr(a_spec) => {
                if a_spec.reverse {
                    self.pull_attr_rev(eid, vld, a_spec, collector)
                } else {
                    self.pull_attr(eid, vld, a_spec, collector)
                }
            }
        }
    }
    pub(crate) fn pull_attr(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &AttrPullSpec,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        if spec.cardinality.is_one() {
            if let Some(found) = self.triple_ea_before_scan(eid, spec.attr.id, vld).next() {
                let (_, _, value) = found?;
                self.pull_attr_collect(spec, value, vld, collector)?;
            } else {
                self.pull_attr_collect(spec, Value::Null, vld, collector)?;
            }
        } else {
            let mut collection: Vec<StaticValue> = vec![];
            let iter = self.triple_ea_before_scan(eid, spec.attr.id, vld);
            for found in iter {
                let (_, _, value) = found?;
                collection.push(value);
                if let Some(n) = spec.take {
                    if n <= collection.len() {
                        break;
                    }
                }
            }
            self.pull_attr_collect_many(spec, collection, vld, collector)?;
        }
        Ok(())
    }
    fn pull_attr_collect(
        &mut self,
        spec: &AttrPullSpec,
        value: StaticValue,
        vld: Validity,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        if spec.nested.is_empty() {
            collector.insert(spec.name.to_string_no_prefix(), value.into());
        } else {
            let eid = value.get_entity_id()?;
            let mut sub_collector = Map::default();
            for sub_spec in &spec.nested {
                self.pull(eid, vld, sub_spec, &mut sub_collector)?;
            }
            collector.insert(spec.name.to_string_no_prefix(), sub_collector.into());
        }
        Ok(())
    }
    fn pull_attr_collect_many(
        &mut self,
        spec: &AttrPullSpec,
        values: Vec<StaticValue>,
        vld: Validity,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        if spec.nested.is_empty() {
            collector.insert(spec.name.to_string_no_prefix(), values.into());
        } else {
            let mut sub_collectors = vec![];
            for value in values {
                let eid = value.get_entity_id()?;
                let mut sub_collector = Map::default();
                for sub_spec in &spec.nested {
                    self.pull(eid, vld, sub_spec, &mut sub_collector)?;
                }
                sub_collectors.push(sub_collector);
            }
            collector.insert(spec.name.to_string_no_prefix(), sub_collectors.into());
        }
        Ok(())
    }
    pub(crate) fn pull_attr_rev(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &AttrPullSpec,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        if spec.cardinality.is_one() {
            if let Some(found) = self
                .triple_vref_a_before_scan(eid, spec.attr.id, vld)
                .next()
            {
                let (_, _, value) = found?;
                self.pull_attr_collect(spec, Value::EnId(value), vld, collector)?;
            } else {
                self.pull_attr_collect(spec, Value::Null, vld, collector)?;
            }
        } else {
            let mut collection: Vec<StaticValue> = vec![];
            let iter = self.triple_vref_a_before_scan(eid, spec.attr.id, vld);
            for found in iter {
                let (_, _, value) = found?;
                collection.push(Value::EnId(value));
                if let Some(n) = spec.take {
                    if n <= collection.len() {
                        break;
                    }
                }
            }
            self.pull_attr_collect_many(spec, collection.into(), vld, collector)?;
        }
        Ok(())
    }
    pub(crate) fn pull_all(
        &mut self,
        eid: EntityId,
        vld: Validity,
        collector: &mut Map<String, JsonValue>,
    ) -> Result<()> {
        let mut current = encode_eav_key(eid, AttrId::MIN_PERM, &Value::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &Value::Bottom, Validity::MIN);

        let mut it = self.tx.iterator().upper_bound(&upper_bound).start();
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
            collector.insert("_id".to_string(), eid.0.into());
            if attr.cardinality.is_many() {
                if attr.val_type == AttributeTyping::Component {
                    let val_id = value.get_entity_id()?;
                    let mut subcollector = Map::default();
                    self.pull_all(val_id, vld, &mut subcollector)?;

                    let arr = collector
                        .entry(attr.keyword.to_string_no_prefix())
                        .or_insert_with(|| json!([]));
                    let arr = arr.as_array_mut().unwrap();
                    arr.push(subcollector.into());
                } else {
                    let arr = collector
                        .entry(attr.keyword.to_string_no_prefix())
                        .or_insert_with(|| json!([]));
                    let arr = arr.as_array_mut().unwrap();
                    arr.push(value.into());
                }
            } else {
                if attr.val_type == AttributeTyping::Component {
                    let val_id = value.get_entity_id()?;
                    let mut subcollector = Map::default();
                    self.pull_all(val_id, vld, &mut subcollector)?;
                    collector.insert(attr.keyword.to_string_no_prefix(), subcollector.into());
                } else {
                    collector.insert(attr.keyword.to_string_no_prefix(), value.into());
                }
            }
            current.encoded_entity_amend_validity_to_inf_past();
            it.seek(&current);
        }
        Ok(())
    }
}
