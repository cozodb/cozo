use std::collections::{BTreeSet, HashSet};

use anyhow::Result;
use either::{Left, Right};
use itertools::Itertools;
use serde_json::{json, Map};
use smallvec::{smallvec, SmallVec, ToSmallVec};

use crate::data::attr::{Attribute, AttributeCardinality, AttributeTyping};
use crate::data::encode::{
    decode_ea_key, decode_value_from_key, decode_value_from_val, encode_eav_key, StorageTag,
};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::json::JsonValue;
use crate::data::symb::Symbol;
use crate::data::triple::StoreOp;
use crate::data::value::DataValue;
use crate::parse::query::QueryOutOptions;
use crate::query::relation::flatten_err;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

pub(crate) type PullSpecs = Vec<PullSpec>;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum PullSpec {
    PullAll,
    PullId(Symbol),
    Attr(AttrPullSpec),
}

impl PullSpec {
    fn as_attr_spec(&self) -> Option<&AttrPullSpec> {
        match self {
            PullSpec::Attr(s) => Some(s),
            _ => None,
        }
    }
}

pub type QueryResult<'a> = Box<dyn Iterator<Item = Result<JsonValue>> + 'a>;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct AttrPullSpec {
    pub(crate) recursive: bool,
    pub(crate) reverse: bool,
    pub(crate) attr: Attribute,
    pub(crate) default_val: DataValue,
    pub(crate) name: Symbol,
    pub(crate) cardinality: AttributeCardinality,
    pub(crate) take: Option<usize>,
    pub(crate) nested: PullSpecs,
    pub(crate) recursion_limit: Option<usize>,
    pub(crate) recursion_depth: usize,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub(crate) struct CurrentPath(SmallVec<[u16; 8]>);

impl CurrentPath {
    pub(crate) fn new(idx: usize) -> Result<Self> {
        Ok(Self(smallvec![idx.try_into()?]))
    }
    fn get_from_root<'a>(&self, depth: usize, root: &'a [PullSpec]) -> &'a [PullSpec] {
        let mut current = root;
        let indices = &self.0[..self.0.len() - depth];
        for i in indices {
            current = &current[*i as usize].as_attr_spec().unwrap().nested;
        }
        current
    }
    fn push(&self, idx: usize) -> Result<Self> {
        let mut ret = CurrentPath(Default::default());
        ret.0.clone_from(&self.0);
        ret.0.push(idx.try_into()?);
        Ok(ret)
    }
    fn recurse_pop(&self, depth: usize) -> Self {
        Self(self.0[..self.0.len() + 1 - depth].to_smallvec())
    }
    fn pop_to_last(&self) -> Self {
        self.recurse_pop(2)
    }
}

impl SessionTx {
    pub(crate) fn run_pull_on_query_results(
        &mut self,
        res_store: TempStore,
        out_opts: QueryOutOptions,
    ) -> Result<QueryResult<'_>> {
        let out_iter = match out_opts.offset {
            None => Left(res_store.scan_all()),
            Some(n) => Right(res_store.scan_all().skip(n)),
        };
        match out_opts.out_spec {
            None => Ok(Box::new(out_iter.map_ok(|tuple| {
                JsonValue::Array(tuple.0.into_iter().map(JsonValue::from).collect_vec())
            }))),
            Some((pull_specs, out_keys)) => {
                // type OutSpec = (Vec<(usize, Option<PullSpecs>)>, Option<Vec<String>>);
                Ok(Box::new(
                    out_iter
                        .map_ok(move |tuple| -> Result<JsonValue> {
                            let tuple = tuple.0;
                            let res_iter =
                                pull_specs.iter().map(|(idx, spec)| -> Result<JsonValue> {
                                    let val = tuple.get(*idx).unwrap();
                                    match spec {
                                        None => Ok(JsonValue::from(val.clone())),
                                        Some(specs) => {
                                            let eid = AttributeTyping::Ref
                                                .coerce_value(val.clone())?
                                                .get_entity_id()
                                                .unwrap();
                                            let mut collected = Default::default();
                                            let mut recursive_seen = Default::default();
                                            for (idx, spec) in specs.iter().enumerate() {
                                                self.pull(
                                                    eid,
                                                    out_opts.vld,
                                                    spec,
                                                    0,
                                                    &specs,
                                                    CurrentPath::new(idx)?,
                                                    &mut collected,
                                                    &mut recursive_seen,
                                                )?;
                                            }
                                            Ok(JsonValue::Object(collected))
                                        }
                                    }
                                });
                            match &out_keys {
                                None => {
                                    let v: Vec<_> = res_iter.try_collect()?;
                                    Ok(json!(v))
                                }
                                Some(keys) => {
                                    let map: Map<_, _> = keys
                                        .iter()
                                        .zip(res_iter)
                                        .map(|(k, v)| match v {
                                            Ok(v) => Ok((k.clone(), v)),
                                            Err(e) => Err(e),
                                        })
                                        .try_collect()?;
                                    Ok(json!(map))
                                }
                            }
                        })
                        .map(flatten_err),
                ))
            }
        }
    }
    pub(crate) fn pull(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &PullSpec,
        depth: usize,
        root: &[PullSpec],
        path: CurrentPath,
        collector: &mut Map<String, JsonValue>,
        recursive_seen: &mut BTreeSet<(CurrentPath, EntityId)>,
    ) -> Result<()> {
        match spec {
            PullSpec::PullAll => {
                let mut seen = HashSet::default();
                self.pull_all(eid, vld, collector, &mut seen)
            }
            PullSpec::Attr(a_spec) => {
                if !a_spec.recursive {
                    recursive_seen.insert((path.pop_to_last(), eid));
                }
                if a_spec.reverse {
                    self.pull_attr_rev(
                        eid,
                        vld,
                        a_spec,
                        depth,
                        root,
                        path,
                        collector,
                        recursive_seen,
                    )
                } else {
                    self.pull_attr(
                        eid,
                        vld,
                        a_spec,
                        depth,
                        root,
                        path,
                        collector,
                        recursive_seen,
                    )
                }
            }
            PullSpec::PullId(kw) => {
                collector.insert(kw.to_string(), eid.into());
                Ok(())
            }
        }
    }
    pub(crate) fn pull_attr(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &AttrPullSpec,
        depth: usize,
        root: &[PullSpec],
        path: CurrentPath,
        collector: &mut Map<String, JsonValue>,
        recursive_seen: &mut BTreeSet<(CurrentPath, EntityId)>,
    ) -> Result<()> {
        if spec.cardinality.is_one() {
            if let Some(found) = self.triple_ea_before_scan(eid, spec.attr.id, vld).next() {
                let (_, _, value) = found?;
                self.pull_attr_collect(
                    spec,
                    value,
                    vld,
                    depth,
                    root,
                    path,
                    collector,
                    recursive_seen,
                )?;
            } else if spec.default_val != DataValue::Null {
                self.pull_attr_collect(
                    spec,
                    spec.default_val.clone(),
                    vld,
                    depth,
                    root,
                    path,
                    collector,
                    recursive_seen,
                )?;
            }
        } else {
            let mut collection: Vec<DataValue> = vec![];
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
            self.pull_attr_collect_many(
                spec,
                collection,
                vld,
                depth,
                root,
                path,
                collector,
                recursive_seen,
            )?;
        }
        Ok(())
    }
    fn pull_attr_collect(
        &mut self,
        spec: &AttrPullSpec,
        value: DataValue,
        vld: Validity,
        depth: usize,
        root: &[PullSpec],
        path: CurrentPath,
        collector: &mut Map<String, JsonValue>,
        recursive_seen: &mut BTreeSet<(CurrentPath, EntityId)>,
    ) -> Result<()> {
        if spec.recursive {
            if let Some(limit) = spec.recursion_limit {
                if depth >= limit {
                    return Ok(());
                }
            }
            let recursion_path = path.recurse_pop(spec.recursion_depth);
            let eid = value.get_entity_id()?;
            let mut sub_collector = Map::default();
            let sentinel = (recursion_path.pop_to_last(), eid);
            if !recursive_seen.insert(sentinel) {
                sub_collector.insert("_id".to_string(), eid.into());
                collector.insert(spec.name.to_string(), sub_collector.into());
                return Ok(());
            }

            let recurse_target = path.get_from_root(spec.recursion_depth, root);
            for sub_spec in recurse_target {
                let next_depth = if let PullSpec::Attr(sub) = sub_spec {
                    if sub.name == spec.name {
                        depth + 1
                    } else {
                        0
                    }
                } else {
                    0
                };
                self.pull(
                    eid,
                    vld,
                    sub_spec,
                    next_depth,
                    root,
                    recursion_path.clone(),
                    &mut sub_collector,
                    recursive_seen,
                )?;
            }

            collector.insert(spec.name.to_string(), sub_collector.into());
        } else if spec.nested.is_empty() {
            collector.insert(spec.name.to_string(), value.into());
        } else {
            let eid = value.get_entity_id()?;
            let sentinel = (path.clone(), eid);
            recursive_seen.insert(sentinel);

            let mut sub_collector = Map::default();
            for (idx, sub_spec) in spec.nested.iter().enumerate() {
                self.pull(
                    eid,
                    vld,
                    sub_spec,
                    depth,
                    root,
                    path.push(idx)?,
                    &mut sub_collector,
                    recursive_seen,
                )?;
            }
            collector.insert(spec.name.to_string(), sub_collector.into());
        }
        Ok(())
    }
    fn pull_attr_collect_many(
        &mut self,
        spec: &AttrPullSpec,
        values: Vec<DataValue>,
        vld: Validity,
        depth: usize,
        root: &[PullSpec],
        path: CurrentPath,
        collector: &mut Map<String, JsonValue>,
        recursive_seen: &mut BTreeSet<(CurrentPath, EntityId)>,
    ) -> Result<()> {
        if spec.recursive {
            if let Some(limit) = spec.recursion_limit {
                if depth >= limit {
                    return Ok(());
                }
            }

            let mut sub_collectors = vec![];
            let recursion_path = path.recurse_pop(spec.recursion_depth);
            for value in values {
                let eid = value.get_entity_id()?;
                let mut sub_collector = Map::default();
                let sentinel = (recursion_path.pop_to_last(), eid);
                if !recursive_seen.insert(sentinel) {
                    sub_collector.insert("_id".to_string(), eid.into());
                    sub_collectors.push(sub_collector);
                    continue;
                }

                let recurse_target = path.get_from_root(spec.recursion_depth, root);
                for sub_spec in recurse_target {
                    let next_depth = if let PullSpec::Attr(sub) = sub_spec {
                        if sub.name == spec.name {
                            depth + 1
                        } else {
                            0
                        }
                    } else {
                        0
                    };

                    self.pull(
                        eid,
                        vld,
                        sub_spec,
                        next_depth,
                        root,
                        recursion_path.clone(),
                        &mut sub_collector,
                        recursive_seen,
                    )?;
                }
                sub_collectors.push(sub_collector);
            }
            collector.insert(spec.name.to_string(), sub_collectors.into());
        } else if spec.nested.is_empty() {
            collector.insert(spec.name.to_string(), values.into());
        } else {
            let mut sub_collectors = vec![];
            for value in values {
                let eid = value.get_entity_id()?;
                let mut sub_collector = Map::default();
                for (idx, sub_spec) in spec.nested.iter().enumerate() {
                    self.pull(
                        eid,
                        vld,
                        sub_spec,
                        depth,
                        root,
                        path.push(idx)?,
                        &mut sub_collector,
                        recursive_seen,
                    )?;
                }
                sub_collectors.push(sub_collector);
            }
            collector.insert(spec.name.to_string(), sub_collectors.into());
        }
        Ok(())
    }
    pub(crate) fn pull_attr_rev(
        &mut self,
        eid: EntityId,
        vld: Validity,
        spec: &AttrPullSpec,
        depth: usize,
        root: &[PullSpec],
        path: CurrentPath,
        collector: &mut Map<String, JsonValue>,
        recursive_seen: &mut BTreeSet<(CurrentPath, EntityId)>,
    ) -> Result<()> {
        if spec.cardinality.is_one() {
            if let Some(found) = self
                .triple_vref_a_before_scan(eid, spec.attr.id, vld)
                .next()
            {
                let (_, _, value) = found?;
                self.pull_attr_collect(
                    spec,
                    value.to_value(),
                    vld,
                    depth,
                    root,
                    path,
                    collector,
                    recursive_seen,
                )?;
            } else if spec.default_val != DataValue::Null {
                self.pull_attr_collect(
                    spec,
                    spec.default_val.clone(),
                    vld,
                    depth,
                    root,
                    path,
                    collector,
                    recursive_seen,
                )?;
            }
        } else {
            let mut collection: Vec<DataValue> = vec![];
            let iter = self.triple_vref_a_before_scan(eid, spec.attr.id, vld);
            for found in iter {
                let (_, _, value) = found?;
                collection.push(value.to_value());
                if let Some(n) = spec.take {
                    if n <= collection.len() {
                        break;
                    }
                }
            }
            self.pull_attr_collect_many(
                spec,
                collection,
                vld,
                depth,
                root,
                path,
                collector,
                recursive_seen,
            )?;
        }
        Ok(())
    }
    pub(crate) fn pull_all(
        &mut self,
        eid: EntityId,
        vld: Validity,
        collector: &mut Map<String, JsonValue>,
        pull_all_seen: &mut HashSet<EntityId>,
    ) -> Result<()> {
        let mut current = encode_eav_key(eid, AttrId::MIN_PERM, &DataValue::Null, Validity::MAX);
        let upper_bound = encode_eav_key(eid, AttrId::MAX_PERM, &DataValue::Bottom, Validity::MIN);

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
            pull_all_seen.insert(eid);
            if attr.cardinality.is_many() {
                if attr.val_type == AttributeTyping::Component {
                    let val_id = value.get_entity_id()?;
                    if pull_all_seen.contains(&val_id) {
                        let arr = collector
                            .entry(attr.name.to_string())
                            .or_insert_with(|| json!([]));
                        let arr = arr.as_array_mut().unwrap();
                        arr.push(value.into());
                    } else {
                        let mut subcollector = Map::default();
                        self.pull_all(val_id, vld, &mut subcollector, pull_all_seen)?;

                        let arr = collector
                            .entry(attr.name.to_string())
                            .or_insert_with(|| json!([]));
                        let arr = arr.as_array_mut().unwrap();
                        arr.push(subcollector.into());
                    }
                } else {
                    let arr = collector
                        .entry(attr.name.to_string())
                        .or_insert_with(|| json!([]));
                    let arr = arr.as_array_mut().unwrap();
                    arr.push(value.into());
                }
            } else if attr.val_type == AttributeTyping::Component {
                let val_id = value.get_entity_id()?;
                if pull_all_seen.contains(&val_id) {
                    collector.insert(attr.name.to_string(), value.into());
                } else {
                    let mut subcollector = Map::default();
                    self.pull_all(val_id, vld, &mut subcollector, pull_all_seen)?;
                    collector.insert(attr.name.to_string(), subcollector.into());
                }
            } else {
                collector.insert(attr.name.to_string(), value.into());
            }
            current.encoded_entity_amend_validity_to_inf_past();
            it.seek(&current);
        }
        Ok(())
    }
}
