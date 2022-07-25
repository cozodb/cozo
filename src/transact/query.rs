use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use anyhow::Result;
use itertools::Itertools;

use crate::data::attr::Attribute;
use crate::data::keyword::Keyword;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::preprocess::query::QueryProcError;
use crate::runtime::transact::SessionTx;
use crate::transact::throwaway::{ThrowawayArea, ThrowawayId};
use crate::Validity;

#[derive(Debug)]
pub enum Relation {
    Fixed(InlineFixedRelation),
    Triple(TripleRelation),
    Derived(StoredDerivedRelation),
    Join(Box<InnerJoin>),
    Reorder(ReorderRelation),
}

impl Relation {
    pub(crate) fn unit() -> Self {
        Self::Fixed(InlineFixedRelation::unit())
    }
    pub(crate) fn is_unit(&self) -> bool {
        if let Relation::Fixed(r) = self {
            r.bindings.is_empty() && r.data.len() == 1
        } else {
            false
        }
    }
    pub(crate) fn cartesian_join(self, right: Relation) -> Self {
        self.join(right, vec![], vec![])
    }
    pub(crate) fn derived(bindings: Vec<Keyword>, storage: ThrowawayArea) -> Self {
        Self::Derived(StoredDerivedRelation {
            bindings,
            storage
        })
    }
    pub(crate) fn triple(
        attr: Attribute,
        vld: Validity,
        e_binding: Keyword,
        v_binding: Keyword,
    ) -> Self {
        Self::Triple(TripleRelation {
            attr,
            vld,
            bindings: [e_binding, v_binding],
        })
    }
    pub(crate) fn singlet(bindings: Vec<Keyword>, data: Vec<DataValue>) -> Self {
        Self::fixed(bindings, vec![data])
    }
    pub(crate) fn fixed(bindings: Vec<Keyword>, data: Vec<Vec<DataValue>>) -> Self {
        Self::Fixed(InlineFixedRelation {
            bindings,
            data,
            to_eliminate: Default::default(),
        })
    }
    pub(crate) fn reorder(self, new_order: Vec<Keyword>) -> Self {
        Self::Reorder(ReorderRelation {
            relation: Box::new(self),
            new_order,
        })
    }
    pub(crate) fn join(
        self,
        right: Relation,
        left_keys: Vec<Keyword>,
        right_keys: Vec<Keyword>,
    ) -> Self {
        Relation::Join(Box::new(InnerJoin {
            left: self,
            right,
            joiner: Joiner {
                left_keys,
                right_keys,
            },
            to_eliminate: Default::default(),
        }))
    }
}

#[derive(Debug)]
pub struct ReorderRelation {
    pub(crate) relation: Box<Relation>,
    pub(crate) new_order: Vec<Keyword>,
}

impl ReorderRelation {
    fn bindings(&self) -> Vec<Keyword> {
        self.new_order.clone()
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<ThrowawayId>,
    ) -> TupleIter<'a> {
        let old_order = self.relation.bindings();
        let old_order_indices: BTreeMap<_, _> = old_order
            .into_iter()
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect();
        let reorder_indices = self
            .new_order
            .iter()
            .map(|k| {
                *old_order_indices
                    .get(k)
                    .expect("program logic error: reorder indices mismatch")
            })
            .collect_vec();
        Box::new(
            self.relation
                .iter(tx, epoch, use_delta)
                .map_ok(move |tuple| {
                    let old = tuple.0;
                    let new = reorder_indices
                        .iter()
                        .map(|i| old[*i].clone())
                        .collect_vec();
                    Tuple(new)
                }),
        )
    }
}

#[derive(Debug)]
pub struct InlineFixedRelation {
    pub(crate) bindings: Vec<Keyword>,
    pub(crate) data: Vec<Vec<DataValue>>,
    pub(crate) to_eliminate: BTreeSet<Keyword>,
}

impl InlineFixedRelation {
    pub(crate) fn unit() -> Self {
        Self {
            bindings: vec![],
            data: vec![vec![]],
            to_eliminate: Default::default(),
        }
    }
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Keyword>) -> Result<()> {
        for binding in &self.bindings {
            if !used.contains(binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        Ok(())
    }
}

impl InlineFixedRelation {
    pub(crate) fn join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        if self.data.is_empty() {
            Box::new([].into_iter())
        } else if self.data.len() == 1 {
            let data = self.data[0].clone();
            let right_join_values = right_join_indices
                .into_iter()
                .map(|v| data[v].clone())
                .collect_vec();
            Box::new(left_iter.filter_map_ok(move |tuple| {
                let left_join_values = left_join_indices.iter().map(|v| &tuple.0[*v]).collect_vec();
                if left_join_values.into_iter().eq(right_join_values.iter()) {
                    let mut ret = tuple.0;
                    ret.extend_from_slice(&data);

                    if !eliminate_indices.is_empty() {
                        ret = ret
                            .into_iter()
                            .enumerate()
                            .filter_map(|(i, v)| {
                                if eliminate_indices.contains(&i) {
                                    None
                                } else {
                                    Some(v)
                                }
                            })
                            .collect_vec();
                    }
                    Some(Tuple(ret))
                } else {
                    None
                }
            }))
        } else {
            let mut right_mapping = BTreeMap::new();
            for data in &self.data {
                let right_join_values = right_join_indices.iter().map(|v| &data[*v]).collect_vec();
                match right_mapping.get_mut(&right_join_values) {
                    None => {
                        right_mapping.insert(right_join_values, vec![data]);
                    }
                    Some(coll) => {
                        coll.push(data);
                    }
                }
            }
            Box::new(
                left_iter
                    .filter_map_ok(move |tuple| {
                        let left_join_values =
                            left_join_indices.iter().map(|v| &tuple.0[*v]).collect_vec();
                        right_mapping.get(&left_join_values).map(|v| {
                            v.iter()
                                .map(|right_values| {
                                    let mut left_data = tuple.0.clone();
                                    left_data.extend_from_slice(right_values);
                                    Tuple(left_data)
                                })
                                .collect_vec()
                        })
                    })
                    .flatten_ok(),
            )
        }
    }
}

#[derive(Debug)]
pub struct TripleRelation {
    pub(crate) attr: Attribute,
    pub(crate) vld: Validity,
    pub(crate) bindings: [Keyword; 2],
}

fn flatten_err<T, E1: Into<anyhow::Error>, E2: Into<anyhow::Error>>(
    v: std::result::Result<std::result::Result<T, E2>, E1>,
) -> Result<T> {
    match v {
        Err(e) => Err(e.into()),
        Ok(Err(e)) => Err(e.into()),
        Ok(Ok(v)) => Ok(v),
    }
}

fn invert_option_err<T>(v: Result<Option<T>>) -> Option<Result<T>> {
    match v {
        Err(e) => Some(Err(e)),
        Ok(None) => None,
        Ok(Some(v)) => Some(Ok(v)),
    }
}

impl TripleRelation {
    pub(crate) fn join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        match right_join_indices.len() {
            0 => self.cartesian_join(left_iter, tx),
            2 => {
                let right_first = *right_join_indices.first().unwrap();
                let right_second = *right_join_indices.last().unwrap();
                let left_first = *left_join_indices.first().unwrap();
                let left_second = *left_join_indices.last().unwrap();
                match (right_first, right_second) {
                    (0, 1) => {
                        self.ev_join(left_iter, left_first, left_second, tx, eliminate_indices)
                    }
                    (1, 0) => {
                        self.ev_join(left_iter, left_second, left_first, tx, eliminate_indices)
                    }
                    _ => panic!("should not happen"),
                }
            }
            1 => {
                if right_join_indices[0] == 0 {
                    self.e_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else if self.attr.val_type.is_ref_type() {
                    self.v_ref_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else if self.attr.indexing.should_index() {
                    self.v_index_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else {
                    self.v_no_index_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                }
            }
            _ => unreachable!(),
        }
    }
    fn cartesian_join<'a>(&'a self, left_iter: TupleIter<'a>, tx: &'a SessionTx) -> TupleIter<'a> {
        // [f, f] not really a join
        Box::new(
            left_iter
                .map_ok(|tuple| {
                    tx.triple_a_before_scan(self.attr.id, self.vld)
                        .map_ok(move |(_, e_id, val)| {
                            let mut ret = tuple.0.clone();
                            ret.push(DataValue::EnId(e_id));
                            ret.push(val);
                            Tuple(ret)
                        })
                })
                .flatten_ok()
                .map(flatten_err),
        )
    }
    fn ev_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        // [b, b] actually a filter
        Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let eid = tuple.0.get(left_e_idx).unwrap().get_entity_id()?;
                    let v = tuple.0.get(left_v_idx).unwrap();
                    let exists = tx.eav_exists(eid, self.attr.id, v, self.vld)?;
                    if exists {
                        let v = v.clone();
                        let mut ret = tuple.0;
                        ret.push(DataValue::EnId(eid));
                        ret.push(v);

                        if !eliminate_indices.is_empty() {
                            ret = ret
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec();
                        }
                        Ok(Some(Tuple(ret)))
                    } else {
                        Ok(None)
                    }
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        )
    }
    fn e_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        // [b, f]
        Box::new(
            left_iter
                .map_ok(move |tuple| {
                    let eliminate_indices = eliminate_indices.clone();
                    tuple
                        .0
                        .get(left_e_idx)
                        .unwrap()
                        .get_entity_id()
                        .map(move |eid| {
                            tx.triple_ea_before_scan(eid, self.attr.id, self.vld)
                                .map_ok(move |(eid, _, val)| {
                                    let mut ret = tuple.0.clone();
                                    ret.push(DataValue::EnId(eid));
                                    ret.push(val);
                                    if !eliminate_indices.is_empty() {
                                        ret = ret
                                            .into_iter()
                                            .enumerate()
                                            .filter_map(|(i, v)| {
                                                if eliminate_indices.contains(&i) {
                                                    None
                                                } else {
                                                    Some(v)
                                                }
                                            })
                                            .collect_vec();
                                    }
                                    Tuple(ret)
                                })
                        })
                })
                .map(flatten_err)
                .flatten_ok()
                .map(flatten_err),
        )
    }
    fn v_ref_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        // [f, b] where b is a ref
        Box::new(
            left_iter
                .map_ok(move |tuple| {
                    let eliminate_indices = eliminate_indices.clone();
                    tuple
                        .0
                        .get(left_v_idx)
                        .unwrap()
                        .get_entity_id()
                        .map(move |v_eid| {
                            tx.triple_vref_a_before_scan(v_eid, self.attr.id, self.vld)
                                .map_ok(move |(_, _, e_id)| {
                                    let mut ret = tuple.0.clone();
                                    ret.push(DataValue::EnId(e_id));
                                    ret.push(DataValue::EnId(v_eid));

                                    if !eliminate_indices.is_empty() {
                                        ret = ret
                                            .into_iter()
                                            .enumerate()
                                            .filter_map(|(i, v)| {
                                                if eliminate_indices.contains(&i) {
                                                    None
                                                } else {
                                                    Some(v)
                                                }
                                            })
                                            .collect_vec();
                                    }

                                    Tuple(ret)
                                })
                        })
                })
                .map(flatten_err)
                .flatten_ok()
                .map(flatten_err),
        )
    }
    fn v_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        // [f, b] where b is indexed
        Box::new(
            left_iter
                .map_ok(move |tuple| {
                    let eliminate_indices = eliminate_indices.clone();
                    let val = tuple.0.get(left_v_idx).unwrap();
                    tx.triple_av_before_scan(self.attr.id, val, self.vld)
                        .map_ok(move |(_, val, eid)| {
                            let mut ret = tuple.0.clone();
                            ret.push(DataValue::EnId(eid));
                            ret.push(val);

                            if !eliminate_indices.is_empty() {
                                ret = ret
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec();
                            }

                            Tuple(ret)
                        })
                })
                .flatten_ok()
                .map(flatten_err),
        )
    }
    fn v_no_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> TupleIter<'a> {
        // [f, b] where b is not indexed
        let mut throwaway = tx.new_throwaway();
        for item in tx.triple_a_before_scan(self.attr.id, self.vld) {
            match item {
                Err(e) => return Box::new([Err(e)].into_iter()),
                Ok((_, eid, val)) => {
                    let t = Tuple(vec![val, DataValue::EnId(eid)]);
                    if let Err(e) = throwaway.put(&t, 0) {
                        return Box::new([Err(e.into())].into_iter());
                    }
                }
            }
        }
        Box::new(
            left_iter
                .map_ok(move |tuple| {
                    let val = tuple.0.get(left_v_idx).unwrap();
                    let prefix = Tuple(vec![val.clone()]);
                    let eliminate_indices = eliminate_indices.clone();
                    throwaway
                        .scan_prefix(&prefix)
                        .map_ok(move |Tuple(mut found)| {
                            let v_eid = found.pop().unwrap();
                            let mut ret = tuple.0.clone();
                            ret.push(v_eid);
                            if !eliminate_indices.is_empty() {
                                ret = ret
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec();
                            }
                            Tuple(ret)
                        })
                })
                .flatten_ok()
                .map(flatten_err),
        )
    }
}

fn get_eliminate_indices(bindings: &[Keyword], eliminate: &BTreeSet<Keyword>) -> BTreeSet<usize> {
    bindings
        .iter()
        .enumerate()
        .filter_map(|(idx, kw)| {
            if eliminate.contains(kw) {
                Some(idx)
            } else {
                None
            }
        })
        .collect::<BTreeSet<_>>()
}

#[derive(Debug)]
pub struct StoredDerivedRelation {
    pub(crate) bindings: Vec<Keyword>,
    pub(crate) storage: ThrowawayArea,
}

impl StoredDerivedRelation {
    fn iter(&self, epoch: Option<u32>, use_delta: &BTreeSet<ThrowawayId>) -> TupleIter {
        if epoch == Some(0) {
            return Box::new(iter::empty());
        }

        let scan_epoch = match epoch {
            None => 0,
            Some(ep) => {
                if use_delta.contains(&self.storage.id) {
                    ep - 1
                } else {
                    0
                }
            }
        };

        Box::new(self.storage.scan_all_for_epoch(scan_epoch))
    }
    fn join_is_prefix(&self, right_join_indices: &[usize]) -> bool {
        let mut indices = right_join_indices.to_vec();
        indices.sort();
        let l = indices.len();
        indices.into_iter().eq(0..l)
    }
    fn prefix_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
        epoch: Option<u32>,
        use_delta: &BTreeSet<ThrowawayId>,
    ) -> TupleIter<'a> {
        if epoch == Some(0) {
            return Box::new(iter::empty());
        }
        let mut right_invert_indices = right_join_indices.iter().enumerate().collect_vec();
        right_invert_indices.sort_by_key(|(_, b)| **b);
        let left_to_prefix_indices = right_invert_indices
            .into_iter()
            .map(|(a, _)| left_join_indices[a])
            .collect_vec();
        let scan_epoch = match epoch {
            None => 0,
            Some(ep) => {
                if use_delta.contains(&self.storage.id) {
                    ep - 1
                } else {
                    0
                }
            }
        };
        Box::new(
            left_iter
                .map_ok(move |tuple| {
                    let eliminate_indices = eliminate_indices.clone();
                    let prefix = Tuple(
                        left_to_prefix_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );
                    self.storage
                        .scan_prefix_for_epoch(&prefix, scan_epoch)
                        .filter_map_ok(move |found| {
                            // dbg!("filter", &tuple, &prefix, &found);
                            let mut ret = tuple.0.clone();
                            ret.extend(found.0);

                            if !eliminate_indices.is_empty() {
                                ret = ret
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec();
                            }
                            Some(Tuple(ret))
                        })
                })
                .flatten_ok()
                .map(flatten_err),
        )
    }
}

#[derive(Debug)]
pub(crate) struct Joiner {
    // invariant: these are of the same lengths
    pub(crate) left_keys: Vec<Keyword>,
    pub(crate) right_keys: Vec<Keyword>,
}

impl Joiner {
    pub(crate) fn join_indices(
        &self,
        left_bindings: &[Keyword],
        right_bindings: &[Keyword],
    ) -> Result<(Vec<usize>, Vec<usize>)> {
        let left_binding_map = left_bindings
            .iter()
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<BTreeMap<_, _>>();
        let right_binding_map = right_bindings
            .iter()
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<BTreeMap<_, _>>();
        let mut ret_l = Vec::with_capacity(self.left_keys.len());
        let mut ret_r = Vec::with_capacity(self.left_keys.len());
        for (l, r) in self.left_keys.iter().zip(self.right_keys.iter()) {
            let l_pos = match left_binding_map.get(l) {
                None => {
                    return Err(QueryProcError::LogicError(format!(
                        "join key is wrong: left binding for {} not found: left {:?} vs right {:?}, {:?}",
                        l, left_bindings, right_bindings, self
                    )).into());
                }
                Some(p) => p,
            };
            let r_pos = match right_binding_map.get(r) {
                None => {
                    return Err(QueryProcError::LogicError(format!(
                        "join key is wrong: right binding for {} not found: left {:?} vs right {:?}, {:?}",
                        r, left_bindings, right_bindings, self
                    )).into());
                }
                Some(p) => p,
            };
            ret_l.push(*l_pos);
            ret_r.push(*r_pos)
        }
        Ok((ret_l, ret_r))
    }
}

#[derive(Debug)]
pub struct InnerJoin {
    pub(crate) left: Relation,
    pub(crate) right: Relation,
    pub(crate) joiner: Joiner,
    pub(crate) to_eliminate: BTreeSet<Keyword>,
}

impl Relation {
    pub(crate) fn eliminate_temp_vars(&mut self, used: &BTreeSet<Keyword>) -> Result<()> {
        match self {
            Relation::Fixed(r) => r.do_eliminate_temp_vars(used),
            Relation::Triple(r) => Ok(()),
            Relation::Derived(r) => Ok(()),
            Relation::Join(r) => r.do_eliminate_temp_vars(used),
            Relation::Reorder(r) => r.relation.eliminate_temp_vars(used),
        }
    }

    fn eliminate_set(&self) -> Option<&BTreeSet<Keyword>> {
        match self {
            Relation::Fixed(r) => Some(&r.to_eliminate),
            Relation::Triple(_) => None,
            Relation::Derived(_) => None,
            Relation::Join(r) => Some(&r.to_eliminate),
            Relation::Reorder(_) => None,
        }
    }

    pub fn bindings(&self) -> Vec<Keyword> {
        let ret = self.bindings_before_eliminate();
        if let Some(to_eliminate) = self.eliminate_set() {
            ret.into_iter()
                .filter(|kw| !to_eliminate.contains(kw))
                .collect()
        } else {
            ret
        }
    }

    fn bindings_before_eliminate(&self) -> Vec<Keyword> {
        match self {
            Relation::Fixed(f) => f.bindings.clone(),
            Relation::Triple(t) => t.bindings.to_vec(),
            Relation::Derived(d) => d.bindings.clone(),
            Relation::Join(j) => j.bindings(),
            Relation::Reorder(r) => r.bindings(),
        }
    }
    pub fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<ThrowawayId>,
    ) -> TupleIter<'a> {
        match self {
            Relation::Fixed(f) => Box::new(f.data.iter().map(|t| Ok(Tuple(t.clone())))),
            Relation::Triple(r) => Box::new(
                tx.triple_a_before_scan(r.attr.id, r.vld)
                    .map_ok(|(_, e_id, y)| Tuple(vec![DataValue::EnId(e_id), y])),
            ),
            Relation::Derived(r) => r.iter(epoch, use_delta),
            Relation::Join(j) => j.iter(tx, epoch, use_delta),
            Relation::Reorder(r) => r.iter(tx, epoch, use_delta),
        }
    }
}

impl InnerJoin {
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Keyword>) -> Result<()> {
        for binding in self.bindings() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut left = used.clone();
        left.extend(self.joiner.left_keys.clone());
        self.left.eliminate_temp_vars(&left)?;
        let mut right = used.clone();
        right.extend(self.joiner.right_keys.clone());
        self.right.eliminate_temp_vars(&right)?;
        Ok(())
    }

    pub(crate) fn bindings(&self) -> Vec<Keyword> {
        let mut ret = self.left.bindings();
        ret.extend(self.right.bindings());
        debug_assert_eq!(ret.len(), ret.iter().collect::<BTreeSet<_>>().len());
        ret
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<ThrowawayId>,
    ) -> TupleIter<'a> {
        let bindings = self.bindings();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        match &self.right {
            Relation::Fixed(f) => {
                let join_indices = self
                    .joiner
                    .join_indices(&self.left.bindings(), &self.right.bindings())
                    .unwrap();
                f.join(
                    self.left.iter(tx, epoch, use_delta),
                    join_indices,
                    eliminate_indices,
                )
            }
            Relation::Triple(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(&self.left.bindings(), &self.right.bindings())
                    .unwrap();
                r.join(
                    self.left.iter(tx, epoch, use_delta),
                    join_indices,
                    tx,
                    eliminate_indices,
                )
            }
            Relation::Derived(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(&self.left.bindings(), &self.right.bindings())
                    .unwrap();
                if r.join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        self.left.iter(tx, epoch, use_delta),
                        join_indices,
                        eliminate_indices,
                        epoch,
                        use_delta,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, epoch, use_delta)
                }
            }
            Relation::Join(_) => self.materialized_join(tx, eliminate_indices, epoch, use_delta),
            Relation::Reorder(_) => {
                panic!("joining on reordered")
            }
        }
    }
    fn materialized_join<'a>(
        &'a self,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
        epoch: Option<u32>,
        use_delta: &BTreeSet<ThrowawayId>,
    ) -> TupleIter<'a> {
        let right_bindings = self.right.bindings();
        let (left_join_indices, right_join_indices) = self
            .joiner
            .join_indices(&self.left.bindings(), &right_bindings)
            .unwrap();
        let right_join_indices_set = BTreeSet::from_iter(right_join_indices.iter().cloned());
        let mut right_store_indices = right_join_indices;
        for i in 0..right_bindings.len() {
            if !right_join_indices_set.contains(&i) {
                right_store_indices.push(i)
            }
        }
        let right_invert_indices = right_store_indices
            .iter()
            .enumerate()
            .sorted_by_key(|(_, b)| **b)
            .map(|(a, _)| a)
            .collect_vec();
        let throwaway = tx.new_throwaway();
        for item in self.right.iter(tx, epoch, use_delta) {
            match item {
                Ok(tuple) => {
                    let stored_tuple = Tuple(
                        right_store_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );
                    if let Err(e) = throwaway.put(&stored_tuple, 0) {
                        return Box::new([Err(e.into())].into_iter());
                    }
                }
                Err(e) => return Box::new([Err(e)].into_iter()),
            }
        }
        Box::new(
            self.left
                .iter(tx, epoch, use_delta)
                .map_ok(move |tuple| {
                    let eliminate_indices = eliminate_indices.clone();
                    let prefix = Tuple(
                        left_join_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );
                    let restore_indices = right_invert_indices.clone();
                    throwaway.scan_prefix(&prefix).map_ok(move |found| {
                        let mut ret = tuple.0.clone();
                        for i in restore_indices.iter() {
                            ret.push(found.0[*i].clone());
                        }
                        if !eliminate_indices.is_empty() {
                            ret = ret
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec();
                        }
                        Tuple(ret)
                    })
                })
                .flatten_ok()
                .map(flatten_err),
        )
    }
}
