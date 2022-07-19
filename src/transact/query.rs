use std::collections::BTreeMap;

use anyhow::Result;
use itertools::Itertools;

use crate::data::attr::Attribute;
use crate::data::keyword::Keyword;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::runtime::transact::SessionTx;
use crate::transact::pull::PullSpec;
use crate::Validity;

pub(crate) struct QuerySpec {
    find: Vec<(Keyword, PullSpec)>,
    rules: (),
    input: (),
    order: (),
    limit: Option<usize>,
    offset: Option<usize>,
}

pub(crate) struct InlineFixedRelation {
    bindings: Vec<Keyword>,
    data: Vec<Vec<DataValue>>,
}

impl InlineFixedRelation {
    pub(crate) fn join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
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
                    let mut left_data = tuple.0;
                    left_data.extend_from_slice(&data);
                    Some(Tuple(left_data))
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

pub(crate) struct TripleRelation {
    attr: Attribute,
    vld: Validity,
    bindings: [Keyword; 2],
}

impl TripleRelation {
    pub(crate) fn join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
    ) -> TupleIter<'a> {
        match right_join_indices.len() {
            0 => self.cartesian_join(left_iter),
            2 => {
                let right_first = *right_join_indices.first().unwrap();
                let right_second = *right_join_indices.last().unwrap();
                let left_first = *left_join_indices.first().unwrap();
                let left_second = *left_join_indices.last().unwrap();
                match (right_first, right_second) {
                    (0, 1) => self.ev_join(left_iter, left_first, left_second),
                    (1, 0) => self.ev_join(left_iter, left_second, left_first),
                    _ => panic!("should not happen"),
                }
            }
            1 => {
                if right_join_indices[0] == 0 {
                    self.e_join(left_iter, left_join_indices[0])
                } else if self.attr.val_type.is_ref_type() {
                    self.v_ref_join(left_iter, left_join_indices[0])
                } else if self.attr.indexing.should_index() {
                    self.v_index_join(left_iter, left_join_indices[0])
                } else {
                    self.v_no_index_join(left_iter, left_join_indices[0])
                }
            }
            _ => unreachable!(),
        }
    }
    fn cartesian_join<'a>(&'a self, left_iter: TupleIter<'a>) -> TupleIter<'a> {
        // [f, f] not really a join
        todo!()
    }
    fn ev_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        left_v_idx: usize,
    ) -> TupleIter<'a> {
        // [b, b] actually a filter
        todo!()
    }
    fn e_join<'a>(&'a self, left_iter: TupleIter<'a>, left_idx: usize) -> TupleIter<'a> {
        // [b, f]
        todo!()
    }
    fn v_ref_join<'a>(&'a self, left_iter: TupleIter<'a>, left_idx: usize) -> TupleIter<'a> {
        // [f, b] where b is a ref
        todo!()
    }
    fn v_index_join<'a>(&'a self, left_iter: TupleIter<'a>, left_idx: usize) -> TupleIter<'a> {
        // [f, b] where b is indexed
        todo!()
    }
    fn v_no_index_join<'a>(&'a self, left_iter: TupleIter<'a>, left_idx: usize) -> TupleIter<'a> {
        // [f, b] where b is not indexed
        todo!()
    }
}

pub(crate) struct ProjectedRelation {
    relation: Relation,
    eliminate: Vec<Keyword>,
}

pub(crate) enum Relation {
    Fixed(InlineFixedRelation),
    Triple(TripleRelation),
    Derived(StoredDerivedRelation),
    Join(Box<InnerJoin>),
    Project(Box<ProjectedRelation>),
}

pub(crate) struct StoredDerivedRelation {
    name: Keyword,
    arity: usize,
    bindings: Vec<Keyword>,
}

pub(crate) struct Joiner {
    // invariant: these are of the same lengths
    left_keys: Vec<Keyword>,
    right_keys: Vec<Keyword>,
}

impl Joiner {
    pub(crate) fn len(&self) -> usize {
        self.left_keys.len()
    }
    pub(crate) fn swap(self) -> Self {
        Self {
            left_keys: self.right_keys,
            right_keys: self.left_keys,
        }
    }
    pub(crate) fn join_indices(
        &self,
        left_bindings: &[Keyword],
        right_bindings: &[Keyword],
    ) -> (Vec<usize>, Vec<usize>) {
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
            let l_pos = left_binding_map
                .get(l)
                .expect("program logic error: join key is wrong");
            let r_pos = right_binding_map
                .get(r)
                .expect("program logic error: join key is wrong");
            ret_l.push(*l_pos);
            ret_r.push(*r_pos)
        }
        (ret_l, ret_r)
    }
}

pub(crate) struct InnerJoin {
    left: Relation,
    right: Relation,
    joiner: Joiner,
}

impl Relation {
    pub(crate) fn bindings(&self) -> &[Keyword] {
        match self {
            Relation::Fixed(f) => &f.bindings,
            Relation::Triple(t) => &t.bindings,
            Relation::Derived(d) => todo!(),
            Relation::Join(j) => todo!(),
            Relation::Project(p) => todo!(),
        }
    }
    pub(crate) fn iter(&self, tx: &mut SessionTx) -> TupleIter {
        match self {
            Relation::Fixed(f) => Box::new(f.data.iter().map(|t| Ok(Tuple(t.clone())))),
            Relation::Triple(r) => Box::new(
                tx.triple_a_before_scan(r.attr.id, r.vld)
                    .map_ok(|(_, e_id, y)| Tuple(vec![DataValue::EnId(e_id), y])),
            ),
            Relation::Derived(r) => {
                todo!()
            }
            Relation::Join(j) => j.iter(tx),
            Relation::Project(_) => {
                todo!()
            }
        }
    }
}

impl InnerJoin {
    pub(crate) fn iter(&self, tx: &mut SessionTx) -> TupleIter {
        let left_iter = self.left.iter(tx);
        match &self.right {
            Relation::Fixed(f) => {
                let join_indices = self
                    .joiner
                    .join_indices(self.left.bindings(), self.right.bindings());
                f.join(left_iter, join_indices)
            }
            Relation::Triple(_) => {
                todo!()
            }
            Relation::Derived(_) => {
                todo!()
            }
            Relation::Join(_) => {
                todo!()
            }
            Relation::Project(_) => {
                todo!()
            }
        }
    }
}
