use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::iter;

use anyhow::{anyhow, bail, Context, Result};
use either::{Left, Right};
use itertools::Itertools;
use log::error;

use crate::data::attr::Attribute;
use crate::data::expr::{compute_bounds, compute_single_bound, Expr};
use crate::data::id::{AttrId, EntityId, Validity};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::runtime::derived::{DerivedRelStore, DerivedRelStoreId};
use crate::runtime::transact::SessionTx;
use crate::runtime::view::ViewRelStore;

pub(crate) enum Relation {
    Fixed(InlineFixedRelation),
    Triple(TripleRelation),
    Derived(DerivedRelation),
    View(ViewRelation),
    Join(Box<InnerJoin>),
    NegJoin(Box<NegJoin>),
    Reorder(ReorderRelation),
    Filter(FilteredRelation),
    Unification(UnificationRelation),
}

pub(crate) struct UnificationRelation {
    parent: Box<Relation>,
    binding: Symbol,
    expr: Expr,
    is_multi: bool,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
}

fn eliminate_from_tuple(mut ret: Tuple, eliminate_indices: &BTreeSet<usize>) -> Tuple {
    if !eliminate_indices.is_empty() {
        ret = Tuple(
            ret.0
                .into_iter()
                .enumerate()
                .filter_map(|(i, v)| {
                    if eliminate_indices.contains(&i) {
                        None
                    } else {
                        Some(v)
                    }
                })
                .collect_vec(),
        );
    }
    ret
}

impl UnificationRelation {
    fn fill_binding_indices(&mut self) -> Result<()> {
        let parent_bindings: BTreeMap<_, _> = self
            .parent
            .bindings_after_eliminate()
            .into_iter()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        self.expr.fill_binding_indices(&parent_bindings)
    }
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.parent.bindings_before_eliminate() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut nxt = used.clone();
        nxt.extend(self.expr.bindings());
        self.parent.eliminate_temp_vars(&nxt)?;
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let mut bindings = self.parent.bindings_after_eliminate();
        bindings.push(self.binding.clone());
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        Ok(if self.is_multi {
            let it = self
                .parent
                .iter(tx, epoch, use_delta)?
                .map_ok(move |tuple| -> Result<Vec<Tuple>> {
                    let result_list = self.expr.eval(&tuple)?;
                    let result_list = result_list.get_list().ok_or_else(|| {
                        anyhow!("multi unification encountered non-list {:?}", result_list)
                    })?;
                    let mut coll = vec![];
                    for result in result_list {
                        let mut ret = tuple.0.clone();
                        ret.push(result.clone());
                        let ret = Tuple(ret);
                        let ret = eliminate_from_tuple(ret, &eliminate_indices);
                        coll.push(ret);
                    }
                    Ok(coll)
                })
                .map(flatten_err)
                .flatten_ok();
            Box::new(it)
        } else {
            Box::new(
                self.parent
                    .iter(tx, epoch, use_delta)?
                    .map_ok(move |tuple| -> Result<Tuple> {
                        let result = self.expr.eval(&tuple)?;
                        let mut ret = tuple.0;
                        ret.push(result);
                        let ret = Tuple(ret);
                        let ret = eliminate_from_tuple(ret, &eliminate_indices);
                        Ok(ret)
                    })
                    .map(flatten_err),
            )
        })
    }
}

pub(crate) struct FilteredRelation {
    parent: Box<Relation>,
    pred: Vec<Expr>,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
}

impl FilteredRelation {
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.parent.bindings_before_eliminate() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut nxt = used.clone();
        for e in self.pred.iter() {
            nxt.extend(e.bindings());
        }
        self.parent.eliminate_temp_vars(&nxt)?;
        Ok(())
    }

    fn fill_binding_indices(&mut self) -> Result<()> {
        let parent_bindings: BTreeMap<_, _> = self
            .parent
            .bindings_after_eliminate()
            .into_iter()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.pred.iter_mut() {
            e.fill_binding_indices(&parent_bindings)?;
        }
        Ok(())
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.parent.bindings_after_eliminate();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        Ok(Box::new(
            self.parent
                .iter(tx, epoch, use_delta)?
                .filter_map(move |tuple| match tuple {
                    Ok(t) => {
                        for p in self.pred.iter() {
                            match p.eval_pred(&t) {
                                Ok(false) => return None,
                                Err(e) => return Some(Err(e)),
                                Ok(true) => {}
                            }
                        }
                        let t = eliminate_from_tuple(t, &eliminate_indices);
                        Some(Ok(t))
                    }
                    Err(e) => Some(Err(e)),
                }),
        ))
    }
}

struct BindingFormatter(Vec<Symbol>);

impl Debug for BindingFormatter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.0.iter().map(|f| f.to_string()).join(", ");
        write!(f, "[{}]", s)
    }
}

impl Debug for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bindings = BindingFormatter(self.bindings_after_eliminate());
        match self {
            Relation::Fixed(r) => {
                if r.bindings.is_empty() && r.data.len() == 1 {
                    f.write_str("Unit")
                } else if r.data.len() == 1 {
                    f.debug_tuple("Singlet")
                        .field(&bindings)
                        .field(r.data.get(0).unwrap())
                        .finish()
                } else {
                    f.debug_tuple("Fixed")
                        .field(&bindings)
                        .field(&["..."])
                        .finish()
                }
            }
            Relation::Triple(r) => f
                .debug_tuple("Triple")
                .field(&bindings)
                .field(&r.attr.name)
                .field(&r.filters)
                .finish(),
            Relation::Derived(r) => f
                .debug_tuple("Derived")
                .field(&bindings)
                .field(&r.storage.rule_name)
                .field(&r.filters)
                .finish(),
            Relation::View(r) => f
                .debug_tuple("Derived")
                .field(&bindings)
                .field(&r.storage.metadata.name)
                .field(&r.filters)
                .finish(),
            Relation::Join(r) => {
                if r.left.is_unit() {
                    r.right.fmt(f)
                } else {
                    f.debug_tuple("Join")
                        .field(&bindings)
                        .field(&r.joiner)
                        .field(&r.left)
                        .field(&r.right)
                        .finish()
                }
            }
            Relation::NegJoin(r) => f
                .debug_tuple("NegJoin")
                .field(&bindings)
                .field(&r.joiner)
                .field(&r.left)
                .field(&r.right)
                .finish(),
            Relation::Reorder(r) => f
                .debug_tuple("Reorder")
                .field(&r.new_order)
                .field(&r.relation)
                .finish(),
            Relation::Filter(r) => f
                .debug_tuple("Filter")
                .field(&bindings)
                .field(&r.pred)
                .field(&r.parent)
                .finish(),
            Relation::Unification(r) => f
                .debug_tuple("Unify")
                .field(&bindings)
                .field(&r.parent)
                .field(&r.binding)
                .field(&r.expr)
                .finish(),
        }
    }
}

impl Relation {
    pub(crate) fn get_filters(&mut self) -> Option<&mut Vec<Expr>> {
        match self {
            Relation::Triple(t) => Some(&mut t.filters),
            Relation::Derived(d) => Some(&mut d.filters),
            Relation::Join(j) => j.right.get_filters(),
            Relation::Filter(f) => Some(&mut f.pred),
            _ => None,
        }
    }
    pub(crate) fn fill_normal_binding_indices(&mut self) -> Result<()> {
        match self {
            Relation::Fixed(_) => {}
            Relation::Triple(t) => {
                t.fill_binding_indices()?;
            }
            Relation::Derived(d) => {
                d.fill_binding_indices()?;
            }
            Relation::View(v) => {
                v.fill_binding_indices()?;
            }
            Relation::Reorder(r) => {
                r.relation.fill_normal_binding_indices()?;
            }
            Relation::Filter(f) => {
                f.parent.fill_normal_binding_indices()?;
                f.fill_binding_indices()?
            }
            Relation::NegJoin(r) => {
                r.left.fill_normal_binding_indices()?;
            }
            Relation::Unification(u) => {
                u.parent.fill_normal_binding_indices()?;
                u.fill_binding_indices()?
            }
            Relation::Join(r) => {
                r.left.fill_normal_binding_indices()?;
            }
        }
        if matches!(self, Relation::Join(_)) {
            let bindings = self.bindings_before_eliminate();
            if let Relation::Join(r) = self {
                r.right.fill_join_binding_indices(bindings)?;
            }
        }
        Ok(())
    }
    pub(crate) fn fill_join_binding_indices(&mut self, bindings: Vec<Symbol>) -> Result<()> {
        match self {
            Relation::Triple(t) => {
                t.fill_join_binding_indices(&bindings)?;
            }
            Relation::Derived(d) => {
                d.fill_join_binding_indices(&bindings)?;
            }
            r => {
                r.fill_normal_binding_indices()?;
            }
        }
        Ok(())
    }
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
    pub(crate) fn derived(bindings: Vec<Symbol>, storage: DerivedRelStore) -> Self {
        Self::Derived(DerivedRelation {
            bindings,
            storage,
            filters: vec![],
        })
    }
    pub(crate) fn view(bindings: Vec<Symbol>, storage: ViewRelStore) -> Self {
        Self::View(ViewRelation {
            bindings,
            storage,
            filters: vec![],
        })
    }
    pub(crate) fn triple(
        attr: Attribute,
        vld: Validity,
        e_binding: Symbol,
        v_binding: Symbol,
    ) -> Self {
        Self::Triple(TripleRelation {
            attr,
            vld,
            bindings: [e_binding, v_binding],
            filters: vec![],
        })
    }
    pub(crate) fn reorder(self, new_order: Vec<Symbol>) -> Self {
        Self::Reorder(ReorderRelation {
            relation: Box::new(self),
            new_order,
        })
    }
    pub(crate) fn filter(self, filter: Expr) -> Self {
        Relation::Filter(FilteredRelation {
            parent: Box::new(self),
            pred: vec![filter],
            to_eliminate: Default::default(),
        })
    }
    pub(crate) fn unify(self, binding: Symbol, expr: Expr, is_multi: bool) -> Self {
        Relation::Unification(UnificationRelation {
            parent: Box::new(self),
            binding,
            expr,
            is_multi,
            to_eliminate: Default::default(),
        })
    }
    pub(crate) fn join(
        self,
        right: Relation,
        left_keys: Vec<Symbol>,
        right_keys: Vec<Symbol>,
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
    pub(crate) fn neg_join(
        self,
        right: Relation,
        left_keys: Vec<Symbol>,
        right_keys: Vec<Symbol>,
    ) -> Self {
        Relation::NegJoin(Box::new(NegJoin {
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
pub(crate) struct ReorderRelation {
    pub(crate) relation: Box<Relation>,
    pub(crate) new_order: Vec<Symbol>,
}

impl ReorderRelation {
    fn bindings(&self) -> Vec<Symbol> {
        self.new_order.clone()
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let old_order = self.relation.bindings_after_eliminate();
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
        Ok(Box::new(self.relation.iter(tx, epoch, use_delta)?.map_ok(
            move |tuple| {
                let old = tuple.0;
                let new = reorder_indices
                    .iter()
                    .map(|i| old[*i].clone())
                    .collect_vec();
                Tuple(new)
            },
        )))
    }
}

#[derive(Debug)]
pub(crate) struct InlineFixedRelation {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) data: Vec<Vec<DataValue>>,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
}

impl InlineFixedRelation {
    pub(crate) fn unit() -> Self {
        Self {
            bindings: vec![],
            data: vec![vec![]],
            to_eliminate: Default::default(),
        }
    }
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
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
    ) -> Result<TupleIter<'a>> {
        Ok(if self.data.is_empty() {
            Box::new(iter::empty())
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
                    let ret = Tuple(ret);
                    let ret = eliminate_from_tuple(ret, &eliminate_indices);
                    Some(ret)
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
        })
    }
}

#[derive(Debug)]
pub(crate) struct TripleRelation {
    pub(crate) attr: Attribute,
    pub(crate) vld: Validity,
    pub(crate) bindings: [Symbol; 2],
    pub(crate) filters: Vec<Expr>,
}

pub(crate) fn flatten_err<T, E1: Into<anyhow::Error>, E2: Into<anyhow::Error>>(
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

fn filter_iter(
    filters: Vec<Expr>,
    it: impl Iterator<Item = Result<Tuple>>,
) -> impl Iterator<Item = Result<Tuple>> {
    it.filter_map_ok(move |t| -> Option<Result<Tuple>> {
        for p in filters.iter() {
            match p.eval_pred(&t) {
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
                Ok(true) => {}
            }
        }
        Some(Ok(t))
    })
    .map(flatten_err)
}

impl TripleRelation {
    fn fill_binding_indices(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
        }
        Ok(())
    }

    fn fill_join_binding_indices(&mut self, bindings: &[Symbol]) -> Result<()> {
        let bindings: BTreeMap<_, _> = bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
        }
        Ok(())
    }

    fn iter<'a>(&'a self, tx: &'a SessionTx) -> Result<TupleIter<'a>> {
        self.join(
            Box::new(iter::once(Ok(Tuple::default()))),
            (vec![], vec![]),
            tx,
            Default::default(),
        )
    }

    pub(crate) fn neg_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        match right_join_indices.len() {
            2 => {
                let right_first = *right_join_indices.first().unwrap();
                let right_second = *right_join_indices.last().unwrap();
                let left_first = *left_join_indices.first().unwrap();
                let left_second = *left_join_indices.last().unwrap();
                match (right_first, right_second) {
                    (0, 1) => {
                        self.neg_ev_join(left_iter, left_first, left_second, tx, eliminate_indices)
                    }
                    (1, 0) => {
                        self.neg_ev_join(left_iter, left_second, left_first, tx, eliminate_indices)
                    }
                    _ => panic!("should not happen"),
                }
            }
            1 => {
                if right_join_indices[0] == 0 {
                    self.neg_e_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else if self.attr.val_type.is_ref_type() {
                    self.neg_v_ref_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else if self.attr.indexing.should_index() {
                    self.neg_v_index_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                } else {
                    self.neg_v_no_index_join(left_iter, left_join_indices[0], tx, eliminate_indices)
                }
            }
            _ => unreachable!(),
        }
    }
    pub(crate) fn join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        match right_join_indices.len() {
            0 => self.cartesian_join(left_iter, tx, eliminate_indices),
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
    fn cartesian_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [f, f] not really a join
        if self.attr.indexing.should_index() && !self.filters.is_empty() {
            if let Some((l_bound, u_bound)) =
                compute_single_bound(&self.filters, &self.bindings[1])?
            {
                let it = left_iter
                    .map_ok(move |tuple| {
                        if self.attr.with_history {
                            Left(
                                tx.triple_av_range_before_scan(
                                    self.attr.id,
                                    &l_bound,
                                    &u_bound,
                                    self.vld,
                                )
                                .map_ok(move |(_, val, e_id)| {
                                    let mut ret = tuple.0.clone();
                                    ret.push(e_id.as_datavalue());
                                    ret.push(val);
                                    Tuple(ret)
                                }),
                            )
                        } else {
                            Right(
                                tx.triple_av_range_scan(self.attr.id, &l_bound, &u_bound)
                                    .map_ok(move |(_, val, e_id)| {
                                        let mut ret = tuple.0.clone();
                                        ret.push(e_id.as_datavalue());
                                        ret.push(val);
                                        Tuple(ret)
                                    }),
                            )
                        }
                    })
                    .flatten_ok()
                    .map(flatten_err);
                return self.return_filtered_iter(it, eliminate_indices);
            }
        }
        let it = left_iter
            .map_ok(|tuple| {
                if self.attr.indexing.should_index() {
                    if let Ok(Some((l_bound, u_bound))) =
                        compute_single_bound(&self.filters, &self.bindings[1])
                    {
                        return Left(if self.attr.with_history {
                            Left(
                                tx.triple_av_range_before_scan(
                                    self.attr.id,
                                    &l_bound,
                                    &u_bound,
                                    self.vld,
                                )
                                .map_ok(move |(_, val, e_id)| {
                                    let mut ret = tuple.0.clone();
                                    ret.push(e_id.as_datavalue());
                                    ret.push(val);
                                    Tuple(ret)
                                }),
                            )
                        } else {
                            Right(
                                tx.triple_av_range_scan(self.attr.id, &l_bound, &u_bound)
                                    .map_ok(move |(_, val, e_id)| {
                                        let mut ret = tuple.0.clone();
                                        ret.push(e_id.as_datavalue());
                                        ret.push(val);
                                        Tuple(ret)
                                    }),
                            )
                        });
                    }
                }
                Right(if self.attr.with_history {
                    Left(tx.triple_a_before_scan(self.attr.id, self.vld).map_ok(
                        move |(_, e_id, val)| {
                            let mut ret = tuple.0.clone();
                            ret.push(e_id.as_datavalue());
                            ret.push(val);
                            Tuple(ret)
                        },
                    ))
                } else {
                    Right(
                        tx.triple_a_scan(self.attr.id)
                            .map_ok(move |(_, e_id, val)| {
                                let mut ret = tuple.0.clone();
                                ret.push(e_id.as_datavalue());
                                ret.push(val);
                                Tuple(ret)
                            }),
                    )
                })
            })
            .flatten_ok()
            .map(flatten_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn neg_ev_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let eid = tuple
                        .0
                        .get(left_e_idx)
                        .unwrap()
                        .get_entity_id()
                        .with_context(|| format!("{:?}", self))?;
                    let v = tuple.0.get(left_v_idx).unwrap();
                    let exists = tx.eav_exists(eid, self.attr.id, v, self.vld)?;
                    Ok(if exists {
                        None
                    } else if !eliminate_indices.is_empty() {
                        Some(Tuple(
                            tuple
                                .0
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec(),
                        ))
                    } else {
                        Some(tuple)
                    })
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn ev_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [b, b] actually a filter
        let it = left_iter
            .map_ok(move |tuple| -> Result<Option<Tuple>> {
                let eid = tuple
                    .0
                    .get(left_e_idx)
                    .unwrap()
                    .get_entity_id()
                    .with_context(|| format!("{:?}", self))?;
                let v = tuple.0.get(left_v_idx).unwrap();
                let exists = tx.eav_exists(eid, self.attr.id, v, self.vld)?;
                if exists {
                    let v = v.clone();
                    let mut ret = tuple.0;
                    ret.push(eid.as_datavalue());
                    ret.push(v);
                    Ok(Some(Tuple(ret)))
                } else {
                    Ok(None)
                }
            })
            .map(flatten_err)
            .filter_map(invert_option_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn neg_e_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let eid = tuple
                        .0
                        .get(left_e_idx)
                        .unwrap()
                        .get_entity_id()
                        .with_context(|| format!("{:?}, {:?}", self, tuple))?;
                    let nxt = if self.attr.with_history {
                        tx.triple_ea_before_scan(eid, self.attr.id, self.vld).next()
                    } else {
                        tx.triple_ea_scan(eid, self.attr.id).next()
                    };
                    match nxt {
                        None => Ok(if !eliminate_indices.is_empty() {
                            Some(Tuple(
                                tuple
                                    .0
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec(),
                            ))
                        } else {
                            Some(tuple)
                        }),
                        Some(Ok(_)) => Ok(None),
                        Some(Err(e)) => Err(e),
                    }
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn e_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_e_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [b, f]
        let mut no_bound_found = false;
        let it = left_iter
            .map_ok(move |tuple| -> Result<_> {
                let bounds = if !self.filters.is_empty() && !no_bound_found {
                    let reduced_filters: Vec<_> = self
                        .filters
                        .iter()
                        .map(|f| f.eval_bound(&tuple))
                        .try_collect()
                        .unwrap();
                    match compute_bounds(&reduced_filters, &self.bindings[1..]) {
                        Ok((l_bound, r_bound)) => {
                            let l_bound = l_bound.into_iter().next().unwrap();
                            let r_bound = r_bound.into_iter().next().unwrap();
                            if l_bound != DataValue::Null || r_bound != DataValue::Bottom {
                                Some((l_bound, r_bound))
                            } else {
                                None
                            }
                        }
                        Err(e) => {
                            error!("internel error {}", e);
                            no_bound_found = true;
                            None
                        }
                    }
                } else {
                    None
                };
                let eid = tuple
                    .0
                    .get(left_e_idx)
                    .unwrap()
                    .get_entity_id()
                    .with_context(|| format!("{:?}, {:?}, {}", self, tuple, left_e_idx))?;

                let clj = move |(eid, _, val): (EntityId, AttrId, DataValue)| {
                    let mut ret = tuple.0.clone();
                    ret.push(eid.as_datavalue());
                    ret.push(val);
                    Tuple(ret)
                };
                Ok(if let Some((l_bound, r_bound)) = bounds {
                    Left(if self.attr.with_history {
                        Left(
                            tx.triple_ea_range_before_scan(
                                eid,
                                self.attr.id,
                                l_bound,
                                r_bound,
                                self.vld,
                            )
                            .map_ok(clj),
                        )
                    } else {
                        Right(
                            tx.triple_ea_range_scan(eid, self.attr.id, l_bound, r_bound)
                                .map_ok(clj),
                        )
                    })
                } else {
                    Right(if self.attr.with_history {
                        Left(
                            tx.triple_ea_before_scan(eid, self.attr.id, self.vld)
                                .map_ok(clj),
                        )
                    } else {
                        Right(tx.triple_ea_scan(eid, self.attr.id).map_ok(clj))
                    })
                })
            })
            .map(flatten_err)
            .flatten_ok()
            .map(flatten_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn neg_v_ref_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let v_eid = tuple
                        .0
                        .get(left_v_idx)
                        .unwrap()
                        .get_entity_id()
                        .with_context(|| format!("{:?}", self))?;
                    let nxt = if self.attr.with_history {
                        tx.triple_vref_a_before_scan(v_eid, self.attr.id, self.vld)
                            .next()
                    } else {
                        tx.triple_vref_a_scan(v_eid, self.attr.id).next()
                    };
                    match nxt {
                        None => Ok(if !eliminate_indices.is_empty() {
                            Some(Tuple(
                                tuple
                                    .0
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec(),
                            ))
                        } else {
                            Some(tuple)
                        }),
                        Some(Ok(_)) => Ok(None),
                        Some(Err(e)) => Err(e),
                    }
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn v_ref_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [f, b] where b is a ref
        let it = left_iter
            .map_ok(move |tuple| {
                tuple
                    .0
                    .get(left_v_idx)
                    .unwrap()
                    .get_entity_id()
                    .with_context(|| format!("{:?}", self))
                    .map(move |v_eid| {
                        if self.attr.with_history {
                            Left(
                                tx.triple_vref_a_before_scan(v_eid, self.attr.id, self.vld)
                                    .map_ok(move |(_, _, e_id)| {
                                        let mut ret = tuple.0.clone();
                                        ret.push(e_id.as_datavalue());
                                        ret.push(v_eid.as_datavalue());
                                        Tuple(ret)
                                    }),
                            )
                        } else {
                            Right(tx.triple_vref_a_scan(v_eid, self.attr.id).map_ok(
                                move |(_, _, e_id)| {
                                    let mut ret = tuple.0.clone();
                                    ret.push(e_id.as_datavalue());
                                    ret.push(v_eid.as_datavalue());
                                    Tuple(ret)
                                },
                            ))
                        }
                    })
            })
            .map(flatten_err)
            .flatten_ok()
            .map(flatten_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn neg_v_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let val = tuple.0.get(left_v_idx).unwrap();
                    let nxt = if self.attr.with_history {
                        tx.triple_av_before_scan(self.attr.id, val, self.vld).next()
                    } else {
                        tx.triple_av_scan(self.attr.id, val).next()
                    };
                    match nxt {
                        None => Ok(if !eliminate_indices.is_empty() {
                            Some(Tuple(
                                tuple
                                    .0
                                    .into_iter()
                                    .enumerate()
                                    .filter_map(|(i, v)| {
                                        if eliminate_indices.contains(&i) {
                                            None
                                        } else {
                                            Some(v)
                                        }
                                    })
                                    .collect_vec(),
                            ))
                        } else {
                            Some(tuple)
                        }),
                        Some(Ok(_)) => Ok(None),
                        Some(Err(e)) => Err(e),
                    }
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn v_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [f, b] where b is indexed
        let it = left_iter
            .map_ok(move |tuple| {
                let val = tuple.0.get(left_v_idx).unwrap();
                if self.attr.with_history {
                    Left(
                        tx.triple_av_before_scan(self.attr.id, val, self.vld)
                            .map_ok(move |(_, val, eid): (AttrId, DataValue, EntityId)| {
                                let mut ret = tuple.0.clone();
                                ret.push(eid.as_datavalue());
                                ret.push(val);
                                Tuple(ret)
                            }),
                    )
                } else {
                    Right(tx.triple_av_scan(self.attr.id, val).map_ok(
                        move |(_, val, eid): (AttrId, DataValue, EntityId)| {
                            let mut ret = tuple.0.clone();
                            ret.push(eid.as_datavalue());
                            ret.push(val);
                            Tuple(ret)
                        },
                    ))
                }
            })
            .flatten_ok()
            .map(flatten_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn neg_v_no_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let val = tuple.0.get(left_v_idx).unwrap();
                    let it = if self.attr.with_history {
                        Left(tx.triple_a_before_scan(self.attr.id, self.vld))
                    } else {
                        Right(tx.triple_a_scan(self.attr.id))
                    };
                    for item in it {
                        let (_, _, found_val) = item?;
                        if *val == found_val {
                            return Ok(None);
                        }
                    }
                    Ok(if !eliminate_indices.is_empty() {
                        Some(Tuple(
                            tuple
                                .0
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec(),
                        ))
                    } else {
                        Some(tuple)
                    })
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn v_no_index_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        left_v_idx: usize,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        // [f, b] where b is not indexed
        let throwaway = tx.new_temp_store();
        let it = if self.attr.with_history {
            Left(tx.triple_a_before_scan(self.attr.id, self.vld))
        } else {
            Right(tx.triple_a_scan(self.attr.id))
        };
        for item in it {
            match item {
                Err(e) => return Ok(Box::new([Err(e)].into_iter())),
                Ok((_, eid, val)) => {
                    let t = Tuple(vec![val, eid.as_datavalue()]);
                    throwaway.put(t, 0);
                }
            }
        }
        let it = left_iter
            .map_ok(move |tuple| {
                let val = tuple.0.get(left_v_idx).unwrap();
                let prefix = Tuple(vec![val.clone()]);
                throwaway
                    .scan_prefix(&prefix)
                    .map_ok(move |Tuple(mut found)| {
                        let v_eid = found.pop().unwrap();
                        let mut ret = tuple.0.clone();
                        ret.push(v_eid);
                        Tuple(ret)
                    })
            })
            .flatten_ok()
            .map(flatten_err);
        self.return_filtered_iter(it, eliminate_indices)
    }
    fn return_filtered_iter<'a>(
        &'a self,
        it: impl Iterator<Item = Result<Tuple>> + 'a,
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        Ok(
            match (self.filters.is_empty(), eliminate_indices.is_empty()) {
                (true, true) => Box::new(it),
                (true, false) => {
                    Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
                }
                (false, true) => Box::new(filter_iter(self.filters.clone(), it)),
                (false, false) => Box::new(
                    filter_iter(self.filters.clone(), it)
                        .map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)),
                ),
            },
        )
    }
}

fn get_eliminate_indices(bindings: &[Symbol], eliminate: &BTreeSet<Symbol>) -> BTreeSet<usize> {
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
pub(crate) struct ViewRelation {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) storage: ViewRelStore,
    pub(crate) filters: Vec<Expr>,
}

impl ViewRelation {
    fn fill_binding_indices(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
        }
        Ok(())
    }

    fn prefix_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        let mut right_invert_indices = right_join_indices.iter().enumerate().collect_vec();
        right_invert_indices.sort_by_key(|(_, b)| **b);
        let left_to_prefix_indices = right_invert_indices
            .into_iter()
            .map(|(a, _)| left_join_indices[a])
            .collect_vec();

        let mut skip_range_check = false;
        let it = left_iter
            .map_ok(move |tuple| {
                let prefix = Tuple(
                    left_to_prefix_indices
                        .iter()
                        .map(|i| tuple.0[*i].clone())
                        .collect_vec(),
                );

                if !skip_range_check && !self.filters.is_empty() {
                    let other_bindings = &self.bindings[right_join_indices.len()..];
                    let (l_bound, u_bound) = match compute_bounds(&self.filters, other_bindings) {
                        Ok(b) => b,
                        _ => (vec![], vec![]),
                    };
                    if !l_bound.iter().all(|v| *v == DataValue::Null)
                        || !u_bound.iter().all(|v| *v == DataValue::Bottom)
                    {
                        return Left(
                            self.storage
                                .scan_bounded_prefix(&prefix, &l_bound, &u_bound)
                                .filter_map_ok(move |found| {
                                    // dbg!("filter", &tuple, &prefix, &found);
                                    let mut ret = tuple.0.clone();
                                    ret.extend(found.0);
                                    Some(Tuple(ret))
                                }),
                        );
                    }
                }
                skip_range_check = true;
                Right(
                    self.storage
                        .scan_prefix(&prefix)
                        .filter_map_ok(move |found| {
                            // dbg!("filter", &tuple, &prefix, &found);
                            let mut ret = tuple.0.clone();
                            ret.extend(found.0);
                            Some(Tuple(ret))
                        }),
                )
            })
            .flatten_ok()
            .map(flatten_err);
        Ok(
            match (self.filters.is_empty(), eliminate_indices.is_empty()) {
                (true, true) => Box::new(it),
                (true, false) => {
                    Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
                }
                (false, true) => Box::new(filter_iter(self.filters.clone(), it)),
                (false, false) => Box::new(
                    filter_iter(self.filters.clone(), it)
                        .map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)),
                ),
            },
        )
    }

    fn neg_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        debug_assert!(!right_join_indices.is_empty());
        let mut right_invert_indices = right_join_indices.iter().enumerate().collect_vec();
        right_invert_indices.sort_by_key(|(_, b)| **b);
        let mut left_to_prefix_indices = vec![];
        for (ord, (idx, ord_sorted)) in right_invert_indices.iter().enumerate() {
            if ord != **ord_sorted {
                break;
            }
            left_to_prefix_indices.push(left_join_indices[*idx]);
        }

        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let prefix = Tuple(
                        left_to_prefix_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );

                    'outer: for found in self.storage.scan_prefix(&prefix) {
                        let found = found?;
                        for (left_idx, right_idx) in
                            left_join_indices.iter().zip(right_join_indices.iter())
                        {
                            if tuple.0[*left_idx] != found.0[*right_idx] {
                                continue 'outer;
                            }
                        }
                        return Ok(None);
                    }
                    Ok(Some(if !eliminate_indices.is_empty() {
                        Tuple(
                            tuple
                                .0
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec(),
                        )
                    } else {
                        tuple
                    }))
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }

    fn iter(&self) -> Result<TupleIter<'_>> {
        let it = self.storage.scan_all()?;
        Ok(if self.filters.is_empty() {
            Box::new(it)
        } else {
            Box::new(filter_iter(self.filters.clone(), it))
        })
    }
    fn join_is_prefix(&self, right_join_indices: &[usize]) -> bool {
        let mut indices = right_join_indices.to_vec();
        indices.sort();
        let l = indices.len();
        indices.into_iter().eq(0..l)
    }
}

#[derive(Debug)]
pub(crate) struct DerivedRelation {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) storage: DerivedRelStore,
    pub(crate) filters: Vec<Expr>,
}

impl DerivedRelation {
    fn fill_binding_indices(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
        }
        Ok(())
    }

    fn fill_join_binding_indices(&mut self, bindings: &[Symbol]) -> Result<()> {
        let bindings: BTreeMap<_, _> = bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
        }
        Ok(())
    }

    fn iter(
        &self,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'_>> {
        if epoch == Some(0) && use_delta.contains(&self.storage.id) {
            return Ok(Box::new(iter::empty()));
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
        let it = self.storage.scan_all_for_epoch(scan_epoch);
        Ok(if self.filters.is_empty() {
            Box::new(it)
        } else {
            Box::new(filter_iter(self.filters.clone(), it))
        })
    }
    fn join_is_prefix(&self, right_join_indices: &[usize]) -> bool {
        let mut indices = right_join_indices.to_vec();
        indices.sort();
        let l = indices.len();
        indices.into_iter().eq(0..l)
    }
    fn neg_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
    ) -> Result<TupleIter<'a>> {
        debug_assert!(!right_join_indices.is_empty());
        let mut right_invert_indices = right_join_indices.iter().enumerate().collect_vec();
        right_invert_indices.sort_by_key(|(_, b)| **b);
        let mut left_to_prefix_indices = vec![];
        for (ord, (idx, ord_sorted)) in right_invert_indices.iter().enumerate() {
            if ord != **ord_sorted {
                break;
            }
            left_to_prefix_indices.push(left_join_indices[*idx]);
        }

        Ok(Box::new(
            left_iter
                .map_ok(move |tuple| -> Result<Option<Tuple>> {
                    let prefix = Tuple(
                        left_to_prefix_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );

                    'outer: for found in self.storage.scan_prefix(&prefix) {
                        let found = found?;
                        for (left_idx, right_idx) in
                            left_join_indices.iter().zip(right_join_indices.iter())
                        {
                            if tuple.0[*left_idx] != found.0[*right_idx] {
                                continue 'outer;
                            }
                        }
                        return Ok(None);
                    }
                    Ok(Some(if !eliminate_indices.is_empty() {
                        Tuple(
                            tuple
                                .0
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec(),
                        )
                    } else {
                        tuple
                    }))
                })
                .map(flatten_err)
                .filter_map(invert_option_err),
        ))
    }
    fn prefix_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        if epoch == Some(0) && use_delta.contains(&self.storage.id) {
            return Ok(Box::new(iter::empty()));
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
        let mut skip_range_check = false;
        let it = left_iter
            .map_ok(move |tuple| {
                let prefix = Tuple(
                    left_to_prefix_indices
                        .iter()
                        .map(|i| tuple.0[*i].clone())
                        .collect_vec(),
                );

                if !skip_range_check && !self.filters.is_empty() {
                    let other_bindings = &self.bindings[right_join_indices.len()..];
                    let (l_bound, u_bound) = match compute_bounds(&self.filters, other_bindings) {
                        Ok(b) => b,
                        _ => (vec![], vec![]),
                    };
                    if !l_bound.iter().all(|v| *v == DataValue::Null)
                        || !u_bound.iter().all(|v| *v == DataValue::Bottom)
                    {
                        return Left(
                            self.storage
                                .scan_bounded_prefix_for_epoch(
                                    &prefix, &l_bound, &u_bound, scan_epoch,
                                )
                                .filter_map_ok(move |found| {
                                    // dbg!("filter", &tuple, &prefix, &found);
                                    let mut ret = tuple.0.clone();
                                    ret.extend(found.0);
                                    Some(Tuple(ret))
                                }),
                        );
                    }
                }
                skip_range_check = true;
                Right(
                    self.storage
                        .scan_prefix_for_epoch(&prefix, scan_epoch)
                        .filter_map_ok(move |found| {
                            // dbg!("filter", &tuple, &prefix, &found);
                            let mut ret = tuple.0.clone();
                            ret.extend(found.0);
                            Some(Tuple(ret))
                        }),
                )
            })
            .flatten_ok()
            .map(flatten_err);
        Ok(
            match (self.filters.is_empty(), eliminate_indices.is_empty()) {
                (true, true) => Box::new(it),
                (true, false) => {
                    Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
                }
                (false, true) => Box::new(filter_iter(self.filters.clone(), it)),
                (false, false) => Box::new(
                    filter_iter(self.filters.clone(), it)
                        .map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)),
                ),
            },
        )
    }
}

pub(crate) struct Joiner {
    // invariant: these are of the same lengths
    pub(crate) left_keys: Vec<Symbol>,
    pub(crate) right_keys: Vec<Symbol>,
}

impl Debug for Joiner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let left_bindings = BindingFormatter(self.left_keys.clone());
        let right_bindings = BindingFormatter(self.right_keys.clone());
        write!(f, "{:?}<->{:?}", left_bindings, right_bindings,)
    }
}

impl Joiner {
    pub(crate) fn join_indices(
        &self,
        left_bindings: &[Symbol],
        right_bindings: &[Symbol],
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
                    bail!("join key is wrong: left binding for {} not found: left {:?} vs right {:?}, {:?}",
                        l, left_bindings, right_bindings, self);
                }
                Some(p) => p,
            };
            let r_pos = match right_binding_map.get(r) {
                None => {
                    bail!("join key is wrong: right binding for {} not found: left {:?} vs right {:?}, {:?}",
                        r, left_bindings, right_bindings, self);
                }
                Some(p) => p,
            };
            ret_l.push(*l_pos);
            ret_r.push(*r_pos)
        }
        Ok((ret_l, ret_r))
    }
}

impl Relation {
    pub(crate) fn eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        match self {
            Relation::Fixed(r) => r.do_eliminate_temp_vars(used),
            Relation::Triple(_r) => Ok(()),
            Relation::Derived(_r) => Ok(()),
            Relation::View(_v) => Ok(()),
            Relation::Join(r) => r.do_eliminate_temp_vars(used),
            Relation::Reorder(r) => r.relation.eliminate_temp_vars(used),
            Relation::Filter(r) => r.do_eliminate_temp_vars(used),
            Relation::NegJoin(r) => r.do_eliminate_temp_vars(used),
            Relation::Unification(r) => r.do_eliminate_temp_vars(used),
        }
    }

    fn eliminate_set(&self) -> Option<&BTreeSet<Symbol>> {
        match self {
            Relation::Fixed(r) => Some(&r.to_eliminate),
            Relation::Triple(_) => None,
            Relation::Derived(_) => None,
            Relation::View(_) => None,
            Relation::Join(r) => Some(&r.to_eliminate),
            Relation::Reorder(_) => None,
            Relation::Filter(r) => Some(&r.to_eliminate),
            Relation::NegJoin(r) => Some(&r.to_eliminate),
            Relation::Unification(u) => Some(&u.to_eliminate),
        }
    }

    pub(crate) fn bindings_after_eliminate(&self) -> Vec<Symbol> {
        let ret = self.bindings_before_eliminate();
        if let Some(to_eliminate) = self.eliminate_set() {
            ret.into_iter()
                .filter(|kw| !to_eliminate.contains(kw))
                .collect()
        } else {
            ret
        }
    }

    fn bindings_before_eliminate(&self) -> Vec<Symbol> {
        match self {
            Relation::Fixed(f) => f.bindings.clone(),
            Relation::Triple(t) => t.bindings.to_vec(),
            Relation::Derived(d) => d.bindings.clone(),
            Relation::View(v) => v.bindings.clone(),
            Relation::Join(j) => j.bindings(),
            Relation::Reorder(r) => r.bindings(),
            Relation::Filter(r) => r.parent.bindings_after_eliminate(),
            Relation::NegJoin(j) => j.left.bindings_after_eliminate(),
            Relation::Unification(u) => {
                let mut bindings = u.parent.bindings_after_eliminate();
                bindings.push(u.binding.clone());
                bindings
            }
        }
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        match self {
            Relation::Fixed(f) => Ok(Box::new(f.data.iter().map(|t| Ok(Tuple(t.clone()))))),
            Relation::Triple(r) => r.iter(tx),
            Relation::Derived(r) => r.iter(epoch, use_delta),
            Relation::View(v) => v.iter(),
            Relation::Join(j) => j.iter(tx, epoch, use_delta),
            Relation::Reorder(r) => r.iter(tx, epoch, use_delta),
            Relation::Filter(r) => r.iter(tx, epoch, use_delta),
            Relation::NegJoin(r) => r.iter(tx, epoch, use_delta),
            Relation::Unification(r) => r.iter(tx, epoch, use_delta),
        }
    }
}

#[derive(Debug)]
pub(crate) struct NegJoin {
    pub(crate) left: Relation,
    pub(crate) right: Relation,
    pub(crate) joiner: Joiner,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
}

impl NegJoin {
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.left.bindings_after_eliminate() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut left = used.clone();
        left.extend(self.joiner.left_keys.clone());
        self.left.eliminate_temp_vars(&left)?;
        // right acts as a filter, introduces nothing, no need to eliminate
        Ok(())
    }

    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.left.bindings_after_eliminate();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        match &self.right {
            Relation::Triple(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                r.neg_join(
                    self.left.iter(tx, epoch, use_delta)?,
                    join_indices,
                    tx,
                    eliminate_indices,
                )
            }
            Relation::Derived(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                r.neg_join(
                    self.left.iter(tx, epoch, use_delta)?,
                    join_indices,
                    eliminate_indices,
                )
            }
            Relation::View(v) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                v.neg_join(
                    self.left.iter(tx, epoch, use_delta)?,
                    join_indices,
                    eliminate_indices,
                )
            }
            _ => {
                unreachable!()
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct InnerJoin {
    pub(crate) left: Relation,
    pub(crate) right: Relation,
    pub(crate) joiner: Joiner,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
}

impl InnerJoin {
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.bindings() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut left = used.clone();
        left.extend(self.joiner.left_keys.clone());
        if let Some(filters) = match &self.right {
            Relation::Triple(r) => Some(&r.filters),
            Relation::Derived(r) => Some(&r.filters),
            _ => None,
        } {
            for filter in filters {
                left.extend(filter.bindings());
            }
        }
        self.left.eliminate_temp_vars(&left)?;
        let mut right = used.clone();
        right.extend(self.joiner.right_keys.clone());
        self.right.eliminate_temp_vars(&right)?;
        Ok(())
    }

    pub(crate) fn bindings(&self) -> Vec<Symbol> {
        let mut ret = self.left.bindings_after_eliminate();
        ret.extend(self.right.bindings_after_eliminate());
        debug_assert_eq!(ret.len(), ret.iter().collect::<BTreeSet<_>>().len());
        ret
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.bindings();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        match &self.right {
            Relation::Fixed(f) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                f.join(
                    self.left.iter(tx, epoch, use_delta)?,
                    join_indices,
                    eliminate_indices,
                )
            }
            Relation::Triple(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                r.join(
                    self.left.iter(tx, epoch, use_delta)?,
                    join_indices,
                    tx,
                    eliminate_indices,
                )
            }
            Relation::Derived(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if r.join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        self.left.iter(tx, epoch, use_delta)?,
                        join_indices,
                        eliminate_indices,
                        epoch,
                        use_delta,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, epoch, use_delta)
                }
            }
            Relation::View(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if r.join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        self.left.iter(tx, epoch, use_delta)?,
                        join_indices,
                        eliminate_indices,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, epoch, use_delta)
                }
            }
            Relation::Join(_) | Relation::Filter(_) | Relation::Unification(_) => {
                self.materialized_join(tx, eliminate_indices, epoch, use_delta)
            }
            Relation::Reorder(_) => {
                panic!("joining on reordered")
            }
            Relation::NegJoin(_) => {
                panic!("joining on NegJoin")
            }
        }
    }
    fn materialized_join<'a>(
        &'a self,
        tx: &'a SessionTx,
        eliminate_indices: BTreeSet<usize>,
        epoch: Option<u32>,
        use_delta: &BTreeSet<DerivedRelStoreId>,
    ) -> Result<TupleIter<'a>> {
        let right_bindings = self.right.bindings_after_eliminate();
        let (left_join_indices, right_join_indices) = self
            .joiner
            .join_indices(&self.left.bindings_after_eliminate(), &right_bindings)
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
        let throwaway = tx.new_temp_store();
        for item in self.right.iter(tx, epoch, use_delta)? {
            match item {
                Ok(tuple) => {
                    let stored_tuple = Tuple(
                        right_store_indices
                            .iter()
                            .map(|i| tuple.0[*i].clone())
                            .collect_vec(),
                    );
                    throwaway.put(stored_tuple, 0);
                }
                Err(e) => return Ok(Box::new([Err(e)].into_iter())),
            }
        }
        Ok(Box::new(
            self.left
                .iter(tx, epoch, use_delta)?
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
                        eliminate_from_tuple(Tuple(ret), &eliminate_indices)
                    })
                })
                .flatten_ok()
                .map(flatten_err),
        ))
    }
}
