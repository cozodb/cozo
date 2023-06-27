/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter, Write};
use std::iter;

use either::{Left, Right};
use itertools::Itertools;
use log::{debug, error};
use miette::{bail, Diagnostic, Result};
use smartstring::SmartString;
use thiserror::Error;

use crate::data::expr::{compute_bounds, eval_bytecode, eval_bytecode_pred, Bytecode, Expr};
use crate::data::program::{FtsSearch, HnswSearch, MagicSymbol};
use crate::data::relation::{ColType, NullableColType};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::{DataValue, ValidityTs};
use crate::parse::SourceSpan;
use crate::runtime::minhash_lsh::LshSearch;
use crate::runtime::relation::RelationHandle;
use crate::runtime::temp_store::EpochStore;
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

pub(crate) enum RelAlgebra {
    Fixed(InlineFixedRA),
    TempStore(TempStoreRA),
    Stored(StoredRA),
    StoredWithValidity(StoredWithValidityRA),
    Join(Box<InnerJoin>),
    NegJoin(Box<NegJoin>),
    Reorder(ReorderRA),
    Filter(FilteredRA),
    Unification(UnificationRA),
    HnswSearch(HnswSearchRA),
    FtsSearch(FtsSearchRA),
    LshSearch(LshSearchRA),
}

impl RelAlgebra {
    pub(crate) fn span(&self) -> SourceSpan {
        match self {
            RelAlgebra::Fixed(i) => i.span,
            RelAlgebra::TempStore(i) => i.span,
            RelAlgebra::Stored(i) => i.span,
            RelAlgebra::Join(i) => i.span,
            RelAlgebra::NegJoin(i) => i.span,
            RelAlgebra::Reorder(i) => i.relation.span(),
            RelAlgebra::Filter(i) => i.span,
            RelAlgebra::Unification(i) => i.span,
            RelAlgebra::StoredWithValidity(i) => i.span,
            RelAlgebra::HnswSearch(i) => i.hnsw_search.span,
            RelAlgebra::FtsSearch(i) => i.fts_search.span,
            RelAlgebra::LshSearch(i) => i.lsh_search.span,
        }
    }
}

pub(crate) struct UnificationRA {
    pub(crate) parent: Box<RelAlgebra>,
    pub(crate) binding: Symbol,
    pub(crate) expr: Expr,
    pub(crate) expr_bytecode: Vec<Bytecode>,
    pub(crate) is_multi: bool,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug, Error, Diagnostic)]
#[error("Found value {0:?} while iterating, unacceptable for an Entity ID")]
#[diagnostic(code(eval::iter_bad_entity_id))]
struct EntityIdExpected(DataValue, #[label] SourceSpan);

fn eliminate_from_tuple(mut ret: Tuple, eliminate_indices: &BTreeSet<usize>) -> Tuple {
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
    ret
}

impl UnificationRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        let parent_bindings: BTreeMap<_, _> = self
            .parent
            .bindings_after_eliminate()
            .into_iter()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        self.expr.fill_binding_indices(&parent_bindings)?;
        self.expr_bytecode = self.expr.compile()?;
        Ok(())
    }
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.parent.bindings_before_eliminate() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut nxt = used.clone();
        nxt.extend(self.expr.bindings()?);
        self.parent.eliminate_temp_vars(&nxt)?;
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let mut bindings = self.parent.bindings_after_eliminate();
        bindings.push(self.binding.clone());
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        let mut stack = vec![];
        Ok(if self.is_multi {
            let it = self
                .parent
                .iter(tx, delta_rule, stores)?
                .map_ok(move |tuple| -> Result<Vec<Tuple>> {
                    let result_list = eval_bytecode(&self.expr_bytecode, &tuple, &mut stack)?;
                    let result_list = result_list.get_slice().ok_or_else(|| {
                        #[derive(Debug, Error, Diagnostic)]
                        #[error("Invalid spread unification")]
                        #[diagnostic(code(eval::invalid_spread_unif))]
                        #[diagnostic(help("Spread unification requires a list at the right"))]
                        struct BadSpreadUnification(#[label] SourceSpan);

                        BadSpreadUnification(self.span)
                    })?;
                    let mut coll = vec![];
                    for result in result_list {
                        let mut ret = tuple.clone();
                        ret.push(result.clone());
                        let ret = ret;
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
                    .iter(tx, delta_rule, stores)?
                    .map_ok(move |tuple| -> Result<Tuple> {
                        let result = eval_bytecode(&self.expr_bytecode, &tuple, &mut stack)?;
                        let mut ret = tuple;
                        ret.push(result);
                        let ret = ret;
                        let ret = eliminate_from_tuple(ret, &eliminate_indices);
                        Ok(ret)
                    })
                    .map(flatten_err),
            )
        })
    }
}

pub(crate) struct FilteredRA {
    pub(crate) parent: Box<RelAlgebra>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) filters_bytecodes: Vec<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
    pub(crate) span: SourceSpan,
}

impl FilteredRA {
    pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        for binding in self.parent.bindings_before_eliminate() {
            if !used.contains(&binding) {
                self.to_eliminate.insert(binding.clone());
            }
        }
        let mut nxt = used.clone();
        for e in self.filters.iter() {
            nxt.extend(e.bindings()?);
        }
        self.parent.eliminate_temp_vars(&nxt)?;
        Ok(())
    }

    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        let parent_bindings: BTreeMap<_, _> = self
            .parent
            .bindings_after_eliminate()
            .into_iter()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&parent_bindings)?;
            self.filters_bytecodes.push((e.compile()?, e.span()));
        }
        Ok(())
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.parent.bindings_after_eliminate();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        let mut stack = vec![];
        Ok(Box::new(
            self.parent
                .iter(tx, delta_rule, stores)?
                .filter_map(move |tuple| match tuple {
                    Ok(t) => {
                        for (p, span) in self.filters_bytecodes.iter() {
                            match eval_bytecode_pred(p, &t, &mut stack, *span) {
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
        write!(f, "[{s}]")
    }
}

impl Debug for RelAlgebra {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bindings = BindingFormatter(self.bindings_after_eliminate());
        match self {
            RelAlgebra::Fixed(r) => {
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
            RelAlgebra::TempStore(r) => f
                .debug_tuple("TempStore")
                .field(&bindings)
                .field(&r.storage_key)
                .field(&r.filters)
                .finish(),
            RelAlgebra::Stored(r) => f
                .debug_tuple("Stored")
                .field(&bindings)
                .field(&r.storage.name)
                .field(&r.filters)
                .finish(),
            RelAlgebra::HnswSearch(s) => f
                .debug_tuple("HnswSearch")
                .field(&bindings)
                .field(&s.hnsw_search.idx_handle.name)
                .finish(),
            RelAlgebra::FtsSearch(s) => f
                .debug_tuple("FtsSearch")
                .field(&bindings)
                .field(&s.fts_search.idx_handle.name)
                .finish(),
            RelAlgebra::LshSearch(s) => f
                .debug_tuple("LshSearch")
                .field(&bindings)
                .field(&s.lsh_search.idx_handle.name)
                .finish(),
            RelAlgebra::StoredWithValidity(r) => f
                .debug_tuple("StoredWithValidity")
                .field(&bindings)
                .field(&r.storage.name)
                .field(&r.filters)
                .field(&r.valid_at)
                .finish(),
            RelAlgebra::Join(r) => {
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
            RelAlgebra::NegJoin(r) => f
                .debug_tuple("NegJoin")
                .field(&bindings)
                .field(&r.joiner)
                .field(&r.left)
                .field(&r.right)
                .finish(),
            RelAlgebra::Reorder(r) => f
                .debug_tuple("Reorder")
                .field(&r.new_order)
                .field(&r.relation)
                .finish(),
            RelAlgebra::Filter(r) => f
                .debug_tuple("Filter")
                .field(&bindings)
                .field(&r.filters)
                .field(&r.parent)
                .finish(),
            RelAlgebra::Unification(r) => f
                .debug_tuple("Unify")
                .field(&bindings)
                .field(&r.parent)
                .field(&r.binding)
                .field(&r.expr)
                .finish(),
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Invalid time travel on relation {0}")]
#[diagnostic(code(eval::invalid_time_travel))]
#[diagnostic(help(
    "Time travel scanning requires the last key column of the relation to be of type 'Validity'"
))]
pub(crate) struct InvalidTimeTravelScanning(pub(crate) String, #[label] pub(crate) SourceSpan);

impl RelAlgebra {
    pub(crate) fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        match self {
            RelAlgebra::Fixed(_) => {}
            RelAlgebra::TempStore(d) => {
                d.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::Stored(v) => {
                v.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::HnswSearch(s) => {
                s.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::FtsSearch(s) => {
                s.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::LshSearch(s) => {
                s.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::StoredWithValidity(v) => {
                v.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::Reorder(r) => {
                r.relation.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::Filter(f) => {
                f.parent.fill_binding_indices_and_compile()?;
                f.fill_binding_indices_and_compile()?
            }
            RelAlgebra::NegJoin(r) => {
                r.left.fill_binding_indices_and_compile()?;
            }
            RelAlgebra::Unification(u) => {
                u.parent.fill_binding_indices_and_compile()?;
                u.fill_binding_indices_and_compile()?
            }
            RelAlgebra::Join(r) => {
                r.left.fill_binding_indices_and_compile()?;
                r.right.fill_binding_indices_and_compile()?;
            }
        }
        Ok(())
    }
    pub(crate) fn unit(span: SourceSpan) -> Self {
        Self::Fixed(InlineFixedRA::unit(span))
    }
    pub(crate) fn is_unit(&self) -> bool {
        if let RelAlgebra::Fixed(r) = self {
            r.bindings.is_empty() && r.data.len() == 1
        } else {
            false
        }
    }
    pub(crate) fn cartesian_join(self, right: RelAlgebra, span: SourceSpan) -> Self {
        self.join(right, vec![], vec![], span)
    }
    pub(crate) fn derived(
        bindings: Vec<Symbol>,
        storage_key: MagicSymbol,
        span: SourceSpan,
    ) -> Self {
        Self::TempStore(TempStoreRA {
            bindings,
            storage_key,
            filters: vec![],
            filters_bytecodes: vec![],
            span,
        })
    }
    pub(crate) fn relation(
        bindings: Vec<Symbol>,
        storage: RelationHandle,
        span: SourceSpan,
        validity: Option<ValidityTs>,
    ) -> Result<Self> {
        match validity {
            None => Ok(Self::Stored(StoredRA {
                bindings,
                storage,
                filters: vec![],
                filters_bytecodes: vec![],
                span,
            })),
            Some(vld) => {
                if storage.metadata.keys.last().unwrap().typing
                    != (NullableColType {
                        coltype: ColType::Validity,
                        nullable: false,
                    })
                {
                    bail!(InvalidTimeTravelScanning(storage.name.to_string(), span));
                };
                Ok(Self::StoredWithValidity(StoredWithValidityRA {
                    bindings,
                    storage,
                    filters: vec![],
                    filters_bytecodes: vec![],
                    valid_at: vld,
                    span,
                }))
            }
        }
    }
    pub(crate) fn reorder(self, new_order: Vec<Symbol>) -> Self {
        Self::Reorder(ReorderRA {
            relation: Box::new(self),
            new_order,
        })
    }
    pub(crate) fn filter(self, filter: Expr) -> Result<Self> {
        Ok(match self {
            s @ (RelAlgebra::Fixed(_)
            | RelAlgebra::Reorder(_)
            | RelAlgebra::NegJoin(_)
            | RelAlgebra::Unification(_)
            | RelAlgebra::HnswSearch(_)
            | RelAlgebra::FtsSearch(_)
            | RelAlgebra::LshSearch(_)) => {
                let span = filter.span();
                RelAlgebra::Filter(FilteredRA {
                    parent: Box::new(s),
                    filters: vec![filter],
                    filters_bytecodes: vec![],
                    to_eliminate: Default::default(),
                    span,
                })
            }
            RelAlgebra::Filter(FilteredRA {
                parent,
                filters: mut pred,
                filters_bytecodes,
                to_eliminate,
                span,
            }) => {
                pred.push(filter);
                RelAlgebra::Filter(FilteredRA {
                    parent,
                    filters: pred,
                    filters_bytecodes,
                    to_eliminate,
                    span,
                })
            }
            RelAlgebra::TempStore(TempStoreRA {
                bindings,
                storage_key,
                mut filters,
                filters_bytecodes: filters_asm,
                span,
            }) => {
                filters.push(filter);
                RelAlgebra::TempStore(TempStoreRA {
                    bindings,
                    storage_key,
                    filters,
                    filters_bytecodes: filters_asm,
                    span,
                })
            }
            RelAlgebra::Stored(StoredRA {
                bindings,
                storage,
                mut filters,
                filters_bytecodes,
                span,
            }) => {
                filters.push(filter);
                RelAlgebra::Stored(StoredRA {
                    bindings,
                    storage,
                    filters,
                    filters_bytecodes,
                    span,
                })
            }
            RelAlgebra::StoredWithValidity(StoredWithValidityRA {
                bindings,
                storage,
                mut filters,
                filters_bytecodes: filter_bytecodes,
                span,
                valid_at,
            }) => {
                filters.push(filter);
                RelAlgebra::StoredWithValidity(StoredWithValidityRA {
                    bindings,
                    storage,
                    filters,
                    span,
                    valid_at,
                    filters_bytecodes: filter_bytecodes,
                })
            }
            RelAlgebra::Join(inner) => {
                let filters = filter.to_conjunction();
                let left_bindings: BTreeSet<Symbol> =
                    inner.left.bindings_before_eliminate().into_iter().collect();
                let right_bindings: BTreeSet<Symbol> = inner
                    .right
                    .bindings_before_eliminate()
                    .into_iter()
                    .collect();
                let mut remaining = vec![];
                let InnerJoin {
                    mut left,
                    mut right,
                    joiner,
                    to_eliminate,
                    span,
                    ..
                } = *inner;
                for filter in filters {
                    let f_bindings = filter.bindings()?;
                    if f_bindings.is_subset(&left_bindings) {
                        left = left.filter(filter)?;
                    } else if f_bindings.is_subset(&right_bindings) {
                        right = right.filter(filter)?;
                    } else {
                        remaining.push(filter);
                    }
                }
                let mut joined = RelAlgebra::Join(Box::new(InnerJoin {
                    left,
                    right,
                    joiner,
                    to_eliminate,
                    span,
                }));
                if !remaining.is_empty() {
                    joined = RelAlgebra::Filter(FilteredRA {
                        parent: Box::new(joined),
                        filters: remaining,
                        filters_bytecodes: vec![],
                        to_eliminate: Default::default(),
                        span,
                    });
                }
                joined
            }
        })
    }
    pub(crate) fn unify(
        self,
        binding: Symbol,
        expr: Expr,
        is_multi: bool,
        span: SourceSpan,
    ) -> Self {
        RelAlgebra::Unification(UnificationRA {
            parent: Box::new(self),
            binding,
            expr,
            expr_bytecode: vec![],
            is_multi,
            to_eliminate: Default::default(),
            span,
        })
    }
    pub(crate) fn hnsw_search(
        self,
        hnsw_search: HnswSearch,
        own_bindings: Vec<Symbol>,
    ) -> Result<Self> {
        Ok(Self::HnswSearch(HnswSearchRA {
            parent: Box::new(self),
            hnsw_search,
            filter_bytecode: None,
            own_bindings,
        }))
    }
    pub(crate) fn fts_search(
        self,
        fts_search: FtsSearch,
        own_bindings: Vec<Symbol>,
    ) -> Result<Self> {
        Ok(Self::FtsSearch(FtsSearchRA {
            parent: Box::new(self),
            fts_search,
            filter_bytecode: None,
            own_bindings,
        }))
    }
    pub(crate) fn lsh_search(
        self,
        fts_search: LshSearch,
        own_bindings: Vec<Symbol>,
    ) -> Result<Self> {
        Ok(Self::LshSearch(LshSearchRA {
            parent: Box::new(self),
            lsh_search: fts_search,
            filter_bytecode: None,
            own_bindings,
        }))
    }
    pub(crate) fn join(
        self,
        right: RelAlgebra,
        left_keys: Vec<Symbol>,
        right_keys: Vec<Symbol>,
        span: SourceSpan,
    ) -> Self {
        RelAlgebra::Join(Box::new(InnerJoin {
            left: self,
            right,
            joiner: Joiner {
                left_keys,
                right_keys,
            },
            to_eliminate: Default::default(),
            span,
        }))
    }
    pub(crate) fn neg_join(
        self,
        right: RelAlgebra,
        left_keys: Vec<Symbol>,
        right_keys: Vec<Symbol>,
        span: SourceSpan,
    ) -> Self {
        RelAlgebra::NegJoin(Box::new(NegJoin {
            left: self,
            right,
            joiner: Joiner {
                left_keys,
                right_keys,
            },
            to_eliminate: Default::default(),
            span,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct ReorderRA {
    pub(crate) relation: Box<RelAlgebra>,
    pub(crate) new_order: Vec<Symbol>,
}

impl ReorderRA {
    fn bindings(&self) -> Vec<Symbol> {
        self.new_order.clone()
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
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
        Ok(Box::new(
            self.relation
                .iter(tx, delta_rule, stores)?
                .map_ok(move |tuple| {
                    let old = tuple;
                    let new = reorder_indices
                        .iter()
                        .map(|i| old[*i].clone())
                        .collect_vec();
                    new
                }),
        ))
    }
}

#[derive(Debug)]
pub(crate) struct InlineFixedRA {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) data: Vec<Vec<DataValue>>,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
    pub(crate) span: SourceSpan,
}

impl InlineFixedRA {
    pub(crate) fn unit(span: SourceSpan) -> Self {
        Self {
            bindings: vec![],
            data: vec![vec![]],
            to_eliminate: Default::default(),
            span,
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

impl InlineFixedRA {
    pub(crate) fn join_type(&self) -> &str {
        if self.data.is_empty() {
            "null_join"
        } else if self.data.len() == 1 {
            "singleton_join"
        } else {
            "fixed_join"
        }
    }
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
                let left_join_values = left_join_indices.iter().map(|v| &tuple[*v]).collect_vec();
                if left_join_values.into_iter().eq(right_join_values.iter()) {
                    let mut ret = tuple;
                    ret.extend_from_slice(&data);
                    let ret = ret;
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
                            left_join_indices.iter().map(|v| &tuple[*v]).collect_vec();
                        right_mapping.get(&left_join_values).map(|v| {
                            v.iter()
                                .map(|right_values| {
                                    let mut left_data = tuple.clone();
                                    left_data.extend_from_slice(right_values);
                                    left_data
                                })
                                .collect_vec()
                        })
                    })
                    .flatten_ok(),
            )
        })
    }
}

pub(crate) fn flatten_err<T, E1: Into<miette::Error>, E2: Into<miette::Error>>(
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
    filters_bytecodes: Vec<(Vec<Bytecode>, SourceSpan)>,
    it: impl Iterator<Item = Result<Tuple>>,
) -> impl Iterator<Item = Result<Tuple>> {
    let mut stack = vec![];
    it.filter_map_ok(move |t| -> Option<Result<Tuple>> {
        for (p, span) in filters_bytecodes.iter() {
            match eval_bytecode_pred(p, &t, &mut stack, *span) {
                Ok(false) => return None,
                Err(e) => {
                    debug!("{:?}", t);
                    return Some(Err(e));
                }
                Ok(true) => {}
            }
        }
        Some(Ok(t))
    })
    .map(flatten_err)
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
pub(crate) struct StoredRA {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) storage: RelationHandle,
    pub(crate) filters: Vec<Expr>,
    pub(crate) filters_bytecodes: Vec<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) span: SourceSpan,
}

#[derive(Debug)]
pub(crate) struct HnswSearchRA {
    pub(crate) parent: Box<RelAlgebra>,
    pub(crate) hnsw_search: HnswSearch,
    pub(crate) filter_bytecode: Option<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) own_bindings: Vec<Symbol>,
}

#[derive(Debug)]
pub(crate) struct LshSearchRA {
    pub(crate) parent: Box<RelAlgebra>,
    pub(crate) lsh_search: LshSearch,
    pub(crate) filter_bytecode: Option<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) own_bindings: Vec<Symbol>,
}

impl LshSearchRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        self.parent.fill_binding_indices_and_compile()?;
        if self.lsh_search.filter.is_some() {
            let bindings: BTreeMap<_, _> = self
                .own_bindings
                .iter()
                .cloned()
                .enumerate()
                .map(|(a, b)| (b, a))
                .collect();
            let filter = self.lsh_search.filter.as_mut().unwrap();
            filter.fill_binding_indices(&bindings)?;
            self.filter_bytecode = Some((filter.compile()?, filter.span()));
        }
        Ok(())
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.parent.bindings_after_eliminate();
        let mut bind_idx = usize::MAX;
        for (i, b) in bindings.iter().enumerate() {
            if *b == self.lsh_search.query {
                bind_idx = i;
                break;
            }
        }
        let config = self.lsh_search.clone();
        let filter_code = self.filter_bytecode.clone();
        let mut stack = vec![];
        let perms = config.manifest.get_hash_perms();
        let tokenizer = tx.tokenizers.get(
            &config.idx_handle.name,
            &config.manifest.tokenizer,
            &config.manifest.filters,
        )?;

        let it = self
            .parent
            .iter(tx, delta_rule, stores)?
            .map_ok(move |tuple| -> Result<_> {
                let res = tx.lsh_search(
                    &tuple[bind_idx],
                    &config,
                    &mut stack,
                    &filter_code,
                    &perms,
                    &tokenizer,
                )?;
                Ok(res.into_iter().map(move |t| {
                    let mut r = tuple.clone();
                    r.extend(t);
                    r
                }))
            })
            .map(flatten_err)
            .flatten_ok();
        Ok(Box::new(it))
    }
}

#[derive(Debug)]
pub(crate) struct FtsSearchRA {
    pub(crate) parent: Box<RelAlgebra>,
    pub(crate) fts_search: FtsSearch,
    pub(crate) filter_bytecode: Option<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) own_bindings: Vec<Symbol>,
}

impl FtsSearchRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        self.parent.fill_binding_indices_and_compile()?;
        if self.fts_search.filter.is_some() {
            let bindings: BTreeMap<_, _> = self
                .own_bindings
                .iter()
                .cloned()
                .enumerate()
                .map(|(a, b)| (b, a))
                .collect();
            let filter = self.fts_search.filter.as_mut().unwrap();
            filter.fill_binding_indices(&bindings)?;
            self.filter_bytecode = Some((filter.compile()?, filter.span()));
        }
        Ok(())
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.parent.bindings_after_eliminate();
        let mut bind_idx = usize::MAX;
        for (i, b) in bindings.iter().enumerate() {
            if *b == self.fts_search.query {
                bind_idx = i;
                break;
            }
        }
        let config = self.fts_search.clone();
        let filter_code = self.filter_bytecode.clone();
        let mut stack = vec![];
        let mut idf_cache = Default::default();
        let tokenizer = tx.tokenizers.get(
            &config.idx_handle.name,
            &config.manifest.tokenizer,
            &config.manifest.filters,
        )?;
        let it = self
            .parent
            .iter(tx, delta_rule, stores)?
            .map_ok(move |tuple| -> Result<_> {
                let q = match tuple[bind_idx].clone() {
                    DataValue::Str(s) => s,
                    DataValue::List(l) => {
                        let mut coll = SmartString::new();
                        for d in l {
                            match d {
                                DataValue::Str(s) => {
                                    if !coll.is_empty() {
                                        coll.write_str(" OR ").unwrap();
                                    }
                                    coll.write_str(&s).unwrap();
                                }
                                d => bail!("Expected string for FTS search, got {:?}", d),
                            }
                        }
                        coll
                    }
                    d => bail!("Expected string for FTS search, got {:?}", d),
                };

                let res = tx.fts_search(
                    &q,
                    &config,
                    &filter_code,
                    &tokenizer,
                    &mut stack,
                    &mut idf_cache,
                )?;
                Ok(res.into_iter().map(move |t| {
                    let mut r = tuple.clone();
                    r.extend(t);
                    r
                }))
            })
            .map(flatten_err)
            .flatten_ok();
        Ok(Box::new(it))
    }
}

impl HnswSearchRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        self.parent.fill_binding_indices_and_compile()?;
        if self.hnsw_search.filter.is_some() {
            let bindings: BTreeMap<_, _> = self
                .own_bindings
                .iter()
                .cloned()
                .enumerate()
                .map(|(a, b)| (b, a))
                .collect();
            let filter = self.hnsw_search.filter.as_mut().unwrap();
            filter.fill_binding_indices(&bindings)?;
            self.filter_bytecode = Some((filter.compile()?, filter.span()));
        }
        Ok(())
    }
    fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.parent.bindings_after_eliminate();
        let mut bind_idx = usize::MAX;
        for (i, b) in bindings.iter().enumerate() {
            if *b == self.hnsw_search.query {
                bind_idx = i;
                break;
            }
        }
        let config = self.hnsw_search.clone();
        let filter_code = self.filter_bytecode.clone();
        let mut stack = vec![];
        let it = self
            .parent
            .iter(tx, delta_rule, stores)?
            .map_ok(move |tuple| -> Result<_> {
                let v = match tuple[bind_idx].clone() {
                    DataValue::Vec(v) => v,
                    d => bail!("Expected vector, got {:?}", d),
                };

                let res = tx.hnsw_knn(v, &config, &filter_code, &mut stack)?;
                Ok(res.into_iter().map(move |t| {
                    let mut r = tuple.clone();
                    r.extend(t);
                    r
                }))
            })
            .map(flatten_err)
            .flatten_ok();
        Ok(Box::new(it))
    }
}

#[derive(Debug)]
pub(crate) struct StoredWithValidityRA {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) storage: RelationHandle,
    pub(crate) filters: Vec<Expr>,
    pub(crate) filters_bytecodes: Vec<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) valid_at: ValidityTs,
    pub(crate) span: SourceSpan,
}

impl StoredWithValidityRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
            self.filters_bytecodes.push((e.compile()?, e.span()));
        }
        Ok(())
    }
    fn iter<'a>(&'a self, tx: &'a SessionTx<'_>) -> Result<TupleIter<'a>> {
        let it = self.storage.skip_scan_all(tx, self.valid_at);
        Ok(if self.filters.is_empty() {
            Box::new(it)
        } else {
            Box::new(filter_iter(self.filters_bytecodes.clone(), it))
        })
    }
    fn prefix_join<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
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
                let prefix = left_to_prefix_indices
                    .iter()
                    .map(|i| tuple[*i].clone())
                    .collect_vec();

                if !skip_range_check && !self.filters.is_empty() {
                    let other_bindings = &self.bindings[right_join_indices.len()..];
                    let (l_bound, u_bound) = match compute_bounds(&self.filters, other_bindings) {
                        Ok(b) => b,
                        _ => (vec![], vec![]),
                    };
                    if !l_bound.iter().all(|v| *v == DataValue::Null)
                        || !u_bound.iter().all(|v| *v == DataValue::Bot)
                    {
                        let mut stack = vec![];
                        return Left(
                            self.storage
                                .skip_scan_bounded_prefix(
                                    tx,
                                    &prefix,
                                    &l_bound,
                                    &u_bound,
                                    self.valid_at,
                                )
                                .map(move |res_found| -> Result<Option<Tuple>> {
                                    let found = res_found?;
                                    for (p, span) in self.filters_bytecodes.iter() {
                                        if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                            return Ok(None);
                                        }
                                    }
                                    let mut ret = tuple.clone();
                                    ret.extend(found);
                                    Ok(Some(ret))
                                })
                                .filter_map(swap_option_result),
                        );
                    }
                }
                skip_range_check = true;
                let mut stack = vec![];
                Right(
                    self.storage
                        .skip_scan_prefix(tx, &prefix, self.valid_at)
                        .map(move |res_found| -> Result<Option<Tuple>> {
                            let found = res_found?;
                            for (p, span) in self.filters_bytecodes.iter() {
                                if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                    return Ok(None);
                                }
                            }
                            let mut ret = tuple.clone();
                            ret.extend(found);
                            Ok(Some(ret))
                        })
                        .filter_map(swap_option_result),
                )
            })
            .flatten_ok()
            .map(flatten_err);
        Ok(if eliminate_indices.is_empty() {
            Box::new(it)
        } else {
            Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
        })
    }
}

impl StoredRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
            self.filters_bytecodes.push((e.compile()?, e.span()));
        }
        Ok(())
    }

    fn point_lookup_join<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        left_iter: TupleIter<'a>,
        key_len: usize,
        left_to_prefix_indices: Vec<usize>,
        eliminate_indices: BTreeSet<usize>,
        left_join_indices: Vec<usize>,
        right_join_indices: Vec<usize>,
    ) -> Result<TupleIter<'a>> {
        let mut stack = vec![];

        let it = left_iter
            .map_ok(move |tuple| -> Result<Option<Tuple>> {
                let prefix = left_to_prefix_indices
                    .iter()
                    .map(|i| tuple[*i].clone())
                    .collect_vec();
                let key = &prefix[0..key_len];
                match self.storage.get(tx, key)? {
                    None => Ok(None),
                    Some(found) => {
                        for (lk, rk) in left_join_indices.iter().zip(right_join_indices.iter()) {
                            if tuple[*lk] != found[*rk] {
                                return Ok(None);
                            }
                        }
                        for (p, span) in self.filters_bytecodes.iter() {
                            if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                return Ok(None);
                            }
                        }
                        let mut ret = tuple;
                        ret.extend(found);
                        Ok(Some(ret))
                    }
                }
            })
            .flatten_ok()
            .filter_map(invert_option_err);
        Ok(if eliminate_indices.is_empty() {
            Box::new(it)
        } else {
            Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
        })
    }

    fn prefix_join<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
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

        let key_len = self.storage.metadata.keys.len();
        if left_to_prefix_indices.len() >= key_len {
            return self.point_lookup_join(
                tx,
                left_iter,
                key_len,
                left_to_prefix_indices,
                eliminate_indices,
                left_join_indices,
                right_join_indices,
            );
        }

        let mut skip_range_check = false;
        // In some cases, maybe we can stop as soon as we get one result?
        let it = left_iter
            .map_ok(move |tuple| {
                let prefix = left_to_prefix_indices
                    .iter()
                    .map(|i| tuple[*i].clone())
                    .collect_vec();
                let mut stack = vec![];

                if !skip_range_check && !self.filters.is_empty() {
                    let other_bindings = &self.bindings[right_join_indices.len()..];
                    let (l_bound, u_bound) = match compute_bounds(&self.filters, other_bindings) {
                        Ok(b) => b,
                        _ => (vec![], vec![]),
                    };
                    if !l_bound.iter().all(|v| *v == DataValue::Null)
                        || !u_bound.iter().all(|v| *v == DataValue::Bot)
                    {
                        return Left(
                            self.storage
                                .scan_bounded_prefix(tx, &prefix, &l_bound, &u_bound)
                                .map(move |res_found| -> Result<Option<Tuple>> {
                                    let found = res_found?;
                                    for (p, span) in self.filters_bytecodes.iter() {
                                        if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                            return Ok(None);
                                        }
                                    }
                                    let mut ret = tuple.clone();
                                    ret.extend(found);
                                    Ok(Some(ret))
                                })
                                .filter_map(swap_option_result),
                        );
                    }
                }
                skip_range_check = true;
                Right(
                    self.storage
                        .scan_prefix(tx, &prefix)
                        .map(move |res_found| -> Result<Option<Tuple>> {
                            let found = res_found?;
                            for (p, span) in self.filters_bytecodes.iter() {
                                if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                    return Ok(None);
                                }
                            }
                            let mut ret = tuple.clone();
                            ret.extend(found);
                            Ok(Some(ret))
                        })
                        .filter_map(swap_option_result),
                )
            })
            .flatten_ok()
            .map(flatten_err);
        Ok(if eliminate_indices.is_empty() {
            Box::new(it)
        } else {
            Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
        })
    }

    fn neg_join<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
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

        if join_is_prefix(&right_join_indices) {
            Ok(Box::new(
                left_iter
                    .map_ok(move |tuple| -> Result<Option<Tuple>> {
                        let prefix = left_to_prefix_indices
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect_vec();

                        'outer: for found in self.storage.scan_prefix(tx, &prefix) {
                            let found = found?;
                            for (left_idx, right_idx) in
                                left_join_indices.iter().zip(right_join_indices.iter())
                            {
                                if tuple[*left_idx] != found[*right_idx] {
                                    continue 'outer;
                                }
                            }
                            return Ok(None);
                        }

                        Ok(Some(if !eliminate_indices.is_empty() {
                            tuple
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec()
                        } else {
                            tuple
                        }))
                    })
                    .map(flatten_err)
                    .filter_map(invert_option_err),
            ))
        } else {
            let mut right_join_vals = BTreeSet::new();

            for tuple in self.storage.scan_all(tx) {
                let tuple = tuple?;
                let to_join: Box<[DataValue]> = right_join_indices
                    .iter()
                    .map(|i| tuple[*i].clone())
                    .collect();
                right_join_vals.insert(to_join);
            }
            Ok(Box::new(
                left_iter
                    .map_ok(move |tuple| -> Result<Option<Tuple>> {
                        let left_join_vals: Box<[DataValue]> = left_join_indices
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect();
                        if right_join_vals.contains(&left_join_vals) {
                            return Ok(None);
                        }

                        Ok(Some(if !eliminate_indices.is_empty() {
                            tuple
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec()
                        } else {
                            tuple
                        }))
                    })
                    .map(flatten_err)
                    .filter_map(invert_option_err),
            ))
        }
    }

    fn iter<'a>(&'a self, tx: &'a SessionTx<'_>) -> Result<TupleIter<'a>> {
        let it = self.storage.scan_all(tx);
        Ok(if self.filters.is_empty() {
            Box::new(it)
        } else {
            Box::new(filter_iter(self.filters_bytecodes.clone(), it))
        })
    }
}

fn join_is_prefix(right_join_indices: &[usize]) -> bool {
    // We do not consider partial index match to be "prefix", e.g. [a, u => c]
    // with a, c bound and u unbound is not "prefix", as it is not clear that
    // using prefix scanning in this case will really save us computation.
    let mut indices = right_join_indices.to_vec();
    indices.sort();
    let l = indices.len();
    indices.into_iter().eq(0..l)
}

#[derive(Debug)]
pub(crate) struct TempStoreRA {
    pub(crate) bindings: Vec<Symbol>,
    pub(crate) storage_key: MagicSymbol,
    pub(crate) filters: Vec<Expr>,
    pub(crate) filters_bytecodes: Vec<(Vec<Bytecode>, SourceSpan)>,
    pub(crate) span: SourceSpan,
}

impl TempStoreRA {
    fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
        let bindings: BTreeMap<_, _> = self
            .bindings
            .iter()
            .cloned()
            .enumerate()
            .map(|(a, b)| (b, a))
            .collect();
        for e in self.filters.iter_mut() {
            e.fill_binding_indices(&bindings)?;
            self.filters_bytecodes.push((e.compile()?, e.span()))
        }
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let storage = stores.get(&self.storage_key).unwrap();

        let scan_epoch = match delta_rule {
            None => false,
            Some(name) => *name == self.storage_key,
        };
        let it = if scan_epoch {
            Left(storage.delta_all_iter().map(|t| Ok(t.into_tuple())))
        } else {
            Right(storage.all_iter().map(|t| Ok(t.into_tuple())))
        };
        Ok(if self.filters.is_empty() {
            Box::new(it)
        } else {
            Box::new(filter_iter(self.filters_bytecodes.clone(), it))
        })
    }
    fn neg_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let storage = stores.get(&self.storage_key).unwrap();
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
        if join_is_prefix(&right_join_indices) {
            Ok(Box::new(
                left_iter
                    .map_ok(move |tuple| -> Result<Option<Tuple>> {
                        let prefix = left_to_prefix_indices
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect_vec();

                        'outer: for found in storage.prefix_iter(&prefix) {
                            for (left_idx, right_idx) in
                                left_join_indices.iter().zip(right_join_indices.iter())
                            {
                                if tuple[*left_idx] != *found.get(*right_idx) {
                                    continue 'outer;
                                }
                            }
                            return Ok(None);
                        }

                        Ok(Some(if !eliminate_indices.is_empty() {
                            tuple
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec()
                        } else {
                            tuple
                        }))
                    })
                    .map(flatten_err)
                    .filter_map(invert_option_err),
            ))
        } else {
            let mut right_join_vals = BTreeSet::new();
            for tuple in storage.all_iter() {
                let to_join: Box<[DataValue]> = right_join_indices
                    .iter()
                    .map(|i| tuple.get(*i).clone())
                    .collect();
                right_join_vals.insert(to_join);
            }

            Ok(Box::new(
                left_iter
                    .map_ok(move |tuple| -> Result<Option<Tuple>> {
                        let left_join_vals: Box<[DataValue]> = left_join_indices
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect();
                        if right_join_vals.contains(&left_join_vals) {
                            return Ok(None);
                        }
                        Ok(Some(if !eliminate_indices.is_empty() {
                            tuple
                                .into_iter()
                                .enumerate()
                                .filter_map(|(i, v)| {
                                    if eliminate_indices.contains(&i) {
                                        None
                                    } else {
                                        Some(v)
                                    }
                                })
                                .collect_vec()
                        } else {
                            tuple
                        }))
                    })
                    .map(flatten_err)
                    .filter_map(invert_option_err),
            ))
        }
    }
    fn prefix_join<'a>(
        &'a self,
        left_iter: TupleIter<'a>,
        (left_join_indices, right_join_indices): (Vec<usize>, Vec<usize>),
        eliminate_indices: BTreeSet<usize>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let storage = stores.get(&self.storage_key).unwrap();

        let mut right_invert_indices = right_join_indices.iter().enumerate().collect_vec();
        right_invert_indices.sort_by_key(|(_, b)| **b);
        let left_to_prefix_indices = right_invert_indices
            .into_iter()
            .map(|(a, _)| left_join_indices[a])
            .collect_vec();
        let scan_epoch = match delta_rule {
            None => false,
            Some(name) => *name == self.storage_key,
        };
        let mut skip_range_check = false;
        let it = left_iter
            .map_ok(move |tuple| {
                let prefix = left_to_prefix_indices
                    .iter()
                    .map(|i| tuple[*i].clone())
                    .collect_vec();
                let mut stack = vec![];

                if !skip_range_check && !self.filters.is_empty() {
                    let other_bindings = &self.bindings[right_join_indices.len()..];
                    let (l_bound, u_bound) = match compute_bounds(&self.filters, other_bindings) {
                        Ok(b) => b,
                        _ => (vec![], vec![]),
                    };
                    if !l_bound.iter().all(|v| *v == DataValue::Null)
                        || !u_bound.iter().all(|v| *v == DataValue::Bot)
                    {
                        let mut lower_bound = prefix.clone();
                        lower_bound.extend(l_bound);
                        let mut upper_bound = prefix;
                        upper_bound.extend(u_bound);
                        let it = if scan_epoch {
                            Left(storage.delta_range_iter(&lower_bound, &upper_bound, true))
                        } else {
                            Right(storage.range_iter(&lower_bound, &upper_bound, true))
                        };
                        return Left(
                            it.map(move |res_found| -> Result<Option<Tuple>> {
                                if self.filters.is_empty() {
                                    let mut ret = tuple.clone();
                                    ret.extend(res_found.into_iter().cloned());
                                    Ok(Some(ret))
                                } else {
                                    let found = res_found.into_tuple();
                                    for (p, span) in self.filters_bytecodes.iter() {
                                        if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                            return Ok(None);
                                        }
                                    }
                                    let mut ret = tuple.clone();
                                    ret.extend(found);
                                    Ok(Some(ret))
                                }
                            })
                            .filter_map(swap_option_result),
                        );
                    }
                }
                skip_range_check = true;

                let it = if scan_epoch {
                    Left(storage.delta_prefix_iter(&prefix))
                } else {
                    Right(storage.prefix_iter(&prefix))
                };

                Right(
                    it.map(move |res_found| -> Result<Option<Tuple>> {
                        if self.filters.is_empty() {
                            let mut ret = tuple.clone();
                            ret.extend(res_found.into_iter().cloned());
                            Ok(Some(ret))
                        } else {
                            let found = res_found.into_tuple();
                            for (p, span) in self.filters_bytecodes.iter() {
                                if !eval_bytecode_pred(p, &found, &mut stack, *span)? {
                                    return Ok(None);
                                }
                            }
                            let mut ret = tuple.clone();
                            ret.extend(found);
                            Ok(Some(ret))
                        }
                    })
                    .filter_map(swap_option_result),
                )
            })
            .flatten_ok()
            .map(flatten_err);
        Ok(if eliminate_indices.is_empty() {
            Box::new(it)
        } else {
            Box::new(it.map_ok(move |t| eliminate_from_tuple(t, &eliminate_indices)))
        })
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
        write!(f, "{left_bindings:?}<->{right_bindings:?}")
    }
}

impl Joiner {
    pub(crate) fn as_map(&self) -> BTreeMap<&str, &str> {
        self.left_keys
            .iter()
            .zip(self.right_keys.iter())
            .map(|(l, r)| (&l.name as &str, &r.name as &str))
            .collect()
    }
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
            let l_pos = left_binding_map.get(l).unwrap();
            let r_pos = right_binding_map.get(r).unwrap();
            ret_l.push(*l_pos);
            ret_r.push(*r_pos)
        }
        Ok((ret_l, ret_r))
    }
}

impl RelAlgebra {
    pub(crate) fn eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
        match self {
            RelAlgebra::Fixed(r) => r.do_eliminate_temp_vars(used),
            RelAlgebra::TempStore(_r) => Ok(()),
            RelAlgebra::Stored(_v) => Ok(()),
            RelAlgebra::StoredWithValidity(_v) => Ok(()),
            RelAlgebra::Join(r) => r.do_eliminate_temp_vars(used),
            RelAlgebra::Reorder(r) => r.relation.eliminate_temp_vars(used),
            RelAlgebra::Filter(r) => r.do_eliminate_temp_vars(used),
            RelAlgebra::NegJoin(r) => r.do_eliminate_temp_vars(used),
            RelAlgebra::Unification(r) => r.do_eliminate_temp_vars(used),
            RelAlgebra::HnswSearch(_) => Ok(()),
            RelAlgebra::FtsSearch(_) => Ok(()),
            RelAlgebra::LshSearch(_) => Ok(()),
        }
    }

    fn eliminate_set(&self) -> Option<&BTreeSet<Symbol>> {
        match self {
            RelAlgebra::Fixed(r) => Some(&r.to_eliminate),
            RelAlgebra::TempStore(_) => None,
            RelAlgebra::Stored(_) => None,
            RelAlgebra::StoredWithValidity(_) => None,
            RelAlgebra::Join(r) => Some(&r.to_eliminate),
            RelAlgebra::Reorder(_) => None,
            RelAlgebra::Filter(r) => Some(&r.to_eliminate),
            RelAlgebra::NegJoin(r) => Some(&r.to_eliminate),
            RelAlgebra::Unification(u) => Some(&u.to_eliminate),
            RelAlgebra::HnswSearch(_) => None,
            RelAlgebra::FtsSearch(_) => None,
            RelAlgebra::LshSearch(_) => None,
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
            RelAlgebra::Fixed(f) => f.bindings.clone(),
            RelAlgebra::TempStore(d) => d.bindings.clone(),
            RelAlgebra::Stored(v) => v.bindings.clone(),
            RelAlgebra::StoredWithValidity(v) => v.bindings.clone(),
            RelAlgebra::Join(j) => j.bindings(),
            RelAlgebra::Reorder(r) => r.bindings(),
            RelAlgebra::Filter(r) => r.parent.bindings_after_eliminate(),
            RelAlgebra::NegJoin(j) => j.left.bindings_after_eliminate(),
            RelAlgebra::Unification(u) => {
                let mut bindings = u.parent.bindings_after_eliminate();
                bindings.push(u.binding.clone());
                bindings
            }
            RelAlgebra::HnswSearch(s) => {
                let mut bindings = s.parent.bindings_after_eliminate();
                bindings.extend_from_slice(&s.own_bindings);
                bindings
            }
            RelAlgebra::FtsSearch(s) => {
                let mut bindings = s.parent.bindings_after_eliminate();
                bindings.extend_from_slice(&s.own_bindings);
                bindings
            }
            RelAlgebra::LshSearch(s) => {
                let mut bindings = s.parent.bindings_after_eliminate();
                bindings.extend_from_slice(&s.own_bindings);
                bindings
            }
        }
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        match self {
            RelAlgebra::Fixed(f) => Ok(Box::new(f.data.iter().map(|t| Ok(t.clone())))),
            RelAlgebra::TempStore(r) => r.iter(delta_rule, stores),
            RelAlgebra::Stored(v) => v.iter(tx),
            RelAlgebra::StoredWithValidity(v) => v.iter(tx),
            RelAlgebra::Join(j) => j.iter(tx, delta_rule, stores),
            RelAlgebra::Reorder(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::Filter(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::NegJoin(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::Unification(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::HnswSearch(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::FtsSearch(r) => r.iter(tx, delta_rule, stores),
            RelAlgebra::LshSearch(r) => r.iter(tx, delta_rule, stores),
        }
    }
}

#[derive(Debug)]
pub(crate) struct NegJoin {
    pub(crate) left: RelAlgebra,
    pub(crate) right: RelAlgebra,
    pub(crate) joiner: Joiner,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
    pub(crate) span: SourceSpan,
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

    pub(crate) fn join_type(&self) -> &str {
        match &self.right {
            RelAlgebra::TempStore(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "mem_neg_prefix_join"
                } else {
                    "mem_neg_mat_join"
                }
            }
            RelAlgebra::Stored(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "stored_neg_prefix_join"
                } else {
                    "stored_neg_mat_join"
                }
            }
            _ => {
                unreachable!()
            }
        }
    }

    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.left.bindings_after_eliminate();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        match &self.right {
            RelAlgebra::TempStore(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                r.neg_join(
                    self.left.iter(tx, delta_rule, stores)?,
                    join_indices,
                    eliminate_indices,
                    stores,
                )
            }
            RelAlgebra::Stored(v) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                v.neg_join(
                    tx,
                    self.left.iter(tx, delta_rule, stores)?,
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
    pub(crate) left: RelAlgebra,
    pub(crate) right: RelAlgebra,
    pub(crate) joiner: Joiner,
    pub(crate) to_eliminate: BTreeSet<Symbol>,
    pub(crate) span: SourceSpan,
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
            RelAlgebra::TempStore(r) => Some(&r.filters),
            _ => None,
        } {
            for filter in filters {
                left.extend(filter.bindings()?);
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
    pub(crate) fn join_type(&self) -> &str {
        match &self.right {
            RelAlgebra::Fixed(f) => f.join_type(),
            RelAlgebra::TempStore(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "mem_prefix_join"
                } else {
                    "mem_mat_join"
                }
            }
            RelAlgebra::Stored(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "stored_prefix_join"
                } else {
                    "stored_mat_join"
                }
            }
            RelAlgebra::HnswSearch(_) => "hnsw_search_join",
            RelAlgebra::FtsSearch(_) => "fts_search_join",
            RelAlgebra::LshSearch(_) => "lsh_search_join",
            RelAlgebra::StoredWithValidity(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "stored_prefix_join"
                } else {
                    "stored_mat_join"
                }
            }
            RelAlgebra::Join(_) | RelAlgebra::Filter(_) | RelAlgebra::Unification(_) => {
                "generic_mat_join"
            }
            RelAlgebra::Reorder(_) => {
                panic!("joining on reordered")
            }
            RelAlgebra::NegJoin(_) => {
                panic!("joining on NegJoin")
            }
        }
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        let bindings = self.bindings();
        let eliminate_indices = get_eliminate_indices(&bindings, &self.to_eliminate);
        match &self.right {
            RelAlgebra::Fixed(f) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                f.join(
                    self.left.iter(tx, delta_rule, stores)?,
                    join_indices,
                    eliminate_indices,
                )
            }
            RelAlgebra::TempStore(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        self.left.iter(tx, delta_rule, stores)?,
                        join_indices,
                        eliminate_indices,
                        delta_rule,
                        stores,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, delta_rule, stores)
                }
            }
            RelAlgebra::Stored(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        tx,
                        self.left.iter(tx, delta_rule, stores)?,
                        join_indices,
                        eliminate_indices,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, delta_rule, stores)
                }
            }
            RelAlgebra::StoredWithValidity(r) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    r.prefix_join(
                        tx,
                        self.left.iter(tx, delta_rule, stores)?,
                        join_indices,
                        eliminate_indices,
                    )
                } else {
                    self.materialized_join(tx, eliminate_indices, delta_rule, stores)
                }
            }
            RelAlgebra::Join(_)
            | RelAlgebra::Filter(_)
            | RelAlgebra::Unification(_)
            | RelAlgebra::HnswSearch(_)
            | RelAlgebra::FtsSearch(_)
            | RelAlgebra::LshSearch(_) => {
                self.materialized_join(tx, eliminate_indices, delta_rule, stores)
            }
            RelAlgebra::Reorder(_) => {
                panic!("joining on reordered")
            }
            RelAlgebra::NegJoin(_) => {
                panic!("joining on NegJoin")
            }
        }
    }
    fn materialized_join<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        eliminate_indices: BTreeSet<usize>,
        delta_rule: Option<&MagicSymbol>,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<TupleIter<'a>> {
        debug!("using materialized join");
        let right_bindings = self.right.bindings_after_eliminate();
        let (left_join_indices, right_join_indices) = self
            .joiner
            .join_indices(&self.left.bindings_after_eliminate(), &right_bindings)
            .unwrap();

        let mut left_iter = self.left.iter(tx, delta_rule, stores)?;
        let left_cache = match left_iter.next() {
            None => return Ok(Box::new(iter::empty())),
            Some(Err(err)) => return Err(err),
            Some(Ok(data)) => data,
        };

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
        let cached_data = {
            let mut cache = BTreeSet::new();
            for item in self.right.iter(tx, delta_rule, stores)? {
                match item {
                    Ok(tuple) => {
                        let stored_tuple = right_store_indices
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect_vec();
                        cache.insert(stored_tuple);
                    }
                    Err(e) => return Err(e),
                }
            }
            cache.into_iter().collect_vec()
        };

        let (prefix, right_idx) =
            build_mat_range_iter(&cached_data, &left_join_indices, &left_cache);

        let it = CachedMaterializedIterator {
            eliminate_indices,
            left: left_iter,
            left_cache,
            left_join_indices,
            materialized: cached_data,
            right_invert_indices,
            right_idx,
            prefix,
        };
        Ok(Box::new(it))
    }
}

struct CachedMaterializedIterator<'a> {
    materialized: Vec<Tuple>,
    eliminate_indices: BTreeSet<usize>,
    left_join_indices: Vec<usize>,
    right_invert_indices: Vec<usize>,
    right_idx: usize,
    prefix: Tuple,
    left: TupleIter<'a>,
    left_cache: Tuple,
}

impl<'a> CachedMaterializedIterator<'a> {
    fn advance_right(&mut self) -> Option<&Tuple> {
        if self.right_idx == self.materialized.len() {
            None
        } else {
            let ret = &self.materialized[self.right_idx];
            if ret.starts_with(&self.prefix) {
                self.right_idx += 1;
                Some(ret)
            } else {
                None
            }
        }
    }
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            let right_nxt = self.advance_right();
            match right_nxt {
                Some(data) => {
                    let data = data.clone();
                    let mut ret = self.left_cache.clone();
                    for i in &self.right_invert_indices {
                        ret.push(data[*i].clone());
                    }
                    let tuple = eliminate_from_tuple(ret, &self.eliminate_indices);
                    return Ok(Some(tuple));
                }
                None => {
                    let next_left = self.left.next();
                    match next_left {
                        None => return Ok(None),
                        Some(l) => {
                            let left_tuple = l?;
                            let (prefix, idx) = build_mat_range_iter(
                                &self.materialized,
                                &self.left_join_indices,
                                &left_tuple,
                            );
                            self.left_cache = left_tuple;

                            self.right_idx = idx;
                            self.prefix = prefix;
                        }
                    }
                }
            }
        }
    }
}

fn build_mat_range_iter(
    mat: &[Tuple],
    left_join_indices: &[usize],
    left_tuple: &Tuple,
) -> (Tuple, usize) {
    let prefix = left_join_indices
        .iter()
        .map(|i| left_tuple[*i].clone())
        .collect_vec();
    let idx = match mat.binary_search(&prefix) {
        Ok(i) => i,
        Err(i) => i,
    };
    (prefix, idx)
}

impl<'a> Iterator for CachedMaterializedIterator<'a> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::DataValue;
    use crate::DbInstance;

    #[test]
    fn test_mat_join() {
        let db = DbInstance::default();
        let res = db
            .run_default(
                r#"
        data[a, b] <- [[1, 2], [1, 3], [2, 3]]
        ?[x] := a = 3, data[x, a]
        "#,
            )
            .unwrap()
            .rows;
        assert_eq!(
            res,
            vec![vec![DataValue::from(1)], vec![DataValue::from(2)]]
        )
    }
}
