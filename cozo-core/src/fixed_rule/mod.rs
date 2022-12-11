/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::sync::Arc;

use either::{Left, Right};
use graph::prelude::{CsrLayout, DirectedCsrGraph, GraphBuilder};
use lazy_static::lazy_static;
use miette::{bail, ensure, Diagnostic, Report, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::program::{
    FixedRuleOptionNotFoundError, MagicFixedRuleApply, MagicFixedRuleRuleArg, MagicSymbol,
    WrongFixedRuleOptionError,
};
use crate::data::symb::Symbol;
use crate::data::tuple::TupleIter;
use crate::data::value::DataValue;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::all_pairs_shortest_path::{
    BetweennessCentrality, ClosenessCentrality,
};
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::astar::ShortestPathAStar;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::bfs::Bfs;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::degree_centrality::DegreeCentrality;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::dfs::Dfs;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::kruskal::MinimumSpanningForestKruskal;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::label_propagation::LabelPropagation;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::louvain::CommunityDetectionLouvain;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::pagerank::PageRank;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::prim::MinimumSpanningTreePrim;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::shortest_path_bfs::ShortestPathBFS;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::shortest_path_dijkstra::ShortestPathDijkstra;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::strongly_connected_components::StronglyConnectedComponent;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::top_sort::TopSort;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::triangles::ClusteringCoefficients;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::yen::KShortestPathYen;
use crate::fixed_rule::utilities::constant::Constant;
use crate::fixed_rule::utilities::csv::CsvReader;
use crate::fixed_rule::utilities::jlines::JsonReader;
use crate::fixed_rule::utilities::random_walk::RandomWalk;
use crate::fixed_rule::utilities::reorder_sort::ReorderSort;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, RegularTempStore};
use crate::runtime::transact::SessionTx;

pub(crate) mod algos;
pub(crate) mod utilities;

pub struct FixedRulePayload<'a, 'b> {
    pub(crate) manifest: &'a MagicFixedRuleApply,
    pub(crate) stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    pub(crate) tx: &'a SessionTx<'b>,
}

#[derive(Copy, Clone)]
pub struct FixedRuleInputRelation<'a, 'b> {
    arg_manifest: &'a MagicFixedRuleRuleArg,
    stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    tx: &'a SessionTx<'b>,
}

impl<'a, 'b> FixedRuleInputRelation<'a, 'b> {
    pub fn arity(&self) -> Result<usize> {
        self.arg_manifest.arity(self.tx, self.stores)
    }
    pub fn ensure_min_len(self, len: usize) -> Result<Self> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Input relation to algorithm has insufficient arity")]
        #[diagnostic(help("Arity should be at least {0} but is {1}"))]
        #[diagnostic(code(algo::input_relation_bad_arity))]
        struct InputRelationArityError(usize, usize, #[label] SourceSpan);

        let arity = self.arg_manifest.arity(self.tx, self.stores)?;
        ensure!(
            arity >= len,
            InputRelationArityError(len, arity, self.arg_manifest.span())
        );
        Ok(self)
    }
    pub fn get_binding_map(&self, offset: usize) -> BTreeMap<Symbol, usize> {
        self.arg_manifest.get_binding_map(offset)
    }
    pub fn iter(&self) -> Result<TupleIter<'a>> {
        Ok(match &self.arg_manifest {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = self.stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                Box::new(store.all_iter().map(|t| Ok(t.into_tuple())))
            }
            MagicFixedRuleRuleArg::Stored { name, .. } => {
                let relation = self.tx.get_relation(name, false)?;
                Box::new(relation.scan_all(self.tx))
            }
        })
    }
    pub fn prefix_iter(&self, prefix: &DataValue) -> Result<TupleIter<'_>> {
        Ok(match self.arg_manifest {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = self.stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                let t = vec![prefix.clone()];
                Box::new(store.prefix_iter(&t).map(|t| Ok(t.into_tuple())))
            }
            MagicFixedRuleRuleArg::Stored { name, .. } => {
                let relation = self.tx.get_relation(name, false)?;
                let t = vec![prefix.clone()];
                Box::new(relation.scan_prefix(self.tx, &t))
            }
        })
    }
    pub fn span(&self) -> SourceSpan {
        self.arg_manifest.span()
    }
    pub fn to_directed_graph(
        &self,
        undirected: bool,
    ) -> Result<(
        DirectedCsrGraph<u32>,
        Vec<DataValue>,
        BTreeMap<DataValue, u32>,
    )> {
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, u32> = Default::default();
        let mut error: Option<Report> = None;
        let it = self.iter()?.filter_map(|r_tuple| match r_tuple {
            Ok(tuple) => {
                let mut tuple = tuple.into_iter();
                let from = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let to = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let from_idx = if let Some(idx) = inv_indices.get(&from) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(from.clone(), idx);
                    indices.push(from.clone());
                    idx
                };
                let to_idx = if let Some(idx) = inv_indices.get(&to) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(to.clone(), idx);
                    indices.push(to.clone());
                    idx
                };
                Some((from_idx, to_idx))
            }
            Err(err) => {
                error = Some(err);
                None
            }
        });
        let it = if undirected {
            Right(it.flat_map(|(f, t)| [(f, t), (t, f)]))
        } else {
            Left(it)
        };
        let graph: DirectedCsrGraph<u32> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges(it)
            .build();
        if let Some(err) = error {
            bail!(err)
        }
        Ok((graph, indices, inv_indices))
    }

    pub fn to_directed_weighted_graph(
        &self,
        undirected: bool,
        allow_negative_weights: bool,
    ) -> Result<(
        DirectedCsrGraph<u32, (), f32>,
        Vec<DataValue>,
        BTreeMap<DataValue, u32>,
    )> {
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, u32> = Default::default();
        let mut error: Option<Report> = None;
        let it = self.iter()?.filter_map(|r_tuple| match r_tuple {
            Ok(tuple) => {
                let mut tuple = tuple.into_iter();
                let from = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let to = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let from_idx = if let Some(idx) = inv_indices.get(&from) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(from.clone(), idx);
                    indices.push(from.clone());
                    idx
                };
                let to_idx = if let Some(idx) = inv_indices.get(&to) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(to.clone(), idx);
                    indices.push(to.clone());
                    idx
                };

                let weight = match tuple.next() {
                    None => 1.0,
                    Some(d) => match d.get_float() {
                        Some(f) => {
                            if !f.is_finite() {
                                error = Some(
                                    BadEdgeWeightError(
                                        d,
                                        self.arg_manifest
                                            .bindings()
                                            .get(2)
                                            .map(|s| s.span)
                                            .unwrap_or_else(|| self.span()),
                                    )
                                    .into(),
                                );
                                return None;
                            };

                            if f < 0. {
                                if !allow_negative_weights {
                                    error = Some(
                                        BadEdgeWeightError(
                                            d,
                                            self.arg_manifest
                                                .bindings()
                                                .get(2)
                                                .map(|s| s.span)
                                                .unwrap_or_else(|| self.span()),
                                        )
                                        .into(),
                                    );
                                    return None;
                                }
                            }
                            f
                        }
                        None => {
                            error = Some(
                                BadEdgeWeightError(
                                    d,
                                    self.arg_manifest
                                        .bindings()
                                        .get(2)
                                        .map(|s| s.span)
                                        .unwrap_or_else(|| self.span()),
                                )
                                .into(),
                            );
                            return None;
                        }
                    },
                };

                Some((from_idx, to_idx, weight as f32))
            }
            Err(err) => {
                error = Some(err);
                None
            }
        });
        let it = if undirected {
            Right(it.flat_map(|(f, t, w)| [(f, t, w), (t, f, w)]))
        } else {
            Left(it)
        };
        let graph: DirectedCsrGraph<u32, (), f32> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges_with_values(it)
            .build();

        if let Some(err) = error {
            bail!(err)
        }

        Ok((graph, indices, inv_indices))
    }
}

impl<'a, 'b> FixedRulePayload<'a, 'b> {
    pub fn get_input(&self, idx: usize) -> Result<FixedRuleInputRelation<'a, 'b>> {
        let arg_manifest = self.manifest.relation(idx)?;
        Ok(FixedRuleInputRelation {
            arg_manifest,
            stores: self.stores,
            tx: self.tx,
        })
    }
    pub fn name(&self) -> &str {
        &self.manifest.fixed_handle.name
    }
    pub fn span(&self) -> SourceSpan {
        self.manifest.span
    }
    pub fn expr_option(&self, name: &str, default: Option<Expr>) -> Result<Expr> {
        match self.manifest.options.get(name) {
            Some(ex) => Ok(ex.clone()),
            None => match default {
                Some(ex) => Ok(ex),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }

    pub fn string_option(
        &self,
        name: &str,
        default: Option<&str>,
    ) -> Result<SmartString<LazyCompact>> {
        match self.manifest.options.get(name) {
            Some(ex) => match ex.clone().eval_to_const()? {
                DataValue::Str(s) => Ok(s),
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: ex.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a string is required".to_string(),
                }
                .into()),
            },
            None => match default {
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
                Some(s) => Ok(SmartString::from(s)),
            },
        }
    }

    pub fn pos_integer_option(&self, name: &str, default: Option<usize>) -> Result<usize> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => match n.get_int() {
                    Some(i) => {
                        ensure!(
                            i > 0,
                            WrongFixedRuleOptionError {
                                name: name.to_string(),
                                span: v.span(),
                                rule_name: self.manifest.fixed_handle.name.to_string(),
                                help: "a positive integer is required".to_string(),
                            }
                        );
                        Ok(i as usize)
                    }
                    None => Err(FixedRuleOptionNotFoundError {
                        name: name.to_string(),
                        span: self.span(),
                        rule_name: self.manifest.fixed_handle.name.to_string(),
                    }
                    .into()),
                },
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a positive integer is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
    pub fn non_neg_integer_option(&self, name: &str, default: Option<usize>) -> Result<usize> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => match n.get_int() {
                    Some(i) => {
                        ensure!(
                            i >= 0,
                            WrongFixedRuleOptionError {
                                name: name.to_string(),
                                span: v.span(),
                                rule_name: self.manifest.fixed_handle.name.to_string(),
                                help: "a non-negative integer is required".to_string(),
                            }
                        );
                        Ok(i as usize)
                    }
                    None => Err(FixedRuleOptionNotFoundError {
                        name: name.to_string(),
                        span: self.manifest.span,
                        rule_name: self.manifest.fixed_handle.name.to_string(),
                    }
                    .into()),
                },
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a non-negative integer is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
    pub fn unit_interval_option(&self, name: &str, default: Option<f64>) -> Result<f64> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => {
                    let f = n.get_float();
                    ensure!(
                        (0. ..=1.).contains(&f),
                        WrongFixedRuleOptionError {
                            name: name.to_string(),
                            span: v.span(),
                            rule_name: self.manifest.fixed_handle.name.to_string(),
                            help: "a number between 0. and 1. is required".to_string(),
                        }
                    );
                    Ok(f)
                }
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a number between 0. and 1. is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
    pub(crate) fn bool_option(&self, name: &str, default: Option<bool>) -> Result<bool> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Bool(b)) => Ok(b),
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a boolean value is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
}

/// Trait for an implementation of an algorithm or a utility
pub trait FixedRule: Send + Sync {
    /// Called to initialize the options given.
    /// Will always be called once, before anything else.
    /// You can mutate the options if you need to.
    /// The default implementation does nothing.
    fn init_options(
        &self,
        _options: &mut BTreeMap<SmartString<LazyCompact>, Expr>,
        _span: SourceSpan,
    ) -> Result<()> {
        Ok(())
    }
    /// You must return the row width of the returned relation and it must be accurate.
    /// This function may be called multiple times.
    fn arity(
        &self,
        options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize>;
    /// You should implement the logic of your algorithm/utility in this function.
    /// The outputs are written to `out`. You should check `poison` periodically
    /// for user-initiated termination.
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &'_ mut RegularTempStore,
        poison: Poison,
    ) -> Result<()>;
}

#[derive(Debug, Error, Diagnostic)]
#[error("Cannot determine arity for algo {0} since {1}")]
#[diagnostic(code(parser::no_algo_arity))]
pub(crate) struct CannotDetermineArity(
    pub(crate) String,
    pub(crate) String,
    #[label] pub(crate) SourceSpan,
);

#[derive(Clone, Debug)]
pub(crate) struct FixedRuleHandle {
    pub(crate) name: Symbol,
}

lazy_static! {
    pub(crate) static ref DEFAULT_FIXED_RULES: Arc<BTreeMap<String, Arc<Box<dyn FixedRule>>>> = {
        Arc::new(BTreeMap::from([
            #[cfg(feature = "graph-algo")]
            (
                "ClusteringCoefficients".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ClusteringCoefficients)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DegreeCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(DegreeCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ClosenessCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ClosenessCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BetweennessCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(BetweennessCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DepthFirstSearch".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Dfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Dfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BreadthFirstSearch".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Bfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Bfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathBFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathBFS)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathDijkstra".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathDijkstra)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathAStar".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathAStar)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "KShortestPathYen".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(KShortestPathYen)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "MinimumSpanningTreePrim".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(MinimumSpanningTreePrim)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "MinimumSpanningForestKruskal".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(MinimumSpanningForestKruskal)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "TopSort".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(TopSort)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ConnectedComponents".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(false))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "StronglyConnectedComponents".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(true))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "SCC".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(true))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "PageRank".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(PageRank)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "CommunityDetectionLouvain".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(CommunityDetectionLouvain)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "LabelPropagation".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(LabelPropagation)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "RandomWalk".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(RandomWalk)),
            ),
            (
                "ReorderSort".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ReorderSort)),
            ),
            (
                "JsonReader".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(JsonReader)),
            ),
            (
                "CsvReader".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(CsvReader)),
            ),
            (
                "Constant".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Constant)),
            ),
        ]))
    };
}

impl FixedRuleHandle {
    pub(crate) fn new(name: &str, span: SourceSpan) -> Self {
        FixedRuleHandle {
            name: Symbol::new(name, span),
        }
    }
}

#[derive(Error, Diagnostic, Debug)]
#[error("The relation cannot be interpreted as an edge")]
#[diagnostic(code(algo::not_an_edge))]
#[diagnostic(help("Edge relation requires tuples of length at least two"))]
struct NotAnEdgeError(#[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error(
    "The value {0:?} at the third position in the relation cannot be interpreted as edge weights"
)]
#[diagnostic(code(algo::invalid_edge_weight))]
#[diagnostic(help(
    "Edge weights must be finite numbers. Some algorithm also requires positivity."
))]
struct BadEdgeWeightError(DataValue, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("The requested rule '{0}' cannot be found")]
#[diagnostic(code(algo::rule_not_found))]
struct RuleNotFoundError(String, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Invalid reverse scanning of triples")]
#[diagnostic(code(algo::invalid_reverse_triple_scan))]
#[diagnostic(help(
    "Inverse scanning of triples requires the type to be 'ref', or the value be indexed"
))]
struct InvalidInverseTripleUse(String, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Required node with key {missing:?} not found")]
#[diagnostic(code(algo::node_with_key_not_found))]
#[diagnostic(help(
    "The relation is interpreted as a relation of nodes, but the required key is missing"
))]
pub(crate) struct NodeNotFoundError {
    pub(crate) missing: DataValue,
    #[label]
    pub(crate) span: SourceSpan,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Unacceptable value {0:?} encountered")]
#[diagnostic(code(algo::unacceptable_value))]
pub(crate) struct BadExprValueError(
    pub(crate) DataValue,
    #[label] pub(crate) SourceSpan,
    #[help] pub(crate) String,
);

#[derive(Error, Diagnostic, Debug)]
#[error("The requested fixed rule '{0}' is not found")]
#[diagnostic(code(parser::fixed_rule_not_found))]
pub(crate) struct FixedRuleNotFoundError(pub(crate) String, #[label] pub(crate) SourceSpan);

impl MagicFixedRuleRuleArg {
    pub(crate) fn arity(
        &self,
        tx: &SessionTx<'_>,
        stores: &BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<usize> {
        Ok(match self {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                store.arity
            }
            MagicFixedRuleRuleArg::Stored { name, .. } => {
                let handle = tx.get_relation(name, false)?;
                handle.arity()
            }
        })
    }
}
