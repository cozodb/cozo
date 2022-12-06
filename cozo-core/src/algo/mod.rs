/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use miette::{bail, ensure, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

#[cfg(feature = "graph-algo")]
use crate::algo::all_pairs_shortest_path::{BetweennessCentrality, ClosenessCentrality};
#[cfg(feature = "graph-algo")]
use crate::algo::astar::ShortestPathAStar;
#[cfg(feature = "graph-algo")]
use crate::algo::bfs::Bfs;
use crate::algo::constant::Constant;
use crate::algo::csv::CsvReader;
#[cfg(feature = "graph-algo")]
use crate::algo::degree_centrality::DegreeCentrality;
#[cfg(feature = "graph-algo")]
use crate::algo::dfs::Dfs;
use crate::algo::jlines::JsonReader;
#[cfg(feature = "graph-algo")]
use crate::algo::kruskal::MinimumSpanningForestKruskal;
#[cfg(feature = "graph-algo")]
use crate::algo::label_propagation::LabelPropagation;
#[cfg(feature = "graph-algo")]
use crate::algo::louvain::CommunityDetectionLouvain;
#[cfg(feature = "graph-algo")]
use crate::algo::pagerank::PageRank;
#[cfg(feature = "graph-algo")]
use crate::algo::prim::MinimumSpanningTreePrim;
#[cfg(feature = "graph-algo")]
use crate::algo::random_walk::RandomWalk;
use crate::algo::reorder_sort::ReorderSort;
#[cfg(feature = "graph-algo")]
use crate::algo::shortest_path_dijkstra::ShortestPathDijkstra;
#[cfg(feature = "graph-algo")]
use crate::algo::strongly_connected_components::StronglyConnectedComponent;
#[cfg(feature = "graph-algo")]
use crate::algo::top_sort::TopSort;
#[cfg(feature = "graph-algo")]
use crate::algo::triangles::ClusteringCoefficients;
#[cfg(feature = "graph-algo")]
use crate::algo::yen::KShortestPathYen;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoApply, MagicAlgoRuleArg, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::TupleIter;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

#[cfg(feature = "graph-algo")]
pub(crate) mod all_pairs_shortest_path;
#[cfg(feature = "graph-algo")]
pub(crate) mod astar;
#[cfg(feature = "graph-algo")]
pub(crate) mod bfs;
pub(crate) mod constant;
pub(crate) mod csv;
#[cfg(feature = "graph-algo")]
pub(crate) mod degree_centrality;
#[cfg(feature = "graph-algo")]
pub(crate) mod dfs;
pub(crate) mod jlines;
#[cfg(feature = "graph-algo")]
pub(crate) mod kruskal;
#[cfg(feature = "graph-algo")]
pub(crate) mod label_propagation;
#[cfg(feature = "graph-algo")]
pub(crate) mod louvain;
#[cfg(feature = "graph-algo")]
pub(crate) mod pagerank;
#[cfg(feature = "graph-algo")]
pub(crate) mod prim;
#[cfg(feature = "graph-algo")]
pub(crate) mod random_walk;
pub(crate) mod reorder_sort;
#[cfg(feature = "graph-algo")]
pub(crate) mod shortest_path_dijkstra;
pub(crate) mod strongly_connected_components;
#[cfg(feature = "graph-algo")]
pub(crate) mod top_sort;
#[cfg(feature = "graph-algo")]
pub(crate) mod triangles;
#[cfg(feature = "graph-algo")]
pub(crate) mod yen;

pub(crate) trait AlgoImpl {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx<'_>,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
        out: &'a InMemRelation,
        poison: Poison,
    ) -> Result<()>;
    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize>;
    fn process_options(
        &self,
        _options: &mut BTreeMap<SmartString<LazyCompact>, Expr>,
        _span: SourceSpan,
    ) -> Result<()> {
        Ok(())
    }
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
pub(crate) struct AlgoHandle {
    pub(crate) name: Symbol,
}

impl AlgoHandle {
    pub(crate) fn new(name: &str, span: SourceSpan) -> Self {
        AlgoHandle {
            name: Symbol::new(name, span),
        }
    }

    pub(crate) fn get_impl(&self) -> Result<Box<dyn AlgoImpl>> {
        Ok(match &self.name.name as &str {
            #[cfg(feature = "graph-algo")]
            "ClusteringCoefficients" => Box::new(ClusteringCoefficients),
            #[cfg(feature = "graph-algo")]
            "DegreeCentrality" => Box::new(DegreeCentrality),
            #[cfg(feature = "graph-algo")]
            "ClosenessCentrality" => Box::new(ClosenessCentrality),
            #[cfg(feature = "graph-algo")]
            "BetweennessCentrality" => Box::new(BetweennessCentrality),
            #[cfg(feature = "graph-algo")]
            "DepthFirstSearch" | "DFS" => Box::new(Dfs),
            #[cfg(feature = "graph-algo")]
            "BreadthFirstSearch" | "BFS" => Box::new(Bfs),
            #[cfg(feature = "graph-algo")]
            "ShortestPathDijkstra" => Box::new(ShortestPathDijkstra),
            #[cfg(feature = "graph-algo")]
            "ShortestPathAStar" => Box::new(ShortestPathAStar),
            #[cfg(feature = "graph-algo")]
            "KShortestPathYen" => Box::new(KShortestPathYen),
            #[cfg(feature = "graph-algo")]
            "MinimumSpanningTreePrim" => Box::new(MinimumSpanningTreePrim),
            #[cfg(feature = "graph-algo")]
            "MinimumSpanningForestKruskal" => Box::new(MinimumSpanningForestKruskal),
            #[cfg(feature = "graph-algo")]
            "TopSort" => Box::new(TopSort),
            #[cfg(feature = "graph-algo")]
            "ConnectedComponents" => Box::new(StronglyConnectedComponent::new(false)),
            #[cfg(feature = "graph-algo")]
            "StronglyConnectedComponents" | "SCC" => {
                Box::new(StronglyConnectedComponent::new(true))
            }
            #[cfg(feature = "graph-algo")]
            "PageRank" => Box::new(PageRank),
            #[cfg(feature = "graph-algo")]
            "CommunityDetectionLouvain" => Box::new(CommunityDetectionLouvain),
            #[cfg(feature = "graph-algo")]
            "LabelPropagation" => Box::new(LabelPropagation),
            #[cfg(feature = "graph-algo")]
            "RandomWalk" => Box::new(RandomWalk),
            "ReorderSort" => Box::new(ReorderSort),
            "JsonReader" => Box::new(JsonReader),
            "CsvReader" => Box::new(CsvReader),
            "Constant" => Box::new(Constant),
            name => bail!(AlgoNotFoundError(name.to_string(), self.name.span)),
        })
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
#[error("The requested algorithm '{0}' is not found")]
#[diagnostic(code(parser::algo_not_found))]
pub(crate) struct AlgoNotFoundError(pub(crate) String, #[label] pub(crate) SourceSpan);

impl MagicAlgoRuleArg {
    #[allow(dead_code)]
    pub(crate) fn convert_edge_to_weighted_graph<'a>(
        &'a self,
        undirected: bool,
        allow_negative_edges: bool,
        tx: &'a SessionTx<'_>,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
    ) -> Result<(
        Vec<Vec<(usize, f64)>>,
        Vec<DataValue>,
        BTreeMap<DataValue, usize>,
        bool,
    )> {
        let mut graph: Vec<Vec<(usize, f64)>> = vec![];
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, usize> = Default::default();
        let mut has_neg_edge = false;

        for tuple in self.iter(tx, stores)? {
            let mut tuple = tuple?.into_iter();
            let from = tuple.next().ok_or_else(|| NotAnEdgeError(self.span()))?;
            let to = tuple.next().ok_or_else(|| NotAnEdgeError(self.span()))?;
            let weight = match tuple.next() {
                None => 1.0,
                Some(d) => match d.get_float() {
                    Some(f) => {
                        ensure!(
                            f.is_finite(),
                            BadEdgeWeightError(
                                d,
                                self.bindings()
                                    .get(2)
                                    .map(|s| s.span)
                                    .unwrap_or_else(|| self.span())
                            )
                        );
                        if f < 0. {
                            if !allow_negative_edges {
                                bail!(BadEdgeWeightError(
                                    d,
                                    self.bindings()
                                        .get(2)
                                        .map(|s| s.span)
                                        .unwrap_or_else(|| self.span())
                                ));
                            }
                            has_neg_edge = true;
                        }
                        f
                    }
                    None => {
                        bail!(BadEdgeWeightError(
                            d,
                            self.bindings()
                                .get(2)
                                .map(|s| s.span)
                                .unwrap_or_else(|| self.span())
                        ))
                    }
                },
            };
            let from_idx = if let Some(idx) = inv_indices.get(&from) {
                *idx
            } else {
                inv_indices.insert(from.clone(), graph.len());
                indices.push(from.clone());
                graph.push(vec![]);
                graph.len() - 1
            };
            let to_idx = if let Some(idx) = inv_indices.get(&to) {
                *idx
            } else {
                inv_indices.insert(to.clone(), graph.len());
                indices.push(to.clone());
                graph.push(vec![]);
                graph.len() - 1
            };
            let from_target = graph.get_mut(from_idx).unwrap();
            from_target.push((to_idx, weight));
            if undirected {
                let to_target = graph.get_mut(to_idx).unwrap();
                to_target.push((from_idx, weight));
            }
        }
        Ok((graph, indices, inv_indices, has_neg_edge))
    }
    #[allow(dead_code)]
    pub(crate) fn convert_edge_to_graph<'a>(
        &'a self,
        undirected: bool,
        tx: &'a SessionTx<'_>,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
    ) -> Result<(Vec<Vec<usize>>, Vec<DataValue>, BTreeMap<DataValue, usize>)> {
        let mut graph: Vec<Vec<usize>> = vec![];
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, usize> = Default::default();

        for tuple in self.iter(tx, stores)? {
            let mut tuple = tuple?.into_iter();
            let from = tuple.next().ok_or_else(|| NotAnEdgeError(self.span()))?;
            let to = tuple.next().ok_or_else(|| NotAnEdgeError(self.span()))?;
            let from_idx = if let Some(idx) = inv_indices.get(&from) {
                *idx
            } else {
                inv_indices.insert(from.clone(), graph.len());
                indices.push(from.clone());
                graph.push(vec![]);
                graph.len() - 1
            };
            let to_idx = if let Some(idx) = inv_indices.get(&to) {
                *idx
            } else {
                inv_indices.insert(to.clone(), graph.len());
                indices.push(to.clone());
                graph.push(vec![]);
                graph.len() - 1
            };
            let from_target = graph.get_mut(from_idx).unwrap();
            from_target.push(to_idx);
            if undirected {
                let to_target = graph.get_mut(to_idx).unwrap();
                to_target.push(from_idx);
            }
        }
        Ok((graph, indices, inv_indices))
    }
    #[allow(dead_code)]
    pub(crate) fn prefix_iter<'a>(
        &'a self,
        prefix: &DataValue,
        tx: &'a SessionTx<'_>,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                let t = vec![prefix.clone()];
                Box::new(store.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let relation = tx.get_relation(name, false)?;
                let t = vec![prefix.clone()];
                Box::new(relation.scan_prefix(tx, &t))
            }
        })
    }
    pub(crate) fn arity(
        &self,
        tx: &SessionTx<'_>,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
    ) -> Result<usize> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                store.arity
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let handle = tx.get_relation(name, false)?;
                handle.arity()
            }
        })
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx<'_>,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                Box::new(store.scan_all())
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let relation = tx.get_relation(name, false)?;
                Box::new(relation.scan_all(tx))
            }
        })
    }
}
