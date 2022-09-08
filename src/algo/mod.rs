use std::collections::BTreeMap;

use either::Either;
use itertools::Itertools;
use miette::{bail, ensure, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::algo::all_pairs_shortest_path::{BetweennessCentrality, ClosenessCentrality};
use crate::algo::astar::ShortestPathAStar;
use crate::algo::bfs::Bfs;
use crate::algo::degree_centrality::DegreeCentrality;
use crate::algo::dfs::Dfs;
use crate::algo::kruskal::MinimumSpanningTreeKruskal;
use crate::algo::label_propagation::LabelPropagation;
use crate::algo::louvain::CommunityDetectionLouvain;
use crate::algo::pagerank::PageRank;
use crate::algo::prim::MinimumSpanningTreePrim;
use crate::algo::random_walk::RandomWalk;
use crate::algo::reorder_sort::ReorderSort;
use crate::algo::shortest_path_dijkstra::ShortestPathDijkstra;
use crate::algo::strongly_connected_components::StronglyConnectedComponent;
use crate::algo::top_sort::TopSort;
use crate::algo::triangles::ClusteringCoefficients;
use crate::algo::yen::KShortestPathYen;
use crate::data::expr::Expr;
use crate::data::functions::OP_LIST;
use crate::data::id::EntityId;
use crate::data::program::{AlgoRuleArg, MagicAlgoApply, MagicAlgoRuleArg, MagicSymbol, TripleDir};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) mod all_pairs_shortest_path;
pub(crate) mod astar;
pub(crate) mod bfs;
pub(crate) mod degree_centrality;
pub(crate) mod dfs;
pub(crate) mod kruskal;
pub(crate) mod label_propagation;
pub(crate) mod louvain;
pub(crate) mod pagerank;
pub(crate) mod prim;
pub(crate) mod random_walk;
pub(crate) mod reorder_sort;
pub(crate) mod shortest_path_dijkstra;
pub(crate) mod strongly_connected_components;
pub(crate) mod top_sort;
pub(crate) mod triangles;
pub(crate) mod yen;

pub(crate) trait AlgoImpl {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()>;
}

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
    pub(crate) fn arity(
        &self,
        _args: Either<&[AlgoRuleArg], &[MagicAlgoRuleArg]>,
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
    ) -> Option<usize> {
        Some(match &self.name.name as &str {
            "clustering_coefficients" => 4,
            "degree_centrality" => 4,
            "closeness_centrality" => 2,
            "betweenness_centrality" => 2,
            "depth_first_search" | "dfs" => 1,
            "breadth_first_search" | "bfs" => 1,
            "shortest_path_dijkstra" => 4,
            "shortest_path_astar" => 4,
            "k_shortest_path_yen" => 4,
            "minimum_spanning_tree_prim" => 3,
            "minimum_spanning_tree_kruskal" => 3,
            "top_sort" => 2,
            "connected_components" => 2,
            "strongly_connected_components" | "scc" => 2,
            "pagerank" => 2,
            "community_detection_louvain" => 2,
            "label_propagation" => 2,
            "random_walk" => 3,
            "reorder_sort" => {
                let out_opts = opts.get("out")?;
                match out_opts {
                    Expr::Const {
                        val: DataValue::List(l),
                        ..
                    } => l.len() + 1,
                    Expr::Apply { op, args, .. } if **op == OP_LIST => args.len() + 1,
                    _ => return None,
                }
            }
            _ => return None,
        })
    }

    pub(crate) fn get_impl(&self) -> Result<Box<dyn AlgoImpl>> {
        Ok(match &self.name.name as &str {
            "clustering_coefficients" => Box::new(ClusteringCoefficients),
            "degree_centrality" => Box::new(DegreeCentrality),
            "closeness_centrality" => Box::new(ClosenessCentrality),
            "betweenness_centrality" => Box::new(BetweennessCentrality),
            "depth_first_search" | "dfs" => Box::new(Dfs),
            "breadth_first_search" | "bfs" => Box::new(Bfs),
            "shortest_path_dijkstra" => Box::new(ShortestPathDijkstra),
            "shortest_path_astar" => Box::new(ShortestPathAStar),
            "k_shortest_path_yen" => Box::new(KShortestPathYen),
            "minimum_spanning_tree_prim" => Box::new(MinimumSpanningTreePrim),
            "minimum_spanning_tree_kruskal" => Box::new(MinimumSpanningTreeKruskal),
            "top_sort" => Box::new(TopSort),
            "connected_components" => Box::new(StronglyConnectedComponent::new(false)),
            "strongly_connected_components" | "scc" => {
                Box::new(StronglyConnectedComponent::new(true))
            }
            "pagerank" => Box::new(PageRank),
            "community_detection_louvain" => Box::new(CommunityDetectionLouvain),
            "label_propagation" => Box::new(LabelPropagation),
            "random_walk" => Box::new(RandomWalk),
            "reorder_sort" => Box::new(ReorderSort),
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
    pub(crate) fn convert_edge_to_weighted_graph(
        &self,
        undirected: bool,
        allow_negative_edges: bool,
        tx: &SessionTx,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
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
            let mut tuple = tuple?.0.into_iter();
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
                                    .unwrap_or(self.span())
                            )
                        );
                        if f < 0. {
                            if !allow_negative_edges {
                                bail!(BadEdgeWeightError(
                                    d,
                                    self.bindings()
                                        .get(2)
                                        .map(|s| s.span)
                                        .unwrap_or(self.span())
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
                                .unwrap_or(self.span())
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
    pub(crate) fn convert_edge_to_graph(
        &self,
        undirected: bool,
        tx: &SessionTx,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<(Vec<Vec<usize>>, Vec<DataValue>, BTreeMap<DataValue, usize>)> {
        let mut graph: Vec<Vec<usize>> = vec![];
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, usize> = Default::default();

        for tuple in self.iter(tx, stores)? {
            let mut tuple = tuple?.0.into_iter();
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

    pub(crate) fn prefix_iter<'a>(
        &'a self,
        prefix: &DataValue,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(store.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let relation = tx.get_relation(name)?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(relation.scan_prefix(tx, &t))
            }
            MagicAlgoRuleArg::Triple {
                attr,
                dir,
                vld,
                span,
                ..
            } => {
                if *dir == TripleDir::Bwd && !attr.val_type.is_ref_type() {
                    ensure!(
                        attr.indexing.should_index(),
                        InvalidInverseTripleUse(attr.name.to_string(), *span)
                    );
                    if attr.with_history {
                        Box::new(
                            tx.triple_av_before_scan(attr.id, prefix, *vld)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_av_scan(attr.id, prefix)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                } else {
                    #[derive(Error, Diagnostic, Debug)]
                    #[error("Encountered bad prefix value {0:?} during triple prefix scanning")]
                    #[diagnostic(code(algo::invalid_triple_prefix))]
                    #[diagnostic(help(
                        "Triple prefix should be an entity ID represented by an integer"
                    ))]
                    struct InvalidTriplePrefixError(DataValue, #[label] SourceSpan);

                    let id = prefix
                        .get_int()
                        .ok_or_else(|| InvalidTriplePrefixError(prefix.clone(), self.span()))?;
                    let id = EntityId(id as u64);
                    match dir {
                        TripleDir::Fwd => {
                            if attr.with_history {
                                Box::new(
                                    tx.triple_ae_before_scan(attr.id, id, *vld)
                                        .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            } else {
                                Box::new(
                                    tx.triple_ae_scan(attr.id, id)
                                        .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            }
                        }
                        TripleDir::Bwd => {
                            if attr.with_history {
                                Box::new(tx.triple_vref_a_before_scan(id, attr.id, *vld).map_ok(
                                    |(v, _, eid)| Tuple(vec![v.as_datavalue(), eid.as_datavalue()]),
                                ))
                            } else {
                                Box::new(tx.triple_vref_a_scan(id, attr.id).map_ok(
                                    |(v, _, eid)| Tuple(vec![v.as_datavalue(), eid.as_datavalue()]),
                                ))
                            }
                        }
                    }
                }
            }
        })
    }
    pub(crate) fn arity(
        &self,
        tx: &SessionTx,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<usize> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                store.arity
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let meta = tx.get_relation(name)?;
                meta.arity
            }
            MagicAlgoRuleArg::Triple { .. } => 2,
        })
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                Box::new(store.scan_all())
            }
            MagicAlgoRuleArg::Stored { name, .. } => {
                let relation = tx.get_relation(name)?;
                Box::new(relation.scan_all(tx))
            }
            MagicAlgoRuleArg::Triple {
                attr: name,
                dir,
                vld,
                ..
            } => match dir {
                TripleDir::Fwd => {
                    if name.with_history {
                        Box::new(
                            tx.triple_a_before_scan(name.id, *vld)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(name.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    }
                }
                TripleDir::Bwd => {
                    if name.with_history {
                        Box::new(
                            tx.triple_a_before_scan(name.id, *vld)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(name.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                }
            },
        })
    }
}
