use std::collections::BTreeMap;

use either::Either;
use miette::{bail, ensure, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::algo::all_pairs_shortest_path::{BetweennessCentrality, ClosenessCentrality};
use crate::algo::astar::ShortestPathAStar;
use crate::algo::bfs::Bfs;
use crate::algo::csv::CsvReader;
use crate::algo::degree_centrality::DegreeCentrality;
use crate::algo::dfs::Dfs;
use crate::algo::jlines::JsonReader;
use crate::algo::kruskal::MinimumSpanningForestKruskal;
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
use crate::data::program::{
    AlgoOptionNotFoundError, AlgoRuleArg, MagicAlgoApply, MagicAlgoRuleArg, MagicSymbol,
};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) mod all_pairs_shortest_path;
pub(crate) mod astar;
pub(crate) mod bfs;
pub(crate) mod csv;
pub(crate) mod degree_centrality;
pub(crate) mod dfs;
pub(crate) mod jlines;
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
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        out: &InMemRelation,
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
    ) -> Result<usize> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Cannot determine arity for algo {0} since {1}")]
        #[diagnostic(code(parser::no_algo_arity))]
        struct CannotDetermineArity(String, String, #[label] SourceSpan);

        Ok(match &self.name.name as &str {
            "ClusteringCoefficients" => 4,
            "DegreeCentrality" => 4,
            "ClosenessCentrality" => 2,
            "BetweennessCentrality" => 2,
            "DepthFirstSearch" | "DFS" => 1,
            "BreadthFirstSearch" | "BFS" => 1,
            "ShortestPathDijkstra" => 4,
            "ShortestPathAStar" => 4,
            "KShortestPathYen" => 4,
            "MinimumSpanningTreePrim" => 3,
            "MinimumSpanningTreeKruskal" => 3,
            "TopSort" => 2,
            "ConnectedComponents" => 2,
            "StronglyConnectedComponents" | "SCC" => 2,
            "PageRank" => 2,
            "CommunityDetectionLouvain" => 2,
            "LabelPropagation" => 2,
            "RandomWalk" => 3,
            n @ "ReorderSort" => {
                let out_opts = opts.get("out").ok_or_else(|| {
                    CannotDetermineArity(
                        n.to_string(),
                        "option 'out' not provided".to_string(),
                        self.name.span,
                    )
                })?;
                match out_opts {
                    Expr::Const {
                        val: DataValue::List(l),
                        ..
                    } => l.len() + 1,
                    Expr::Apply { op, args, .. } if **op == OP_LIST => args.len() + 1,
                    _ => bail!(CannotDetermineArity(
                        n.to_string(),
                        "invalid option 'out' given, expect a list".to_string(),
                        self.name.span
                    )),
                }
            }
            n @ "CsvReader" => {
                let with_row_num = match opts.get("prepend_index") {
                    None => 0,
                    Some(Expr::Const {
                        val: DataValue::Bool(true),
                        ..
                    }) => 1,
                    Some(Expr::Const {
                        val: DataValue::Bool(false),
                        ..
                    }) => 0,
                    _ => bail!(CannotDetermineArity(
                        n.to_string(),
                        "invalid option 'prepend_index' given, expect a boolean".to_string(),
                        self.name.span
                    )),
                };
                let columns = opts.get("types").ok_or_else(|| AlgoOptionNotFoundError {
                    name: "types".to_string(),
                    span: self.name.span,
                    algo_name: n.to_string(),
                })?;
                let columns = columns.clone().eval_to_const()?;
                if let Some(l) = columns.get_list() {
                    return Ok(l.len() + with_row_num);
                }
                bail!(CannotDetermineArity(
                    n.to_string(),
                    "invalid option 'types' given, expect positive number or list".to_string(),
                    self.name.span
                ))
            }
            n @ "JsonReader" => {
                let with_row_num = match opts.get("prepend_index") {
                    None => 0,
                    Some(Expr::Const {
                        val: DataValue::Bool(true),
                        ..
                    }) => 1,
                    Some(Expr::Const {
                        val: DataValue::Bool(false),
                        ..
                    }) => 0,
                    _ => bail!(CannotDetermineArity(
                        n.to_string(),
                        "invalid option 'prepend_index' given, expect a boolean".to_string(),
                        self.name.span
                    )),
                };
                let fields = opts.get("fields").ok_or_else(|| {
                    CannotDetermineArity(
                        n.to_string(),
                        "option 'fields' not provided".to_string(),
                        self.name.span,
                    )
                })?;
                match fields.clone().eval_to_const()? {
                    DataValue::List(l) => l.len() + with_row_num,
                    _ => bail!(CannotDetermineArity(
                        n.to_string(),
                        "invalid option 'fields' given, expect a list".to_string(),
                        self.name.span
                    )),
                }
            }
            n => bail!(AlgoNotFoundError(n.to_string(), self.name.span)),
        })
    }

    pub(crate) fn get_impl(&self) -> Result<Box<dyn AlgoImpl>> {
        Ok(match &self.name.name as &str {
            "ClusteringCoefficients" => Box::new(ClusteringCoefficients),
            "DegreeCentrality" => Box::new(DegreeCentrality),
            "ClosenessCentrality" => Box::new(ClosenessCentrality),
            "BetweennessCentrality" => Box::new(BetweennessCentrality),
            "DepthFirstSearch" | "DFS" => Box::new(Dfs),
            "BreadthFirstSearch" | "BFS" => Box::new(Bfs),
            "ShortestPathDijkstra" => Box::new(ShortestPathDijkstra),
            "ShortestPathAStar" => Box::new(ShortestPathAStar),
            "KShortestPathYen" => Box::new(KShortestPathYen),
            "MinimumSpanningTreePrim" => Box::new(MinimumSpanningTreePrim),
            "MinimumSpanningForestKruskal" => Box::new(MinimumSpanningForestKruskal),
            "TopSort" => Box::new(TopSort),
            "ConnectedComponents" => Box::new(StronglyConnectedComponent::new(false)),
            "StronglyConnectedComponents" | "SCC" => {
                Box::new(StronglyConnectedComponent::new(true))
            }
            "PageRank" => Box::new(PageRank),
            "CommunityDetectionLouvain" => Box::new(CommunityDetectionLouvain),
            "LabelPropagation" => Box::new(LabelPropagation),
            "RandomWalk" => Box::new(RandomWalk),
            "ReorderSort" => Box::new(ReorderSort),
            "JsonReader" => Box::new(JsonReader),
            "CsvReader" => Box::new(CsvReader),
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
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
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
    pub(crate) fn convert_edge_to_graph(
        &self,
        undirected: bool,
        tx: &SessionTx,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
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
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
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
                let relation = tx.get_relation(name, false)?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(relation.scan_prefix(tx, &t))
            }
        })
    }
    pub(crate) fn arity(
        &self,
        tx: &SessionTx,
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
        tx: &'a SessionTx,
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
