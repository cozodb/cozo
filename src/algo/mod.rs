use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use smartstring::{LazyCompact, SmartString};
use crate::algo::all_pairs_shortest_path::ClosenessCentrality;

use crate::algo::astar::ShortestPathAStar;
use crate::algo::bfs::Bfs;
use crate::algo::degree_centrality::DegreeCentrality;
use crate::algo::dfs::Dfs;
use crate::algo::kruskal::MinimumSpanningTreeKruskal;
use crate::algo::prim::MinimumSpanningTreePrim;
use crate::algo::shortest_path_dijkstra::ShortestPathDijkstra;
use crate::algo::strongly_connected_components::StronglyConnectedComponent;
use crate::algo::top_sort::TopSort;
use crate::algo::yen::KShortestPathYen;
use crate::data::expr::Expr;
use crate::data::id::{EntityId, Validity};
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol, TripleDir};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, TupleIter};
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) mod astar;
pub(crate) mod bfs;
pub(crate) mod degree_centrality;
pub(crate) mod dfs;
pub(crate) mod kruskal;
pub(crate) mod page_rank;
pub(crate) mod prim;
pub(crate) mod shortest_path_dijkstra;
pub(crate) mod strongly_connected_components;
pub(crate) mod top_sort;
pub(crate) mod triangles;
pub(crate) mod yen;
pub(crate) mod all_pairs_shortest_path;

pub(crate) trait AlgoImpl {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct AlgoHandle {
    pub(crate) name: Symbol,
}

impl AlgoHandle {
    pub(crate) fn new(name: &str) -> Self {
        AlgoHandle {
            name: Symbol::from(name),
        }
    }
    pub(crate) fn arity(&self) -> Result<usize> {
        Ok(match &self.name.0 as &str {
            "degree_centrality" => 4,
            "closeness_centrality" => 2,
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
            "page_rank" => todo!(),
            name => bail!("algorithm '{}' not found", name),
        })
    }

    pub(crate) fn get_impl(&self) -> Result<Box<dyn AlgoImpl>> {
        Ok(match &self.name.0 as &str {
            "degree_centrality" => Box::new(DegreeCentrality),
            "closeness_centrality" => Box::new(ClosenessCentrality),
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
            "page_rank" => todo!(),
            name => bail!("algorithm '{}' not found", name),
        })
    }
}

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
            let from = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation too short"))?;
            let to = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation too short"))?;
            let weight = match tuple.next() {
                None => 1.0,
                Some(d) => match d.get_float() {
                    Some(f) => {
                        ensure!(f.is_finite(), "edge weight must be finite, got {}", f);
                        if f < 0. {
                            if !allow_negative_edges {
                                bail!("edge weight must be non-negative, got {}", f);
                            }
                            has_neg_edge = true;
                        }
                        f
                    }
                    None => bail!("edge weight must be a number, got {:?}", d),
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
            let from = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation too short"))?;
            let to = tuple
                .next()
                .ok_or_else(|| anyhow!("edges relation too short"))?;
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
            MagicAlgoRuleArg::InMem(s, _) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(store.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Stored(s, _) => {
                let view_rel = tx.get_view_rel(s)?;
                let t = Tuple(vec![prefix.clone()]);
                Box::new(view_rel.scan_prefix(&t))
            }
            MagicAlgoRuleArg::Triple(attr, _, dir) => {
                if *dir == TripleDir::Bwd && !attr.val_type.is_ref_type() {
                    ensure!(
                        attr.indexing.should_index(),
                        "reverse scanning of triple values requires indexing: {:?}",
                        attr.name
                    );
                    if attr.with_history {
                        Box::new(
                            tx.triple_av_before_scan(attr.id, prefix, Validity::MAX)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_av_scan(attr.id, prefix)
                                .map_ok(|(_, v, eid)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                } else {
                    let id = prefix.get_int().ok_or_else(|| {
                        anyhow!(
                            "prefix scanning of triple requires integer id, got {:?}",
                            prefix
                        )
                    })?;
                    let id = EntityId(id as u64);
                    match dir {
                        TripleDir::Fwd => {
                            if attr.with_history {
                                Box::new(
                                    tx.triple_ea_before_scan(id, attr.id, Validity::MAX)
                                        .map_ok(|(eid, _, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            } else {
                                Box::new(
                                    tx.triple_ea_scan(id, attr.id)
                                        .map_ok(|(eid, _, v)| Tuple(vec![eid.as_datavalue(), v])),
                                )
                            }
                        }
                        TripleDir::Bwd => {
                            if attr.with_history {
                                Box::new(
                                    tx.triple_vref_a_before_scan(id, attr.id, Validity::MAX)
                                        .map_ok(|(v, _, eid)| {
                                            Tuple(vec![v.as_datavalue(), eid.as_datavalue()])
                                        }),
                                )
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
            MagicAlgoRuleArg::InMem(s, _) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                store.arity
            }
            MagicAlgoRuleArg::Stored(s, _) => {
                let view_rel = tx.get_view_rel(s)?;
                view_rel.metadata.arity
            }
            MagicAlgoRuleArg::Triple(_, _, _) => 2,
        })
    }
    pub(crate) fn iter<'a>(
        &'a self,
        tx: &'a SessionTx,
        stores: &'a BTreeMap<MagicSymbol, DerivedRelStore>,
    ) -> Result<TupleIter<'a>> {
        Ok(match self {
            MagicAlgoRuleArg::InMem(s, _) => {
                let store = stores
                    .get(s)
                    .ok_or_else(|| anyhow!("rule not found: {:?}", s))?;
                Box::new(store.scan_all())
            }
            MagicAlgoRuleArg::Stored(s, _) => {
                let view_rel = tx.get_view_rel(s)?;
                Box::new(view_rel.scan_all()?)
            }
            MagicAlgoRuleArg::Triple(attr, _, dir) => match dir {
                TripleDir::Fwd => {
                    if attr.with_history {
                        Box::new(
                            tx.triple_a_before_scan(attr.id, Validity::MAX)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(attr.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![eid.as_datavalue(), v])),
                        )
                    }
                }
                TripleDir::Bwd => {
                    if attr.with_history {
                        Box::new(
                            tx.triple_a_before_scan(attr.id, Validity::MAX)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    } else {
                        Box::new(
                            tx.triple_a_scan(attr.id)
                                .map_ok(|(_, eid, v)| Tuple(vec![v, eid.as_datavalue()])),
                        )
                    }
                }
            },
        })
    }
}
