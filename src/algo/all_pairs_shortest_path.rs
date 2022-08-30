use std::cmp::Reverse;
use std::collections::BTreeMap;

use anyhow::{anyhow, bail};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use rayon::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::algo::shortest_path_dijkstra::dijkstra_keep_ties;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct BetweennessCentrality;

impl AlgoImpl for BetweennessCentrality {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> anyhow::Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'betweenness_centrality' requires edges relation"))?;
        let undirected = match opts.get("undirected") {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'undirected' for 'betweenness_centrality' requires a boolean, got {:?}",
                v
            ),
        };

        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false, tx, stores)?;

        let n = graph.len();
        if n == 0 {
            return Ok(());
        }

        let centrality_segs: Vec<_> = (0..n)
            .into_par_iter()
            .map(|start| {
                let res_for_start = dijkstra_keep_ties(&graph, start, &(), &(), &());
                let mut ret: BTreeMap<usize, f64> = Default::default();
                let grouped = res_for_start.into_iter().group_by(|(n, _, _)| *n);
                for (_, grp) in grouped.into_iter() {
                    let grp = grp.collect_vec();
                    let l = grp.len() as f64;
                    for (_, _, path) in grp {
                        if path.len() < 3 {
                            continue;
                        }
                        for i in 1..(path.len() - 1) {
                            let middle = path[i];
                            let entry = ret.entry(middle).or_default();
                            *entry += 1. / l;
                        }
                    }
                }
                ret
            })
            .collect();
        let mut centrality: Vec<f64> = vec![0.; graph.len()];
        for m in centrality_segs {
            for (k, v) in m {
                centrality[k] += v;
            }
        }

        for (i, s) in centrality.into_iter().enumerate() {
            let node = indices[i].clone();
            out.put(Tuple(vec![node, s.into()]), 0);
        }

        Ok(())
    }
}

pub(crate) struct ClosenessCentrality;

impl AlgoImpl for ClosenessCentrality {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> anyhow::Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'closeness_centrality' requires edges relation"))?;
        let undirected = match opts.get("undirected") {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'undirected' for 'closeness_centrality' requires a boolean, got {:?}",
                v
            ),
        };

        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false, tx, stores)?;

        let n = graph.len();
        if n == 0 {
            return Ok(());
        }
        let res: Vec<_> = (0..n)
            .into_par_iter()
            .map(|start| -> f64 {
                let distances = dijkstra_cost_only(&graph, start);
                let total_dist: f64 = distances.iter().filter(|d| d.is_finite()).cloned().sum();
                let nc: f64 = distances.iter().filter(|d| d.is_finite()).count() as f64;
                nc * nc / total_dist / (n - 1) as f64
            })
            .collect();
        for (idx, centrality) in res.into_iter().enumerate() {
            out.put(
                Tuple(vec![indices[idx].clone(), DataValue::from(centrality)]),
                0,
            );
        }
        Ok(())
    }
}

pub(crate) fn dijkstra_cost_only(edges: &[Vec<(usize, f64)>], start: usize) -> Vec<f64> {
    let mut distance = vec![f64::INFINITY; edges.len()];
    let mut pq = PriorityQueue::new();
    let mut back_pointers = vec![usize::MAX; edges.len()];
    distance[start] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node] {
            continue;
        }

        for (nxt_node, path_weight) in &edges[node] {
            let nxt_cost = cost + *path_weight;
            if nxt_cost < distance[*nxt_node] {
                pq.push_increase(*nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[*nxt_node] = nxt_cost;
                back_pointers[*nxt_node] = node;
            }
        }
    }

    distance
}
