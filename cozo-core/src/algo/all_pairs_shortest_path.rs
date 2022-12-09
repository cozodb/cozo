/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Reverse;
use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::algo::shortest_path_dijkstra::dijkstra_keep_ties;
use crate::algo::{AlgoImpl, AlgoPayload};
use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct BetweennessCentrality;

impl AlgoImpl for BetweennessCentrality {
    fn run(
        &self,
        payload: AlgoPayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let undirected = payload.bool_option("undirected", Some(false))?;

        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false)?;

        let n = graph.len();
        if n == 0 {
            return Ok(());
        }

        #[cfg(feature = "rayon")]
        let it = (0..n).into_par_iter();
        #[cfg(not(feature = "rayon"))]
        let it = (0..n).into_iter();

        let centrality_segs: Vec<_> = it
            .map(|start| -> Result<BTreeMap<usize, f64>> {
                let res_for_start =
                    dijkstra_keep_ties(&graph, start, &(), &(), &(), poison.clone())?;
                let mut ret: BTreeMap<usize, f64> = Default::default();
                let grouped = res_for_start.into_iter().group_by(|(n, _, _)| *n);
                for (_, grp) in grouped.into_iter() {
                    let grp = grp.collect_vec();
                    let l = grp.len() as f64;
                    for (_, _, path) in grp {
                        if path.len() < 3 {
                            continue;
                        }
                        for middle in path.iter().take(path.len() - 1).skip(1) {
                            let entry = ret.entry(*middle).or_default();
                            *entry += 1. / l;
                        }
                    }
                }
                Ok(ret)
            })
            .collect::<Result<_>>()?;
        let mut centrality: Vec<f64> = vec![0.; graph.len()];
        for m in centrality_segs {
            for (k, v) in m {
                centrality[k] += v;
            }
        }

        for (i, s) in centrality.into_iter().enumerate() {
            let node = indices[i].clone();
            out.put(vec![node, s.into()]);
        }

        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(2)
    }
}

pub(crate) struct ClosenessCentrality;

impl AlgoImpl for ClosenessCentrality {
    fn run(
        &self,
        payload: AlgoPayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let undirected = payload.bool_option("undirected", Some(false))?;

        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false)?;

        let n = graph.len();
        if n == 0 {
            return Ok(());
        }
        #[cfg(feature = "rayon")]
        let it = (0..n).into_par_iter();
        #[cfg(not(feature = "rayon"))]
        let it = (0..n).into_iter();

        let res: Vec<_> = it
            .map(|start| -> Result<f64> {
                let distances = dijkstra_cost_only(&graph, start, poison.clone())?;
                let total_dist: f64 = distances.iter().filter(|d| d.is_finite()).cloned().sum();
                let nc: f64 = distances.iter().filter(|d| d.is_finite()).count() as f64;
                Ok(nc * nc / total_dist / (n - 1) as f64)
            })
            .collect::<Result<_>>()?;
        for (idx, centrality) in res.into_iter().enumerate() {
            out.put(vec![indices[idx].clone(), DataValue::from(centrality)]);
            poison.check()?;
        }
        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(2)
    }
}

pub(crate) fn dijkstra_cost_only(
    edges: &[Vec<(usize, f64)>],
    start: usize,
    poison: Poison,
) -> Result<Vec<f64>> {
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
        poison.check()?;
    }

    Ok(distance)
}
