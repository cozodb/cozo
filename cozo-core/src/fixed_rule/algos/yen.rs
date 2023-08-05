/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use graph::prelude::{DirectedCsrGraph, DirectedNeighborsWithValues};
use itertools::Itertools;
use miette::Result;
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::algos::shortest_path_dijkstra::dijkstra;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct KShortestPathYen;

impl FixedRule for KShortestPathYen {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let starting = payload.get_input(1)?;
        let termination = payload.get_input(2)?;
        let undirected = payload.bool_option("undirected", Some(false))?;
        let k = payload.pos_integer_option("k", None)?;

        let (graph, indices, inv_indices) = edges.as_directed_weighted_graph(undirected, false)?;

        let mut starting_nodes = BTreeSet::new();
        for tuple in starting.iter()? {
            let tuple = tuple?;
            let node = &tuple[0];
            if let Some(idx) = inv_indices.get(node) {
                starting_nodes.insert(*idx);
            }
        }
        let mut termination_nodes = BTreeSet::new();
        for tuple in termination.iter()? {
            let tuple = tuple?;
            let node = &tuple[0];
            if let Some(idx) = inv_indices.get(node) {
                termination_nodes.insert(*idx);
            }
        }
        if starting_nodes.len() <= 1 && termination_nodes.len() <= 1 {
            for start in starting_nodes {
                for goal in &termination_nodes {
                    for (cost, path) in
                        k_shortest_path_yen(k, &graph, start, *goal, poison.clone())?
                    {
                        let t = vec![
                            indices[start as usize].clone(),
                            indices[*goal as usize].clone(),
                            DataValue::from(cost as f64),
                            DataValue::List(
                                path.into_iter()
                                    .map(|u| indices[u as usize].clone())
                                    .collect_vec(),
                            ),
                        ];
                        out.put(t)
                    }
                }
            }
        } else {
            let first_it = starting_nodes
                .iter()
                .flat_map(|start| termination_nodes.iter().map(|goal| (*start, *goal)));
            #[cfg(feature = "rayon")]
            let first_it = first_it.par_bridge();

            let res_all: Vec<_> = first_it
                .map(
                    |(start, goal)| -> Result<(u32, u32, Vec<(f32, Vec<u32>)>)> {
                        Ok((
                            start,
                            goal,
                            k_shortest_path_yen(k, &graph, start, goal, poison.clone())?,
                        ))
                    },
                )
                .collect::<Result<_>>()?;

            for (start, goal, res) in res_all {
                for (cost, path) in res {
                    let t = vec![
                        indices[start as usize].clone(),
                        indices[goal as usize].clone(),
                        DataValue::from(cost as f64),
                        DataValue::List(
                            path.into_iter()
                                .map(|u| indices[u as usize].clone())
                                .collect_vec(),
                        ),
                    ];
                    out.put(t)
                }
            }
        }
        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(4)
    }
}

fn k_shortest_path_yen(
    k: usize,
    edges: &DirectedCsrGraph<u32, (), f32>,
    start: u32,
    goal: u32,
    poison: Poison,
) -> Result<Vec<(f32, Vec<u32>)>> {
    let mut k_shortest: Vec<(f32, Vec<u32>)> = Vec::with_capacity(k);
    let mut candidates: Vec<(f32, Vec<u32>)> = vec![];

    match dijkstra(edges, start, &Some(goal), &(), &())
        .into_iter()
        .next()
    {
        None => return Ok(k_shortest),
        Some((_, cost, path)) => k_shortest.push((cost, path)),
    }

    for _ in 1..k {
        let (_, prev_path) = k_shortest.last().unwrap();
        for i in 0..prev_path.len() - 1 {
            let spur_node = match prev_path.get(i) {
                None => return Ok(vec![]),
                Some(n) => *n,
            };
            let root_path = &prev_path[0..i + 1];
            let mut forbidden_edges = BTreeSet::new();
            for (_, p) in &k_shortest {
                if p.len() < root_path.len() + 1 {
                    continue;
                }
                let p_prefix = &p[0..i + 1];
                if p_prefix == root_path {
                    forbidden_edges.insert((p[i], p[i + 1]));
                }
            }
            let mut forbidden_nodes = BTreeSet::new();
            for node in &prev_path[0..i] {
                forbidden_nodes.insert(*node);
            }
            if let Some((_, spur_cost, spur_path)) = dijkstra(
                edges,
                spur_node,
                &Some(goal),
                &forbidden_edges,
                &forbidden_nodes,
            )
            .into_iter()
            .next()
            {
                let mut total_cost = spur_cost;
                for i in 0..root_path.len() - 1 {
                    let s = root_path[i];
                    let d = root_path[i + 1];
                    for target in edges.out_neighbors_with_values(s) {
                        let e = target.target;
                        let c = target.value;
                        if e == d {
                            total_cost += c;
                            break;
                        }
                    }
                }
                let mut total_path = root_path.to_vec();
                total_path.pop();
                total_path.extend(spur_path);
                if candidates.iter().all(|(_, v)| *v != total_path) {
                    candidates.push((total_cost, total_path));
                }
                poison.check()?;
            }
        }
        if candidates.is_empty() {
            break;
        }
        candidates.sort_by(|(a_cost, _), (b_cost, _)| b_cost.total_cmp(a_cost));
        let shortest = candidates.pop().unwrap();
        let shortest_dist = shortest.0;
        if shortest_dist.is_finite() {
            k_shortest.push(shortest);
        }
    }
    Ok(k_shortest)
}
