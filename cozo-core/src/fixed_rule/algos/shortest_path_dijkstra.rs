/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use graph::prelude::{DirectedCsrGraph, DirectedNeighborsWithValues, Graph};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet};
use std::iter;

use itertools::Itertools;
use miette::Result;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use rayon::prelude::*;
use smallvec::{smallvec, SmallVec};
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct ShortestPathDijkstra;

impl FixedRule for ShortestPathDijkstra {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let starting = payload.get_input(1)?;
        let termination = payload.get_input(2);
        let undirected = payload.bool_option("undirected", Some(false))?;
        let keep_ties = payload.bool_option("keep_ties", Some(false))?;

        let (graph, indices, inv_indices) = edges.as_directed_weighted_graph(undirected, false)?;

        let mut starting_nodes = BTreeSet::new();
        for tuple in starting.iter()? {
            let tuple = tuple?;
            let node = &tuple[0];
            if let Some(idx) = inv_indices.get(node) {
                starting_nodes.insert(*idx);
            }
        }
        let termination_nodes = match termination {
            Err(_) => None,
            Ok(t) => {
                let mut tn = BTreeSet::new();
                for tuple in t.iter()? {
                    let tuple = tuple?;
                    let node = &tuple[0];
                    if let Some(idx) = inv_indices.get(node) {
                        tn.insert(*idx);
                    }
                }
                Some(tn)
            }
        };

        if starting_nodes.len() <= 1 {
            for start in starting_nodes {
                let res = if let Some(tn) = &termination_nodes {
                    if tn.len() == 1 {
                        let single = Some(*tn.iter().next().unwrap());
                        if keep_ties {
                            dijkstra_keep_ties(&graph, start, &single, &(), &(), poison.clone())?
                        } else {
                            dijkstra(&graph, start, &single, &(), &())
                        }
                    } else if keep_ties {
                        dijkstra_keep_ties(&graph, start, tn, &(), &(), poison.clone())?
                    } else {
                        dijkstra(&graph, start, tn, &(), &())
                    }
                } else {
                    dijkstra(&graph, start, &(), &(), &())
                };
                for (target, cost, path) in res {
                    let t = vec![
                        indices[start as usize].clone(),
                        indices[target as usize].clone(),
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
        } else {
            let it = starting_nodes.into_par_iter();

            let all_res: Vec<_> = it
                .map(|start| -> Result<(u32, Vec<(u32, f32, Vec<u32>)>)> {
                    Ok((
                        start,
                        if let Some(tn) = &termination_nodes {
                            if tn.len() == 1 {
                                let single = Some(*tn.iter().next().unwrap());
                                if keep_ties {
                                    dijkstra_keep_ties(
                                        &graph,
                                        start,
                                        &single,
                                        &(),
                                        &(),
                                        poison.clone(),
                                    )?
                                } else {
                                    dijkstra(&graph, start, &single, &(), &())
                                }
                            } else if keep_ties {
                                dijkstra_keep_ties(&graph, start, tn, &(), &(), poison.clone())?
                            } else {
                                dijkstra(&graph, start, tn, &(), &())
                            }
                        } else {
                            dijkstra(&graph, start, &(), &(), &())
                        },
                    ))
                })
                .collect::<Result<_>>()?;
            for (start, res) in all_res {
                for (target, cost, path) in res {
                    let t = vec![
                        indices[start as usize].clone(),
                        indices[target as usize].clone(),
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

#[derive(PartialEq)]
struct HeapState {
    cost: f64,
    node: usize,
}

impl PartialOrd for HeapState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapState {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cost
            .total_cmp(&other.cost)
            .reverse()
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl Eq for HeapState {}

pub(crate) trait ForbiddenEdge {
    fn is_forbidden(&self, src: u32, dst: u32) -> bool;
}

impl ForbiddenEdge for () {
    fn is_forbidden(&self, _src: u32, _dst: u32) -> bool {
        false
    }
}

impl ForbiddenEdge for BTreeSet<(u32, u32)> {
    fn is_forbidden(&self, src: u32, dst: u32) -> bool {
        self.contains(&(src, dst))
    }
}

pub(crate) trait ForbiddenNode {
    fn is_forbidden(&self, node: u32) -> bool;
}

impl ForbiddenNode for () {
    fn is_forbidden(&self, _node: u32) -> bool {
        false
    }
}

impl ForbiddenNode for BTreeSet<u32> {
    fn is_forbidden(&self, node: u32) -> bool {
        self.contains(&node)
    }
}

pub(crate) trait Goal {
    fn is_exhausted(&self) -> bool;
    fn visit(&mut self, node: u32);
    fn iter(&self, total: u32) -> Box<dyn Iterator<Item = u32> + '_>;
}

impl Goal for () {
    fn is_exhausted(&self) -> bool {
        false
    }

    fn visit(&mut self, _node: u32) {}

    fn iter(&self, total: u32) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(0..total)
    }
}

impl Goal for Option<u32> {
    fn is_exhausted(&self) -> bool {
        self.is_none()
    }

    fn visit(&mut self, node: u32) {
        if let Some(u) = &self {
            if *u == node {
                self.take();
            }
        }
    }

    fn iter(&self, _total: u32) -> Box<dyn Iterator<Item = u32> + '_> {
        if let Some(u) = self {
            Box::new(iter::once(*u))
        } else {
            Box::new(iter::empty())
        }
    }
}

impl Goal for BTreeSet<u32> {
    fn is_exhausted(&self) -> bool {
        self.is_empty()
    }

    fn visit(&mut self, node: u32) {
        self.remove(&node);
    }

    fn iter(&self, _total: u32) -> Box<dyn Iterator<Item = u32> + '_> {
        Box::new(self.iter().cloned())
    }
}

pub(crate) fn dijkstra<FE: ForbiddenEdge, FN: ForbiddenNode, G: Goal + Clone>(
    edges: &DirectedCsrGraph<u32, (), f32>,
    start: u32,
    goals: &G,
    forbidden_edges: &FE,
    forbidden_nodes: &FN,
) -> Vec<(u32, f32, Vec<u32>)> {
    let graph_size = edges.node_count();
    let mut distance = vec![f32::INFINITY; graph_size as usize];
    let mut pq = PriorityQueue::new();
    let mut back_pointers = vec![u32::MAX; graph_size as usize];
    distance[start as usize] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));
    let mut goals_remaining = goals.clone();

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node as usize] {
            continue;
        }

        for target in edges.out_neighbors_with_values(node) {
            let nxt_node = target.target;
            let path_weight = target.value;

            if forbidden_nodes.is_forbidden(nxt_node) {
                continue;
            }
            if forbidden_edges.is_forbidden(node, nxt_node) {
                continue;
            }
            let nxt_cost = cost + path_weight;
            if nxt_cost < distance[nxt_node as usize] {
                pq.push_increase(nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[nxt_node as usize] = nxt_cost;
                back_pointers[nxt_node as usize] = node;
            }
        }

        goals_remaining.visit(node);
        if goals_remaining.is_exhausted() {
            break;
        }
    }

    let ret = goals
        .iter(edges.node_count())
        .map(|target| {
            let cost = distance[target as usize];
            if !cost.is_finite() {
                (target, cost, vec![])
            } else {
                let mut path = vec![];
                let mut current = target;
                while current != start {
                    path.push(current);
                    current = back_pointers[current as usize];
                }
                path.push(start);
                path.reverse();
                (target, cost, path)
            }
        })
        .collect_vec();

    ret
}

pub(crate) fn dijkstra_keep_ties<FE: ForbiddenEdge, FN: ForbiddenNode, G: Goal + Clone>(
    edges: &DirectedCsrGraph<u32, (), f32>,
    start: u32,
    goals: &G,
    forbidden_edges: &FE,
    forbidden_nodes: &FN,
    poison: Poison,
) -> Result<Vec<(u32, f32, Vec<u32>)>> {
    let mut distance = vec![f32::INFINITY; edges.node_count() as usize];
    let mut pq = PriorityQueue::new();
    let mut back_pointers: Vec<SmallVec<[u32; 1]>> = vec![smallvec![]; edges.node_count() as usize];
    distance[start as usize] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));
    let mut goals_remaining = goals.clone();

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node as usize] {
            continue;
        }

        for target in edges.out_neighbors_with_values(node) {
            let nxt_node = target.target;
            let path_weight = target.value;

            if forbidden_nodes.is_forbidden(nxt_node) {
                continue;
            }
            if forbidden_edges.is_forbidden(node, nxt_node) {
                continue;
            }
            let nxt_cost = cost + path_weight;
            if nxt_cost < distance[nxt_node as usize] {
                pq.push_increase(nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[nxt_node as usize] = nxt_cost;
                back_pointers[nxt_node as usize].clear();
                back_pointers[nxt_node as usize].push(node);
            } else if nxt_cost == distance[nxt_node as usize] {
                pq.push_increase(nxt_node, Reverse(OrderedFloat(nxt_cost)));
                back_pointers[nxt_node as usize].push(node);
            }
            poison.check()?;
        }

        goals_remaining.visit(node);
        if goals_remaining.is_exhausted() {
            break;
        }
    }

    let ret = goals
        .iter(edges.node_count())
        .flat_map(|target| {
            let cost = distance[target as usize];
            if !cost.is_finite() {
                vec![(target, cost, vec![])]
            } else {
                struct CollectPath {
                    collected: Vec<(u32, f32, Vec<u32>)>,
                }

                impl CollectPath {
                    fn collect(
                        &mut self,
                        chain: &[u32],
                        start: u32,
                        target: u32,
                        cost: f32,
                        back_pointers: &[SmallVec<[u32; 1]>],
                    ) {
                        let last = chain.last().unwrap();
                        let prevs = &back_pointers[*last as usize];
                        for nxt in prevs {
                            let mut ret = chain.to_vec();
                            ret.push(*nxt);
                            if *nxt == start {
                                ret.reverse();
                                self.collected.push((target, cost, ret));
                            } else {
                                self.collect(&ret, start, target, cost, back_pointers)
                            }
                        }
                    }
                }
                let mut cp = CollectPath { collected: vec![] };
                cp.collect(&[target], start, target, cost, &back_pointers);
                cp.collected
            }
        })
        .collect_vec();

    Ok(ret)
}
