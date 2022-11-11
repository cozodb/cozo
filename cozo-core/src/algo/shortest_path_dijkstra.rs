/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

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

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct ShortestPathDijkstra;

impl AlgoImpl for ShortestPathDijkstra {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
        out: &'a InMemRelation,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation(0)?;
        let starting = algo.relation(1)?;
        let termination = algo.relation(2);
        let undirected = algo.bool_option("undirected", Some(false))?;
        let keep_ties = algo.bool_option("keep_ties", Some(false))?;

        let (graph, indices, inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false, tx, stores)?;

        let mut starting_nodes = BTreeSet::new();
        for tuple in starting.iter(tx, stores)? {
            let tuple = tuple?;
            let node = &tuple.0[0];
            if let Some(idx) = inv_indices.get(node) {
                starting_nodes.insert(*idx);
            }
        }
        let termination_nodes = match termination {
            Err(_) => None,
            Ok(t) => {
                let mut tn = BTreeSet::new();
                for tuple in t.iter(tx, stores)? {
                    let tuple = tuple?;
                    let node = &tuple.0[0];
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
                        indices[start].clone(),
                        indices[target].clone(),
                        DataValue::from(cost),
                        DataValue::List(path.into_iter().map(|u| indices[u].clone()).collect_vec()),
                    ];
                    out.put(Tuple(t), 0)
                }
            }
        } else {
            let all_res: Vec<_> = starting_nodes
                .into_par_iter()
                .map(|start| -> Result<(usize, Vec<(usize, f64, Vec<usize>)>)> {
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
                        indices[start].clone(),
                        indices[target].clone(),
                        DataValue::from(cost),
                        DataValue::List(path.into_iter().map(|u| indices[u].clone()).collect_vec()),
                    ];
                    out.put(Tuple(t), 0)
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
    fn is_forbidden(&self, src: usize, dst: usize) -> bool;
}

impl ForbiddenEdge for () {
    fn is_forbidden(&self, _src: usize, _dst: usize) -> bool {
        false
    }
}

impl ForbiddenEdge for BTreeSet<(usize, usize)> {
    fn is_forbidden(&self, src: usize, dst: usize) -> bool {
        self.contains(&(src, dst))
    }
}

pub(crate) trait ForbiddenNode {
    fn is_forbidden(&self, node: usize) -> bool;
}

impl ForbiddenNode for () {
    fn is_forbidden(&self, _node: usize) -> bool {
        false
    }
}

impl ForbiddenNode for BTreeSet<usize> {
    fn is_forbidden(&self, node: usize) -> bool {
        self.contains(&node)
    }
}

pub(crate) trait Goal {
    fn is_exhausted(&self) -> bool;
    fn visit(&mut self, node: usize);
    fn iter(&self, total: usize) -> Box<dyn Iterator<Item = usize> + '_>;
}

impl Goal for () {
    fn is_exhausted(&self) -> bool {
        false
    }

    fn visit(&mut self, _node: usize) {}

    fn iter(&self, total: usize) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(0..total)
    }
}

impl Goal for Option<usize> {
    fn is_exhausted(&self) -> bool {
        self.is_none()
    }

    fn visit(&mut self, node: usize) {
        if let Some(u) = &self {
            if *u == node {
                self.take();
            }
        }
    }

    fn iter(&self, _total: usize) -> Box<dyn Iterator<Item = usize> + '_> {
        if let Some(u) = self {
            Box::new(iter::once(*u))
        } else {
            Box::new(iter::empty())
        }
    }
}

impl Goal for BTreeSet<usize> {
    fn is_exhausted(&self) -> bool {
        self.is_empty()
    }

    fn visit(&mut self, node: usize) {
        self.remove(&node);
    }

    fn iter(&self, _total: usize) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.iter().cloned())
    }
}

pub(crate) fn dijkstra<FE: ForbiddenEdge, FN: ForbiddenNode, G: Goal + Clone>(
    edges: &[Vec<(usize, f64)>],
    start: usize,
    goals: &G,
    forbidden_edges: &FE,
    forbidden_nodes: &FN,
) -> Vec<(usize, f64, Vec<usize>)> {
    let mut distance = vec![f64::INFINITY; edges.len()];
    let mut pq = PriorityQueue::new();
    let mut back_pointers = vec![usize::MAX; edges.len()];
    distance[start] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));
    let mut goals_remaining = goals.clone();

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node] {
            continue;
        }

        for (nxt_node, path_weight) in &edges[node] {
            if forbidden_nodes.is_forbidden(*nxt_node) {
                continue;
            }
            if forbidden_edges.is_forbidden(node, *nxt_node) {
                continue;
            }
            let nxt_cost = cost + *path_weight;
            if nxt_cost < distance[*nxt_node] {
                pq.push_increase(*nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[*nxt_node] = nxt_cost;
                back_pointers[*nxt_node] = node;
            }
        }

        goals_remaining.visit(node);
        if goals_remaining.is_exhausted() {
            break;
        }
    }

    let ret = goals
        .iter(edges.len())
        .map(|target| {
            let cost = distance[target];
            if !cost.is_finite() {
                (target, cost, vec![])
            } else {
                let mut path = vec![];
                let mut current = target;
                while current != start {
                    path.push(current);
                    current = back_pointers[current];
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
    edges: &[Vec<(usize, f64)>],
    start: usize,
    goals: &G,
    forbidden_edges: &FE,
    forbidden_nodes: &FN,
    poison: Poison,
) -> Result<Vec<(usize, f64, Vec<usize>)>> {
    let mut distance = vec![f64::INFINITY; edges.len()];
    let mut pq = PriorityQueue::new();
    let mut back_pointers: Vec<SmallVec<[usize; 1]>> = vec![smallvec![]; edges.len()];
    distance[start] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));
    let mut goals_remaining = goals.clone();

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node] {
            continue;
        }

        for (nxt_node, path_weight) in &edges[node] {
            if forbidden_nodes.is_forbidden(*nxt_node) {
                continue;
            }
            if forbidden_edges.is_forbidden(node, *nxt_node) {
                continue;
            }
            let nxt_cost = cost + *path_weight;
            if nxt_cost < distance[*nxt_node] {
                pq.push_increase(*nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[*nxt_node] = nxt_cost;
                back_pointers[*nxt_node].clear();
                back_pointers[*nxt_node].push(node);
            } else if nxt_cost == distance[*nxt_node] {
                pq.push_increase(*nxt_node, Reverse(OrderedFloat(nxt_cost)));
                back_pointers[*nxt_node].push(node);
            }
            poison.check()?;
        }

        goals_remaining.visit(node);
        if goals_remaining.is_exhausted() {
            break;
        }
    }

    let ret = goals
        .iter(edges.len())
        .flat_map(|target| {
            let cost = distance[target];
            if !cost.is_finite() {
                vec![(target, cost, vec![])]
            } else {
                struct CollectPath {
                    collected: Vec<(usize, f64, Vec<usize>)>,
                }

                impl CollectPath {
                    fn collect(
                        &mut self,
                        chain: &[usize],
                        start: usize,
                        target: usize,
                        cost: f64,
                        back_pointers: &[SmallVec<[usize; 1]>],
                    ) {
                        let last = chain.last().unwrap();
                        let prevs = &back_pointers[*last];
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
