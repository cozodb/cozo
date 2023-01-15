/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use graph::prelude::{DirectedCsrGraph, DirectedNeighborsWithValues, Graph};
use std::cmp::Reverse;
use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct MinimumSpanningForestKruskal;

impl FixedRule for MinimumSpanningForestKruskal {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let (graph, indices, _) = edges.as_directed_weighted_graph(true, true)?;
        if graph.node_count() == 0 {
            return Ok(());
        }
        let msp = kruskal(&graph, poison)?;
        for (src, dst, cost) in msp {
            out.put(vec![
                indices[src as usize].clone(),
                indices[dst as usize].clone(),
                DataValue::from(cost as f64),
            ]);
        }

        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(3)
    }
}

fn kruskal(edges: &DirectedCsrGraph<u32, (), f32>, poison: Poison) -> Result<Vec<(u32, u32, f32)>> {
    let mut pq = PriorityQueue::new();
    let mut uf = UnionFind::new(edges.node_count());
    let mut mst = Vec::with_capacity((edges.node_count() - 1) as usize);
    for from in 0..edges.node_count() {
        for target in edges.out_neighbors_with_values(from) {
            let to = target.target;
            let cost = target.value;
            pq.push((from, to), Reverse(OrderedFloat(cost)));
        }
    }
    while let Some(((from, to), Reverse(OrderedFloat(cost)))) = pq.pop() {
        if uf.connected(from, to) {
            continue;
        }
        uf.union(from, to);

        mst.push((from, to, cost));
        if uf.szs[0] == edges.node_count() {
            break;
        }
        poison.check()?;
    }
    Ok(mst)
}

struct UnionFind {
    ids: Vec<u32>,
    szs: Vec<u32>,
}

impl UnionFind {
    fn new(n: u32) -> Self {
        Self {
            ids: (0..n).collect_vec(),
            szs: vec![1; n as usize],
        }
    }
    fn union(&mut self, p: u32, q: u32) {
        let root1 = self.find(p);
        let root2 = self.find(q);
        if root1 != root2 {
            if self.szs[root1 as usize] < self.szs[root2 as usize] {
                self.szs[root2 as usize] += self.szs[root1 as usize];
                self.ids[root1 as usize] = root2;
            } else {
                self.szs[root1 as usize] += self.szs[root2 as usize];
                self.ids[root2 as usize] = root1;
            }
        }
    }
    fn find(&mut self, mut p: u32) -> u32 {
        let mut root = p;
        while root != self.ids[root as usize] {
            root = self.ids[root as usize];
        }
        while p != root {
            let next = self.ids[p as usize];
            self.ids[p as usize] = root;
            p = next;
        }
        root
    }
    fn connected(&mut self, p: u32, q: u32) -> bool {
        self.find(p) == self.find(q)
    }
}
