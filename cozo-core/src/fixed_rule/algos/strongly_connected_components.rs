/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#![allow(unused_imports)]

use graph::prelude::{DirectedCsrGraph, DirectedNeighbors, Graph};
use std::cmp::min;
use std::collections::BTreeMap;

use itertools::Itertools;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::program::{MagicFixedRuleApply, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, RegularTempStore};
use crate::runtime::transact::SessionTx;

#[cfg(feature = "graph-algo")]
pub(crate) struct StronglyConnectedComponent {
    strong: bool,
}
#[cfg(feature = "graph-algo")]
impl StronglyConnectedComponent {
    pub(crate) fn new(strong: bool) -> Self {
        Self { strong }
    }
}

#[cfg(feature = "graph-algo")]
impl FixedRule for StronglyConnectedComponent {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;

        let (graph, indices, mut inv_indices) = edges.as_directed_graph(!self.strong)?;

        let tarjan = TarjanSccG::new(graph).run(poison)?;
        for (grp_id, cc) in tarjan.iter().enumerate() {
            for idx in cc {
                let val = indices.get(*idx as usize).unwrap();
                let tuple = vec![val.clone(), DataValue::from(grp_id as i64)];
                out.put(tuple);
            }
        }

        let mut counter = tarjan.len() as i64;

        if let Ok(nodes) = payload.get_input(1) {
            for tuple in nodes.iter()? {
                let tuple = tuple?;
                let node = tuple.into_iter().next().unwrap();
                if !inv_indices.contains_key(&node) {
                    inv_indices.insert(node.clone(), u32::MAX);
                    let tuple = vec![node, DataValue::from(counter)];
                    out.put(tuple);
                    counter += 1;
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
        Ok(2)
    }
}

pub(crate) struct TarjanSccG {
    graph: DirectedCsrGraph<u32>,
    id: u32,
    ids: Vec<Option<u32>>,
    low: Vec<u32>,
    on_stack: Vec<bool>,
    stack: Vec<u32>,
}

impl TarjanSccG {
    pub(crate) fn new(graph: DirectedCsrGraph<u32>) -> Self {
        let graph_size = graph.node_count();
        Self {
            graph,
            id: 0,
            ids: vec![None; graph_size as usize],
            low: vec![0; graph_size as usize],
            on_stack: vec![false; graph_size as usize],
            stack: vec![],
        }
    }
    pub(crate) fn run(mut self, poison: Poison) -> Result<Vec<Vec<u32>>> {
        for i in 0..self.graph.node_count() {
            if self.ids[i as usize].is_none() {
                self.dfs(i);
                poison.check()?;
            }
        }

        let mut low_map: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for (idx, grp) in self.low.into_iter().enumerate() {
            low_map.entry(grp).or_default().push(idx as u32);
        }

        Ok(low_map.into_values().collect_vec())
    }
    fn dfs(&mut self, at: u32) {
        self.stack.push(at);
        self.on_stack[at as usize] = true;
        self.id += 1;
        self.ids[at as usize] = Some(self.id);
        self.low[at as usize] = self.id;
        for to in self.graph.out_neighbors(at).cloned().collect_vec() {
            if self.ids[to as usize].is_none() {
                self.dfs(to);
            }
            if self.on_stack[to as usize] {
                self.low[at as usize] = min(self.low[at as usize], self.low[to as usize]);
            }
        }
        if self.ids[at as usize].unwrap() == self.low[at as usize] {
            while let Some(node) = self.stack.pop() {
                self.on_stack[node as usize] = false;
                self.low[node as usize] = self.ids[at as usize].unwrap();
                if node == at {
                    break;
                }
            }
        }
    }
}
