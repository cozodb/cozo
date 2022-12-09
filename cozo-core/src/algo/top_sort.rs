/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::algo::{AlgoImpl, AlgoPayload};
use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct TopSort;

impl AlgoImpl for TopSort {
    fn run(
        &mut self,
        payload: AlgoPayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;

        let (graph, indices, _) = edges.convert_edge_to_graph(false)?;

        let sorted = kahn(&graph, poison)?;

        for (idx, val_id) in sorted.iter().enumerate() {
            let val = indices.get(*val_id).unwrap();
            let tuple = vec![DataValue::from(idx as i64), val.clone()];
            out.put(tuple);
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

pub(crate) fn kahn(graph: &[Vec<usize>], poison: Poison) -> Result<Vec<usize>> {
    let mut in_degree = vec![0; graph.len()];
    for tos in graph {
        for to in tos {
            in_degree[*to] += 1;
        }
    }
    let mut sorted = Vec::with_capacity(graph.len());
    let mut pending = vec![];

    for (node, degree) in in_degree.iter().enumerate() {
        if *degree == 0 {
            pending.push(node);
        }
    }

    while !pending.is_empty() {
        let removed = pending.pop().unwrap();
        sorted.push(removed);
        if let Some(edges) = graph.get(removed) {
            for nxt in edges {
                in_degree[*nxt] -= 1;
                if in_degree[*nxt] == 0 {
                    pending.push(*nxt);
                }
            }
        }
        poison.check()?;
    }

    Ok(sorted)
}
