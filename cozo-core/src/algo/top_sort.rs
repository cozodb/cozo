/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::collections::BTreeMap;

use miette::Result;
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

pub(crate) struct TopSort;

impl AlgoImpl for TopSort {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx<'_>,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
        out: &'a InMemRelation,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation(0)?;

        let (graph, indices, _) = edges.convert_edge_to_graph(false, tx, stores)?;

        let sorted = kahn(&graph, poison)?;

        for (idx, val_id) in sorted.iter().enumerate() {
            let val = indices.get(*val_id).unwrap();
            let tuple = Tuple(vec![DataValue::from(idx as i64), val.clone()]);
            out.put(tuple, 0);
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
