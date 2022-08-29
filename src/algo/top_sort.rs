use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct TopSort;

impl AlgoImpl for TopSort {
    fn run(
        &mut self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        _opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'top_sort' missing edges relation"))?;

        let (graph, indices, _) = edges.convert_edge_to_graph(false, tx, stores)?;

        let sorted = kahn(&graph);

        for (idx, val_id) in sorted.iter().enumerate() {
            let val = indices.get(*val_id).unwrap();
            let tuple = Tuple(vec![DataValue::from(idx as i64), val.clone()]);
            out.put(tuple, 0);
        }

        Ok(())
    }
}

pub(crate) fn kahn(graph: &[Vec<usize>]) -> Vec<usize> {
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
    }

    sorted
}
