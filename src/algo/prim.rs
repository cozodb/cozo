use std::cmp::Reverse;
use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct MinimumSpanningTreePrim;

impl AlgoImpl for MinimumSpanningTreePrim {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        _opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'minimum_spanning_tree_prim' requires edge relation"))?;
        let (graph, indices, _, _) =
            edges.convert_edge_to_weighted_graph(true, true, tx, stores)?;
        if graph.is_empty() {
            return Ok(());
        }
        let msp = prim(&graph);
        for (src, dst, cost) in msp {
            out.put(
                Tuple(vec![
                    indices[src].clone(),
                    indices[dst].clone(),
                    DataValue::from(cost),
                ]),
                0,
            );
        }
        Ok(())
    }
}

fn prim(graph: &[Vec<(usize, f64)>]) -> Vec<(usize, usize, f64)> {
    let mut visited = vec![false; graph.len()];
    let mut mst_edges = Vec::with_capacity(graph.len() - 1);
    let mut pq = PriorityQueue::new();

    let mut relax_edges_at_node = |node: usize, pq: &mut PriorityQueue<_, _>| {
        visited[node] = true;
        let edges = &graph[node];
        for (to_node, cost) in edges {
            if visited[*to_node] {
                continue;
            }
            pq.push_increase(*to_node, (Reverse(OrderedFloat(*cost)), node));
        }
    };

    relax_edges_at_node(0, &mut pq);

    while let Some((to_node, (Reverse(OrderedFloat(cost)), from_node))) = pq.pop() {
        if mst_edges.len() == graph.len() - 1 {
            break;
        }
        mst_edges.push((from_node, to_node, cost));
        relax_edges_at_node(to_node, &mut pq);
    }

    mst_edges
}
