use std::cmp::Reverse;
use std::collections::BTreeMap;

use miette::Result;
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;

use crate::algo::AlgoImpl;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct MinimumSpanningTreePrim;

impl AlgoImpl for MinimumSpanningTreePrim {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation(0)?;
        let (graph, indices, _, _) =
            edges.convert_edge_to_weighted_graph(true, true, tx, stores)?;
        if graph.is_empty() {
            return Ok(());
        }
        let msp = prim(&graph, poison)?;
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

fn prim(graph: &[Vec<(usize, f64)>], poison: Poison) -> Result<Vec<(usize, usize, f64)>> {
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
        poison.check()?;
    }

    Ok(mst_edges)
}
