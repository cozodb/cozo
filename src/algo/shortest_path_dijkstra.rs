use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use anyhow::{anyhow, bail, Result};
use either::{Left, Right};
use itertools::Itertools;

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct ShortestPathDijkstra;

impl AlgoImpl for ShortestPathDijkstra {
    fn run(
        &mut self,
        tx: &mut SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<Symbol, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'shortest_path_dijkstra' requires edges relation"))?;
        let starting = rels.get(1).ok_or_else(|| {
            anyhow!("'shortest_path_dijkstra' requires starting relation as second argument")
        })?;
        let termination = rels.get(2);
        let allow_negative_edges = match opts.get(&Symbol::from("allow_negative_edges")) {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!("option 'allow_negative_edges' for 'shortest_path_dijkstra' requires a boolean, got {:?}", v)
        };
        let undirected = match opts.get(&Symbol::from("undirected")) {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'undirected' for 'shortest_path_dijkstra' requires a boolean, got {:?}",
                v
            ),
        };

        let (graph, indices, inv_indices) =
            edges.convert_edge_to_weighted_graph(undirected, allow_negative_edges, tx, stores)?;

        let mut starting_nodes = BTreeSet::new();
        for tuple in starting.iter(tx, stores)? {
            let tuple = tuple?;
            let node = tuple
                .0
                .get(0)
                .ok_or_else(|| anyhow!("node relation too short"))?;
            if let Some(idx) = inv_indices.get(node) {
                starting_nodes.insert(*idx);
            }
        }
        let termination_nodes = match termination {
            None => None,
            Some(t) => {
                let mut tn = BTreeSet::new();
                for tuple in t.iter(tx, stores)? {
                    let tuple = tuple?;
                    let node = tuple
                        .0
                        .get(0)
                        .ok_or_else(|| anyhow!("node relation too short"))?;
                    if let Some(idx) = inv_indices.get(node) {
                        tn.insert(*idx);
                    }
                }
                Some(tn)
            }
        };

        for start in starting_nodes {
            let res = dijkstra(&graph, start, &termination_nodes);
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

        Ok(())
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

fn dijkstra(
    edges: &[Vec<(usize, f64)>],
    start: usize,
    maybe_goals: &Option<BTreeSet<usize>>,
) -> Vec<(usize, f64, Vec<usize>)> {
    let mut distance = vec![f64::INFINITY; edges.len()];
    let mut heap = BinaryHeap::new();
    let mut back_pointers = vec![usize::MAX; edges.len()];
    distance[start] = 0.;
    heap.push(HeapState {
        cost: 0.,
        node: start,
    });
    let mut goals_remaining = maybe_goals.clone();

    while let Some(state) = heap.pop() {
        if state.cost > distance[state.node] {
            continue;
        }

        for (nxt_node, path_weight) in &edges[state.node] {
            let nxt_cost = state.cost + *path_weight;
            if nxt_cost < distance[*nxt_node] {
                heap.push(HeapState {
                    cost: nxt_cost,
                    node: *nxt_node,
                });
                distance[*nxt_node] = nxt_cost;
                back_pointers[*nxt_node] = state.node;
            }
        }

        if let Some(goals) = &mut goals_remaining {
            if goals.remove(&state.node) {
                if goals.is_empty() {
                    break;
                }
            }
        }
    }

    let targets = if let Some(goals) = maybe_goals {
        Left(goals.iter().cloned())
    } else {
        Right(0..edges.len())
    };
    let ret = targets
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
