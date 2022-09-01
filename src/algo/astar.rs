use std::cmp::Reverse;
use std::collections::BTreeMap;

use anyhow::{anyhow, ensure, Result};
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct ShortestPathAStar;

impl AlgoImpl for ShortestPathAStar {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'shortest_path_astar' requires edges relation"))?;
        let nodes = rels.get(1).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires nodes relation as second argument")
        })?;
        let starting = rels.get(2).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires starting relation as third argument")
        })?;
        let goals = rels.get(3).ok_or_else(|| {
            anyhow!("'shortest_path_astar' requires goal relation as fourth argument")
        })?;
        let mut heuristic = opts
            .get("heuristic")
            .ok_or_else(|| anyhow!("'heuristic' option required for 'shortest_path_astar'"))?
            .clone();

        let mut binding_map = nodes.get_binding_map(0);
        let goal_binding_map = goals.get_binding_map(nodes.arity(tx, stores)?);
        binding_map.extend(goal_binding_map);
        heuristic.fill_binding_indices(&binding_map)?;
        for start in starting.iter(tx, stores)? {
            let start = start?;
            for goal in goals.iter(tx, stores)? {
                let goal = goal?;
                let (cost, path) = astar(&start, &goal, edges, nodes, &heuristic, tx, stores, poison.clone())?;
                out.put(
                    Tuple(vec![
                        start.0[0].clone(),
                        goal.0[0].clone(),
                        DataValue::from(cost),
                        DataValue::List(path),
                    ]),
                    0,
                );
            }
        }

        Ok(())
    }
}

fn astar(
    starting: &Tuple,
    goal: &Tuple,
    edges: &MagicAlgoRuleArg,
    nodes: &MagicAlgoRuleArg,
    heuristic: &Expr,
    tx: &SessionTx,
    stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
    poison: Poison,
) -> Result<(f64, Vec<DataValue>)> {
    let start_node = starting
        .0
        .get(0)
        .ok_or_else(|| anyhow!("starting node too short"))?;
    let goal_node = goal
        .0
        .get(0)
        .ok_or_else(|| anyhow!("goal node too short"))?;
    let eval_heuristic = |node: &Tuple| -> Result<f64> {
        let mut v = node.0.clone();
        v.extend_from_slice(&goal.0);
        let t = Tuple(v);
        let cost = heuristic.eval(&t)?.get_float().ok_or_else(|| {
            anyhow!("heuristic function of 'shortest_path_astar' must return a float")
        })?;
        ensure!(
            !cost.is_nan(),
            "got cost NaN for heuristic function of 'shortest_path_astar'"
        );
        Ok(cost)
    };
    let mut back_trace: BTreeMap<DataValue, DataValue> = Default::default();
    let mut g_score: BTreeMap<DataValue, f64> = BTreeMap::from([(start_node.clone(), 0.)]);
    let mut open_set: PriorityQueue<DataValue, (Reverse<OrderedFloat<f64>>, usize)> =
        PriorityQueue::new();
    open_set.push(start_node.clone(), (Reverse(OrderedFloat(0.)), 0));
    let mut sub_priority: usize = 0;
    // let mut f_score: BTreeMap<DataValue, f64> =
    //     BTreeMap::from([(start_node.clone(), eval_heuristic(starting)?)]);
    while let Some((node, (Reverse(OrderedFloat(cost)), _))) = open_set.pop() {
        if node == *goal_node {
            let mut current = node;
            let mut ret = vec![];
            while current != *start_node {
                let prev = back_trace.get(&current).unwrap().clone();
                ret.push(current);
                current = prev;
            }
            ret.push(current);
            ret.reverse();
            return Ok((cost, ret));
        }

        for edge in edges.prefix_iter(&node, tx, stores)? {
            let edge = edge?;
            ensure!(
                edge.0.len() >= 3,
                "edge relation for 'shortest_path_astar' too short"
            );
            let edge_dst = &edge.0[1];
            let edge_cost = edge.0[2]
                .get_float()
                .ok_or_else(|| anyhow!("cost on edge for 'astar' must be a number"))?;
            ensure!(
                !edge_cost.is_nan(),
                "got cost NaN for edge of 'shortest_path_astar'"
            );

            let cost_to_src = g_score.get(&node).cloned().unwrap_or(f64::INFINITY);
            let tentative_cost_to_dst = cost_to_src + edge_cost;
            let prev_cost_to_dst = g_score.get(edge_dst).cloned().unwrap_or(f64::INFINITY);
            if tentative_cost_to_dst < prev_cost_to_dst {
                back_trace.insert(edge_dst.clone(), node.clone());
                g_score.insert(edge_dst.clone(), tentative_cost_to_dst);

                let edge_dst_tuple = nodes
                    .prefix_iter(edge_dst, tx, stores)?
                    .next()
                    .ok_or_else(|| {
                        anyhow!(
                            "node {:?} not found in nodes relation of 'shortest_path_astar'",
                            edge_dst
                        )
                    })??;

                let heuristic_cost = eval_heuristic(&edge_dst_tuple)?;
                sub_priority += 1;
                open_set.push_increase(
                    edge_dst.clone(),
                    (
                        Reverse(OrderedFloat(tentative_cost_to_dst + heuristic_cost)),
                        sub_priority,
                    ),
                );
            }
            poison.check()?;
        }
    }
    Ok((f64::INFINITY, vec![]))
}
