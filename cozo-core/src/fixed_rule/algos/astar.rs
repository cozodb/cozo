/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Reverse;
use std::collections::BTreeMap;

use miette::{ensure, Result};
use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::{eval_bytecode, Expr};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::fixed_rule::{
    BadExprValueError, FixedRule, FixedRuleInputRelation, FixedRulePayload, NodeNotFoundError,
};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct ShortestPathAStar;

impl FixedRule for ShortestPathAStar {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?.ensure_min_len(2)?;
        let nodes = payload.get_input(1)?;
        let starting = payload.get_input(2)?;
        let goals = payload.get_input(3)?;
        let mut heuristic = payload.expr_option("heuristic", None)?;

        let mut binding_map = nodes.get_binding_map(0);
        let goal_binding_map = goals.get_binding_map(nodes.arity()?);
        binding_map.extend(goal_binding_map);
        heuristic.fill_binding_indices(&binding_map)?;
        for start in starting.iter()? {
            let start = start?;
            for goal in goals.iter()? {
                let goal = goal?;
                let (cost, path) = astar(&start, &goal, edges, nodes, &heuristic, poison.clone())?;
                out.put(vec![
                    start[0].clone(),
                    goal[0].clone(),
                    DataValue::from(cost),
                    DataValue::List(path),
                ]);
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

fn astar(
    starting: &Tuple,
    goal: &Tuple,
    edges: FixedRuleInputRelation<'_, '_>,
    nodes: FixedRuleInputRelation<'_, '_>,
    heuristic: &Expr,
    poison: Poison,
) -> Result<(f64, Vec<DataValue>)> {
    let start_node = &starting[0];
    let goal_node = &goal[0];
    let heuristic_bytecode = heuristic.compile()?;
    let mut stack = vec![];
    let mut eval_heuristic = |node: &Tuple| -> Result<f64> {
        let mut v = node.clone();
        v.extend_from_slice(goal);
        let t = v;
        let cost_val = eval_bytecode(&heuristic_bytecode, &t, &mut stack)?;
        let cost = cost_val.get_float().ok_or_else(|| {
            BadExprValueError(
                cost_val,
                heuristic.span(),
                "a number is required".to_string(),
            )
        })?;
        ensure!(
            !cost.is_nan(),
            BadExprValueError(
                DataValue::from(cost),
                heuristic.span(),
                "a number is required".to_string(),
            )
        );
        Ok(cost)
    };
    let mut back_trace: BTreeMap<DataValue, DataValue> = Default::default();
    let mut g_score: BTreeMap<DataValue, f64> = BTreeMap::from([(start_node.clone(), 0.)]);
    let mut open_set: PriorityQueue<DataValue, (Reverse<OrderedFloat<f64>>, usize)> =
        PriorityQueue::new();
    open_set.push(start_node.clone(), (Reverse(OrderedFloat(0.)), 0));
    let mut sub_priority: usize = 0;
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

        for edge in edges.prefix_iter(&node)? {
            let edge = edge?;
            let edge_dst = &edge[1];
            let edge_cost = match edge.get(2) {
                None => 1.,
                Some(cost) => cost.get_float().ok_or_else(|| {
                    BadExprValueError(
                        edge_dst.clone(),
                        edges.span(),
                        "edge cost must be a number".to_string(),
                    )
                })?,
            };
            ensure!(
                !edge_cost.is_nan(),
                BadExprValueError(
                    edge_dst.clone(),
                    edges.span(),
                    "edge cost must be a number".to_string(),
                )
            );

            let cost_to_src = g_score.get(&node).cloned().unwrap_or(f64::INFINITY);
            let tentative_cost_to_dst = cost_to_src + edge_cost;
            let prev_cost_to_dst = g_score.get(edge_dst).cloned().unwrap_or(f64::INFINITY);
            if tentative_cost_to_dst < prev_cost_to_dst {
                back_trace.insert(edge_dst.clone(), node.clone());
                g_score.insert(edge_dst.clone(), tentative_cost_to_dst);

                let edge_dst_tuple =
                    nodes
                        .prefix_iter(edge_dst)?
                        .next()
                        .ok_or_else(|| NodeNotFoundError {
                            missing: edge_dst.clone(),
                            span: nodes.span(),
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
