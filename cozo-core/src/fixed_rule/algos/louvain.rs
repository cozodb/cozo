/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use graph::prelude::{
    CsrLayout, DirectedCsrGraph, DirectedNeighborsWithValues, Graph, GraphBuilder,
};
use itertools::Itertools;
use log::debug;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct CommunityDetectionLouvain;

impl FixedRule for CommunityDetectionLouvain {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let undirected = payload.bool_option("undirected", Some(false))?;
        let max_iter = payload.pos_integer_option("max_iter", Some(10))?;
        let delta = payload.unit_interval_option("delta", Some(0.0001))? as f32;
        let keep_depth = payload.non_neg_integer_option("keep_depth", None).ok();

        let (graph, indices, _inv_indices) = edges.as_directed_weighted_graph(undirected, false)?;
        let result = louvain(&graph, delta, max_iter, poison)?;
        for (idx, node) in indices.into_iter().enumerate() {
            let mut labels = vec![];
            let mut cur_idx = idx as u32;
            for hierarchy in &result {
                let nxt_idx = hierarchy[cur_idx as usize];
                labels.push(DataValue::from(nxt_idx as i64));
                cur_idx = nxt_idx;
            }
            labels.reverse();
            if let Some(l) = keep_depth {
                labels.truncate(l);
            }
            out.put(vec![DataValue::List(labels), node]);
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

fn louvain(
    graph: &DirectedCsrGraph<u32, (), f32>,
    delta: f32,
    max_iter: usize,
    poison: Poison,
) -> Result<Vec<Vec<u32>>> {
    let mut current = graph;
    let mut collected = vec![];
    while current.node_count() > 2 {
        let (node2comm, new_graph) = louvain_step(current, delta, max_iter, poison.clone())?;
        debug!(
            "before size: {}, after size: {}",
            current.node_count(),
            new_graph.node_count()
        );
        if new_graph.node_count() == current.node_count() {
            break;
        }
        collected.push((node2comm, new_graph));
        current = &collected.last().unwrap().1;
    }
    Ok(collected.into_iter().map(|(a, _)| a).collect_vec())
}

fn calculate_delta(
    node: u32,
    target_community: u32,
    graph: &DirectedCsrGraph<u32, (), f32>,
    comm2nodes: &[BTreeSet<u32>],
    out_weights: &[f32],
    in_weights: &[f32],
    total_weight: f32,
) -> f32 {
    let mut sigma_out_total = 0.;
    let mut sigma_in_total = 0.;
    let mut d2comm = 0.;
    let target_community_members = &comm2nodes[target_community as usize];
    for member in target_community_members.iter() {
        if *member == node {
            continue;
        }
        sigma_out_total += out_weights[*member as usize];
        sigma_in_total += in_weights[*member as usize];
        for target in graph.out_neighbors_with_values(node) {
            if target.target == *member {
                d2comm += target.value;
                break;
            }
        }
        for target in graph.out_neighbors_with_values(*member) {
            if target.target == node {
                d2comm += target.value;
                break;
            }
        }
    }
    d2comm
        - (sigma_out_total * in_weights[node as usize]
            + sigma_in_total * out_weights[node as usize])
            / total_weight
}

fn louvain_step(
    graph: &DirectedCsrGraph<u32, (), f32>,
    delta: f32,
    max_iter: usize,
    poison: Poison,
) -> Result<(Vec<u32>, DirectedCsrGraph<u32, (), f32>)> {
    let n_nodes = graph.node_count();
    let mut total_weight = 0.;
    let mut out_weights = vec![0.; n_nodes as usize];
    let mut in_weights = vec![0.; n_nodes as usize];

    for from in 0..n_nodes {
        for target in graph.out_neighbors_with_values(from) {
            let to = target.target;
            let weight = target.value;

            total_weight += weight;
            out_weights[from as usize] += weight;
            in_weights[to as usize] += weight;
        }
    }

    let mut node2comm = (0..n_nodes).collect_vec();
    let mut comm2nodes = (0..n_nodes).map(|i| BTreeSet::from([i])).collect_vec();

    let mut last_modurality = f32::NEG_INFINITY;

    for _ in 0..max_iter {
        let modularity = {
            let mut modularity = 0.;
            for from in 0..n_nodes {
                for to in &comm2nodes[node2comm[from as usize] as usize] {
                    for target in graph.out_neighbors_with_values(from) {
                        if target.target == *to {
                            modularity += target.value;
                        }
                    }
                    modularity -=
                        in_weights[from as usize] * out_weights[*to as usize] / total_weight;
                }
            }
            modularity /= total_weight;
            debug!("modurality {}", modularity);
            modularity
        };
        if modularity <= last_modurality + delta {
            break;
        } else {
            last_modurality = modularity;
        }

        let mut moved = false;
        for node in 0..n_nodes {
            let community_for_node = node2comm[node as usize];

            let original_delta_q = calculate_delta(
                node,
                community_for_node,
                graph,
                &comm2nodes,
                &out_weights,
                &in_weights,
                total_weight,
            );
            let mut candidate_community = community_for_node;
            let mut best_improvement = 0.;

            let mut considered_communities = BTreeSet::from([community_for_node]);
            for target in graph.out_neighbors_with_values(node) {
                let to_node = target.target;

                let target_community = node2comm[to_node as usize];
                if target_community == community_for_node
                    || considered_communities.contains(&target_community)
                {
                    continue;
                }
                considered_communities.insert(target_community);

                let delta_q = calculate_delta(
                    node,
                    target_community,
                    graph,
                    &comm2nodes,
                    &out_weights,
                    &in_weights,
                    total_weight,
                );
                if delta_q - original_delta_q > best_improvement {
                    best_improvement = delta_q - original_delta_q;
                    candidate_community = target_community;
                }
            }
            if best_improvement > 0. {
                moved = true;
                node2comm[node as usize] = candidate_community;
                comm2nodes[community_for_node as usize].remove(&node);
                comm2nodes[candidate_community as usize].insert(node);
            }
            poison.check()?;
        }
        if !moved {
            break;
        }
    }
    let mut new_comm_indices: BTreeMap<u32, u32> = Default::default();
    let mut new_comm_count: u32 = 0;

    for temp_comm_idx in node2comm.iter_mut() {
        if let Some(new_comm_idx) = new_comm_indices.get(temp_comm_idx) {
            *temp_comm_idx = *new_comm_idx;
        } else {
            new_comm_indices.insert(*temp_comm_idx, new_comm_count);
            *temp_comm_idx = new_comm_count;
            new_comm_count += 1;
        }
    }

    let mut new_graph_list: Vec<BTreeMap<u32, f32>> =
        vec![BTreeMap::new(); new_comm_count as usize];
    for (node, comm) in node2comm.iter().enumerate() {
        let target = &mut new_graph_list[*comm as usize];
        for t in graph.out_neighbors_with_values(node as u32) {
            let to_node = t.target;
            let weight = t.value;
            let to_comm = node2comm[to_node as usize];
            *target.entry(to_comm).or_default() += weight;
        }
    }

    let new_graph: DirectedCsrGraph<u32, (), f32> = GraphBuilder::new()
        .csr_layout(CsrLayout::Sorted)
        .edges_with_values(
            new_graph_list
                .into_iter()
                .enumerate()
                .flat_map(move |(fr, nds)| {
                    nds.into_iter()
                        .map(move |(to, weight)| (fr as u32, to, weight))
                }),
        )
        .build();

    Ok((node2comm, new_graph))
}

#[cfg(test)]
mod tests {
    use graph::prelude::{CsrLayout, GraphBuilder};

    use crate::fixed_rule::algos::louvain::louvain;
    use crate::runtime::db::Poison;

    #[test]
    fn sample() {
        let graph: Vec<Vec<u32>> = vec![
            vec![2, 3, 5],           // 0
            vec![2, 4, 7],           // 1
            vec![0, 1, 4, 5, 6],     // 2
            vec![0, 7],              // 3
            vec![1, 2, 10],          // 4
            vec![0, 2, 7, 11],       // 5
            vec![2, 7, 11],          // 6
            vec![1, 3, 5, 6],        // 7
            vec![9, 10, 11, 12, 15], // 8
            vec![8, 12, 14],         // 9
            vec![4, 8, 12, 13, 14],  // 10
            vec![5, 6, 8, 13],       // 11
            vec![9, 10],             // 12
            vec![10, 11],            // 13
            vec![8, 9, 10],          // 14
            vec![8],                 // 15
        ];
        let graph = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges_with_values(
                graph
                    .into_iter()
                    .enumerate()
                    .flat_map(|(fr, tos)| tos.into_iter().map(move |to| (fr as u32, to, 1.))),
            )
            .build();
        louvain(&graph, 0., 100, Poison::default()).unwrap();
    }
}
