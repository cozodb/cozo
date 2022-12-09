/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use log::debug;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::algo::{AlgoImpl, AlgoPayload};
use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct CommunityDetectionLouvain;

impl AlgoImpl for CommunityDetectionLouvain {
    fn run(
        &self,
        payload: AlgoPayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let undirected = payload.bool_option("undirected", Some(false))?;
        let max_iter = payload.pos_integer_option("max_iter", Some(10))?;
        let delta = payload.unit_interval_option("delta", Some(0.0001))?;
        let keep_depth = payload.non_neg_integer_option("keep_depth", None).ok();

        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false)?;
        let graph = graph
            .into_iter()
            .map(|edges| -> BTreeMap<usize, f64> {
                let mut m = BTreeMap::default();
                for (to, weight) in edges {
                    *m.entry(to).or_default() += weight;
                }
                m
            })
            .collect_vec();
        let result = louvain(&graph, delta, max_iter, poison)?;
        for (idx, node) in indices.into_iter().enumerate() {
            let mut labels = vec![];
            let mut cur_idx = idx;
            for hierarchy in &result {
                let nxt_idx = hierarchy[cur_idx];
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
    graph: &[BTreeMap<usize, f64>],
    delta: f64,
    max_iter: usize,
    poison: Poison,
) -> Result<Vec<Vec<usize>>> {
    let mut current = graph;
    let mut collected = vec![];
    while current.len() > 2 {
        let (node2comm, new_graph) = louvain_step(current, delta, max_iter, poison.clone())?;
        debug!(
            "before size: {}, after size: {}",
            current.len(),
            new_graph.len()
        );
        if new_graph.len() == current.len() {
            break;
        }
        collected.push((node2comm, new_graph));
        current = &collected.last().unwrap().1;
    }
    Ok(collected.into_iter().map(|(a, _)| a).collect_vec())
}

fn calculate_delta(
    node: usize,
    target_community: usize,
    graph: &[BTreeMap<usize, f64>],
    comm2nodes: &[BTreeSet<usize>],
    out_weights: &[f64],
    in_weights: &[f64],
    total_weight: f64,
) -> f64 {
    let mut sigma_out_total = 0.;
    let mut sigma_in_total = 0.;
    let mut d2comm = 0.;
    let target_community_members = &comm2nodes[target_community];
    for member in target_community_members.iter() {
        if *member == node {
            continue;
        }
        sigma_out_total += out_weights[*member];
        sigma_in_total += in_weights[*member];
        if let Some(weight) = graph[node].get(member) {
            d2comm += *weight;
        }
        if let Some(weight) = graph[*member].get(&node) {
            d2comm += *weight;
        }
    }
    d2comm
        - (sigma_out_total * in_weights[node] + sigma_in_total * out_weights[node]) / total_weight
}

fn louvain_step(
    graph: &[BTreeMap<usize, f64>],
    delta: f64,
    max_iter: usize,
    poison: Poison,
) -> Result<(Vec<usize>, Vec<BTreeMap<usize, f64>>)> {
    let n_nodes = graph.len();
    let mut total_weight = 0.;
    let mut out_weights = vec![0.; n_nodes];
    let mut in_weights = vec![0.; n_nodes];

    for (from, edges) in graph.iter().enumerate() {
        for (to, weight) in edges {
            total_weight += *weight;
            out_weights[from] += *weight;
            in_weights[*to] += *weight;
        }
    }

    let mut node2comm = (0..n_nodes).collect_vec();
    let mut comm2nodes = (0..n_nodes).map(|i| BTreeSet::from([i])).collect_vec();

    let mut last_modurality = f64::NEG_INFINITY;

    for _ in 0..max_iter {
        let modularity = {
            let mut modularity = 0.;
            for from in 0..n_nodes {
                for to in &comm2nodes[node2comm[from]] {
                    if let Some(weight) = graph[from].get(to) {
                        modularity += *weight;
                    }
                    modularity -= in_weights[from] * out_weights[*to] / total_weight;
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
        for (node, edges) in graph.iter().enumerate() {
            let community_for_node = node2comm[node];
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
            for to_node in edges.keys() {
                let target_community = node2comm[*to_node];
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
                node2comm[node] = candidate_community;
                comm2nodes[community_for_node].remove(&node);
                comm2nodes[candidate_community].insert(node);
            }
            poison.check()?;
        }
        if !moved {
            break;
        }
    }
    let mut new_comm_indices: BTreeMap<usize, usize> = Default::default();
    let mut new_comm_count: usize = 0;

    for temp_comm_idx in node2comm.iter_mut() {
        if let Some(new_comm_idx) = new_comm_indices.get(temp_comm_idx) {
            *temp_comm_idx = *new_comm_idx;
        } else {
            new_comm_indices.insert(*temp_comm_idx, new_comm_count);
            *temp_comm_idx = new_comm_count;
            new_comm_count += 1;
        }
    }

    let mut new_graph = vec![BTreeMap::new(); new_comm_count];
    for (node, comm) in node2comm.iter().enumerate() {
        let target = &mut new_graph[*comm];
        for (to_node, weight) in &graph[node] {
            let to_comm = node2comm[*to_node];
            *target.entry(to_comm).or_default() += weight;
        }
    }
    Ok((node2comm, new_graph))
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::algo::louvain::louvain;
    use crate::runtime::db::Poison;

    #[test]
    fn sample() {
        let graph: Vec<Vec<usize>> = vec![
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
        let graph = graph
            .into_iter()
            .map(|edges| edges.into_iter().map(|n| (n, 1.)).collect())
            .collect_vec();
        louvain(&graph, 0., 100, Poison::default()).unwrap();
    }
}
