use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct CommunityDetectionLouvain;

impl AlgoImpl for CommunityDetectionLouvain {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'community_detection_louvain' requires edges relation"))?;
        let undirected = match opts.get("undirected") {
            None => false,
            Some(Expr::Const(DataValue::Bool(b))) => *b,
            Some(v) => bail!(
                "option 'undirected' for 'community_detection_louvain' requires a boolean, got {:?}",
                v
            ),
        };
        let keep_depth = match opts.get("keep_depth") {
            None => None,
            Some(Expr::Const(DataValue::Number(n))) => Some({
                let i = n.get_int().ok_or_else(|| {
                    anyhow!(
                    "'keep_depth' for 'community_detection_louvain' requires an integer, got {:?}",
                    n
                )
                })?;
                ensure!(
                    i > 0,
                    "'keep_depth' for 'community_detection_louvain' must be positive, got {}",
                    i
                );
                i as usize
            }),
            Some(n) => bail!(
                "'keep_depth' for 'community_detection_louvain' requires an integer, got {:?}",
                n
            ),
        };
        let (graph, indices, _, _) =
            edges.convert_edge_to_weighted_graph(undirected, false, tx, stores)?;
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
        let result = louvain(&graph);
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
            out.put(Tuple(vec![node, DataValue::List(labels)]), 0);
        }

        Ok(())
    }
}

fn louvain(graph: &[BTreeMap<usize, f64>]) -> Vec<Vec<usize>> {
    let mut current = graph;
    let mut collected = vec![];
    loop {
        let (node2comm, new_graph) = louvain_step(current);
        if new_graph.len() == current.len() {
            break;
        }
        collected.push((node2comm, new_graph));
        current = &collected.last().unwrap().1;
    }
    collected.into_iter().map(|(a, _)| a).collect_vec()
}

fn louvain_step(graph: &[BTreeMap<usize, f64>]) -> (Vec<usize>, Vec<BTreeMap<usize, f64>>) {
    let n_nodes = graph.len();
    let mut total_weight = 0.;
    let mut out_weights = vec![0.; n_nodes];
    let mut in_weights = vec![0.; n_nodes];
    let mut back_graph = vec![BTreeMap::default(); n_nodes];

    for (from, edges) in graph.iter().enumerate() {
        for (to, weight) in edges {
            total_weight += *weight;
            out_weights[from] += *weight;
            in_weights[*to] += *weight;
            *back_graph[*to].entry(from).or_default() += *weight;
        }
    }

    let mut node2comm = (0..n_nodes).collect_vec();
    let mut comm2nodes = (0..n_nodes).map(|i| BTreeSet::from([i])).collect_vec();

    let mut last_loop_changed = true;

    while last_loop_changed {
        last_loop_changed = false;
        for (node, edges) in graph.iter().enumerate() {
            let d_in = in_weights[node];
            let d_out = out_weights[node];
            let community_for_node = node2comm[node];
            let mut candidate_community = community_for_node;
            let mut best_improvement = 0.;
            let mut considered_communities = BTreeSet::new();
            for to_node in edges.keys() {
                let target_community = node2comm[*to_node];
                if target_community == community_for_node
                    || considered_communities.contains(&target_community)
                {
                    continue;
                }
                considered_communities.insert(target_community);

                let target_community_members = &comm2nodes[*to_node];
                let mut sigma_in_total = 0.;
                let mut sigma_out_total = 0.;
                let mut d_comm = 0.;
                for member in target_community_members {
                    sigma_in_total += in_weights[*member];
                    sigma_out_total += out_weights[*member];
                    if let Some(weight) = graph[node].get(member) {
                        d_comm += weight;
                    }
                    if let Some(weight) = back_graph[node].get(member) {
                        d_comm += weight;
                    }
                }
                let delta_q = d_comm / total_weight
                    - (d_out * sigma_in_total + d_in * sigma_out_total)
                        / (total_weight * total_weight);
                if delta_q > best_improvement {
                    best_improvement = delta_q;
                    candidate_community = target_community;
                }
            }
            if best_improvement > 0. {
                // last_loop_changed = true;

                node2comm[node] = candidate_community;
                comm2nodes[community_for_node].remove(&node);
                comm2nodes[candidate_community].insert(node);
            }
        }
    }
    let mut new_comm_indices: BTreeMap<usize, usize> = Default::default();
    let mut new_comm_count: usize = 0;
    for temp_comm_idx in node2comm.iter_mut() {
        if let Some(new_comm_idx) = new_comm_indices.get(temp_comm_idx) {
            *temp_comm_idx = *new_comm_idx;
        } else {
            *temp_comm_idx = new_comm_count;
            new_comm_indices.insert(*temp_comm_idx, new_comm_count);
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
    (node2comm, new_graph)
}
