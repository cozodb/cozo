use std::collections::{BTreeMap, BTreeSet};

use miette::{miette, ensure, Result};
use itertools::Itertools;
use rayon::prelude::*;

use crate::algo::shortest_path_dijkstra::dijkstra;
use crate::algo::{get_bool_option_required, AlgoImpl};
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct KShortestPathYen;

impl AlgoImpl for KShortestPathYen {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let rels = &algo.rule_args;
        let opts = &algo.options;
        let edges = rels
            .get(0)
            .ok_or_else(|| miette!("'k_shortest_path_yen' requires edges relation"))?;
        let starting = rels.get(1).ok_or_else(|| {
            miette!("'k_shortest_path_yen' requires starting relation as second argument")
        })?;
        let termination = rels.get(2).ok_or_else(|| {
            miette!("'k_shortest_path_yen' requires termination relation as third argument")
        })?;
        let undirected =
            get_bool_option_required("undirected", opts, Some(false), "k_shortest_path_yen")?;
        let k = opts
            .get("k")
            .ok_or_else(|| miette!("option 'k' required for 'k_shortest_path_yen'"))?
            .get_const()
            .ok_or_else(|| miette!("option 'k' for 'k_shortest_path_yen' must be a constant"))?
            .get_int()
            .ok_or_else(|| miette!("option 'k' for 'k_shortest_path_yen' must be an integer"))?;
        ensure!(
            k > 1,
            "option 'k' for 'k_shortest_path_yen' must be greater than 1"
        );

        let (graph, indices, inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, false, tx, stores)?;

        let mut starting_nodes = BTreeSet::new();
        for tuple in starting.iter(tx, stores)? {
            let tuple = tuple?;
            let node = tuple
                .0
                .get(0)
                .ok_or_else(|| miette!("node relation too short"))?;
            if let Some(idx) = inv_indices.get(node) {
                starting_nodes.insert(*idx);
            }
        }
        let mut termination_nodes = BTreeSet::new();
        for tuple in termination.iter(tx, stores)? {
            let tuple = tuple?;
            let node = tuple
                .0
                .get(0)
                .ok_or_else(|| miette!("node relation too short"))?;
            if let Some(idx) = inv_indices.get(node) {
                termination_nodes.insert(*idx);
            }
        }
        if starting_nodes.len() <= 1 && termination_nodes.len() <= 1 {
            for start in starting_nodes {
                for goal in &termination_nodes {
                    for (cost, path) in k_shortest_path_yen(k as usize, &graph, start, *goal, poison.clone())? {
                        let t = vec![
                            indices[start].clone(),
                            indices[*goal].clone(),
                            DataValue::from(cost),
                            DataValue::List(
                                path.into_iter().map(|u| indices[u].clone()).collect_vec(),
                            ),
                        ];
                        out.put(Tuple(t), 0)
                    }
                }
            }
        } else {
            let res_all: Vec<_> = starting_nodes
                .iter()
                .flat_map(|start| termination_nodes.iter().map(|goal| (*start, *goal)))
                .par_bridge()
                .map(|(start, goal)| -> Result<(usize, usize, Vec<(f64, Vec<usize>)>)> {
                    Ok((
                        start,
                        goal,
                        k_shortest_path_yen(k as usize, &graph, start, goal, poison.clone())?,
                    ))
                })
                .collect::<Result<_>>()?;
            for (start, goal, res) in res_all {
                for (cost, path) in res {
                    let t = vec![
                        indices[start].clone(),
                        indices[goal].clone(),
                        DataValue::from(cost),
                        DataValue::List(path.into_iter().map(|u| indices[u].clone()).collect_vec()),
                    ];
                    out.put(Tuple(t), 0)
                }
            }
        }
        Ok(())
    }
}

fn k_shortest_path_yen(
    k: usize,
    edges: &[Vec<(usize, f64)>],
    start: usize,
    goal: usize,
    poison: Poison
) -> Result<Vec<(f64, Vec<usize>)>> {
    let mut k_shortest: Vec<(f64, Vec<usize>)> = Vec::with_capacity(k);
    let mut candidates: Vec<(f64, Vec<usize>)> = vec![];

    match dijkstra(edges, start, &Some(goal), &(), &())
        .into_iter()
        .next()
    {
        None => return Ok(k_shortest),
        Some((_, cost, path)) => k_shortest.push((cost, path)),
    }

    for _ in 1..k {
        let (_, prev_path) = k_shortest.last().unwrap();
        for i in 0..prev_path.len() - 1 {
            let spur_node = prev_path[i];
            let root_path = &prev_path[0..i + 1];
            let mut forbidden_edges = BTreeSet::new();
            for (_, p) in &k_shortest {
                if p.len() < root_path.len() + 1 {
                    continue;
                }
                let p_prefix = &p[0..i + 1];
                if p_prefix == root_path {
                    forbidden_edges.insert((p[i], p[i + 1]));
                }
            }
            let mut forbidden_nodes = BTreeSet::new();
            for node in &prev_path[0..i] {
                forbidden_nodes.insert(*node);
            }
            if let Some((_, spur_cost, spur_path)) = dijkstra(
                edges,
                spur_node,
                &Some(goal),
                &forbidden_edges,
                &forbidden_nodes,
            )
            .into_iter()
            .next()
            {
                let mut total_cost = spur_cost;
                for i in 0..root_path.len() - 1 {
                    let s = root_path[i];
                    let d = root_path[i + 1];
                    let eds = &edges[s];
                    for (e, c) in eds {
                        if *e == d {
                            total_cost += *c;
                            break;
                        }
                    }
                }
                let mut total_path = root_path.to_vec();
                total_path.pop();
                total_path.extend(spur_path);
                if candidates.iter().all(|(_, v)| *v != total_path) {
                    candidates.push((total_cost, total_path));
                }
                poison.check()?;
            }
        }
        if candidates.is_empty() {
            break;
        }
        candidates.sort_by(|(a_cost, _), (b_cost, _)| b_cost.total_cmp(a_cost));
        let shortest = candidates.pop().unwrap();
        k_shortest.push(shortest);
    }
    Ok(k_shortest)
}
