use std::cmp::Reverse;

use ordered_float::OrderedFloat;
use priority_queue::PriorityQueue;
use rayon::prelude::*;

pub(crate) fn dijkstra_cost_only(edges: &[Vec<(usize, f64)>], start: usize) -> Vec<f64> {
    let mut distance = vec![f64::INFINITY; edges.len()];
    let mut pq = PriorityQueue::new();
    let mut back_pointers = vec![usize::MAX; edges.len()];
    distance[start] = 0.;
    pq.push(start, Reverse(OrderedFloat(0.)));

    while let Some((node, Reverse(OrderedFloat(cost)))) = pq.pop() {
        if cost > distance[node] {
            continue;
        }

        for (nxt_node, path_weight) in &edges[node] {
            let nxt_cost = cost + *path_weight;
            if nxt_cost < distance[*nxt_node] {
                pq.push_increase(*nxt_node, Reverse(OrderedFloat(nxt_cost)));
                distance[*nxt_node] = nxt_cost;
                back_pointers[*nxt_node] = node;
            }
        }
    }

    distance
}

pub(crate) fn apsp(edges: &[Vec<(usize, f64)>]) -> Vec<Vec<f64>> {
    (0..edges.len())
        .into_par_iter()
        .map(|start| dijkstra_cost_only(edges, start))
        .collect()
}
