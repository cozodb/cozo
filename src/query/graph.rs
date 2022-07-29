use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;

struct TarjanScc<'a> {
    graph: &'a [Vec<usize>],
    id: usize,
    ids: Vec<Option<usize>>,
    low: Vec<usize>,
    on_stack: Vec<bool>,
    stack: Vec<usize>,
}

impl<'a> TarjanScc<'a> {
    pub(crate) fn new(graph: &'a [Vec<usize>]) -> Self {
        Self {
            graph,
            id: 0,
            ids: vec![None; graph.len()],
            low: vec![0; graph.len()],
            on_stack: vec![false; graph.len()],
            stack: vec![],
        }
    }
    pub(crate) fn run(mut self) -> Vec<Vec<usize>> {
        for i in 0..self.graph.len() {
            if self.ids[i].is_none() {
                self.dfs(i)
            }
        }
        self.low
            .into_iter()
            .enumerate()
            .group_by(|(_id, scc)| *scc)
            .into_iter()
            .map(|(_scc, args)| args.map(|(id, _scc)| id).collect_vec())
            .collect_vec()
    }
    fn dfs(&mut self, at: usize) {
        self.stack.push(at);
        self.on_stack[at] = true;
        self.id += 1;
        self.ids[at] = Some(self.id);
        self.low[at] = self.id;
        for to in &self.graph[at] {
            let to = *to;
            if self.ids[to].is_none() {
                self.dfs(to);
            }
            if self.on_stack[to] {
                self.low[at] = min(self.low[at], self.low[to]);
            }
        }
        if self.ids[at].unwrap() == self.low[at] {
            while let Some(node) = self.stack.pop() {
                self.on_stack[node] = false;
                self.low[node] = self.ids[at].unwrap();
                if node == at {
                    break;
                }
            }
        }
    }
}

pub(crate) type Graph<T> = BTreeMap<T, Vec<T>>;

pub(crate) fn strongly_connected_components<T>(graph: &Graph<T>) -> Vec<Vec<&T>>
where
    T: Ord,
{
    let indices = graph.keys().collect_vec();
    let invert_indices: BTreeMap<_, _> = indices
        .iter()
        .enumerate()
        .map(|(idx, k)| (*k, idx))
        .collect();
    let idx_graph = graph
        .values()
        .map(|vs| vs.iter().map(|v| invert_indices[v]).collect_vec())
        .collect_vec();
    TarjanScc::new(&idx_graph)
        .run()
        .into_iter()
        .map(|vs| vs.into_iter().map(|i| indices[i]).collect_vec())
        .collect_vec()
}

struct Reachable<'a, T> {
    graph: &'a Graph<T>,
}

impl<'a, T: Ord> Reachable<'a, T> {
    fn walk(&self, starting: &T, collected: &mut BTreeSet<&'a T>) {
        for el in self.graph.get(starting).unwrap() {
            if collected.insert(el) {
                self.walk(el, collected);
            }
        }
    }
}

pub(crate) fn reachable_components<'a, T: Ord>(
    graph: &'a Graph<T>,
    start: &'a T,
) -> BTreeSet<&'a T> {
    let mut collected = BTreeSet::from([start]);
    let worker = Reachable { graph };
    worker.walk(start, &mut collected);
    collected
}

pub(crate) type StratifiedGraph<T> = BTreeMap<T, BTreeMap<T, bool>>;

/// For this generalized Kahn's algorithm, graph edges can be labelled 'poisoned', so that no
/// stratum contains any poisoned edges within it.
/// the returned vector of vector is simultaneously a topological ordering and a
/// stratification, which is greedy with respect to the starting node.
pub(crate) fn generalized_kahn(
    graph: &StratifiedGraph<usize>,
    num_nodes: usize,
) -> Vec<Vec<usize>> {
    let mut in_degree = vec![0; num_nodes];
    for (_from, tos) in graph {
        for to in tos.keys() {
            in_degree[*to] += 1;
        }
    }
    let mut ret = vec![];
    let mut current_stratum = vec![];
    let mut safe_pending = vec![];
    let mut unsafe_nodes: BTreeSet<usize> = BTreeSet::new();

    for (node, degree) in in_degree.iter().enumerate() {
        if *degree == 0 {
            safe_pending.push(node);
        }
    }

    loop {
        if safe_pending.is_empty() && !unsafe_nodes.is_empty() {
            ret.push(current_stratum.clone());
            current_stratum.clear();
            for node in &unsafe_nodes {
                if in_degree[*node] == 0 {
                    safe_pending.push(*node);
                }
            }
            unsafe_nodes.clear();
        }
        if safe_pending.is_empty() {
            if !current_stratum.is_empty() {
                ret.push(current_stratum);
            }
            break;
        }
        let removed = safe_pending.pop().unwrap();
        current_stratum.push(removed);
        if let Some(edges) = graph.get(&removed) {
            for (nxt, poisoned) in edges {
                in_degree[*nxt] -= 1;
                if *poisoned {
                    unsafe_nodes.insert(*nxt);
                }
                if in_degree[*nxt] == 0 && !unsafe_nodes.contains(nxt) {
                    safe_pending.push(*nxt)
                }
            }
        }
    }
    ret
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::query::graph::{
        generalized_kahn, reachable_components, strongly_connected_components, StratifiedGraph,
    };

    #[test]
    fn test_scc() {
        let graph = BTreeMap::from([
            ("a", vec!["b"]),
            ("b", vec!["a", "c"]),
            ("c", vec!["a", "d", "e"]),
            ("d", vec!["e", "e", "e"]),
            ("e", vec![]),
            ("f", vec![]),
        ]);
        let scc = strongly_connected_components(&graph);
        dbg!(scc);
        let reachable = reachable_components(&graph, &"a");
        dbg!(reachable);

        let s_graph: StratifiedGraph<usize> = BTreeMap::from([
            (
                0,
                BTreeMap::from([(1, false), (2, false), (3, false), (4, true), (5, true)]),
            ),
            (1, BTreeMap::from([(6, false)])),
            (2, BTreeMap::from([(6, false)])),
            (3, BTreeMap::from([(6, true)])),
            (4, BTreeMap::from([(6, true)])),
            (5, BTreeMap::from([(6, false)])),
        ]);
        dbg!(generalized_kahn(&s_graph, 7));
    }
}
