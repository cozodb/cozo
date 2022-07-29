use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use itertools::Itertools;

use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::query::compile::{Atom, DatalogProgram, RuleSet};
use crate::query::graph::{
    generalized_kahn, reachable_components, strongly_connected_components, Graph, StratifiedGraph,
};

#[derive(thiserror::Error, Debug)]
pub enum GraphError {
    #[error("every program requires an entry named '?'")]
    EntryNotFound,
    #[error("the rules #{0:?} form a cycle with negation/aggregation inside, which is unsafe")]
    GraphNotStratified(BTreeSet<Keyword>),
}

impl Atom {
    fn contained_rules(&self) -> BTreeMap<&Keyword, bool> {
        match self {
            Atom::AttrTriple(_) | Atom::Predicate(_) => Default::default(),
            Atom::Rule(r) => BTreeMap::from([(&r.name, false)]),
            Atom::Negation(a) => a
                .contained_rules()
                .into_iter()
                .map(|(k, is_neg)| (k, !is_neg))
                .collect(),
            Atom::Conjunction(_args) | Atom::Disjunction(_args) => {
                panic!("expect program in disjunctive normal form");
                // let mut ret: BTreeMap<&Keyword, bool> = Default::default();
                // for arg in args {
                //     for (k, v) in arg.contained_rules() {
                //         match ret.entry(k) {
                //             Entry::Vacant(e) => {
                //                 e.insert(v);
                //             }
                //             Entry::Occupied(mut e) => {
                //                 let old = *e.get();
                //                 e.insert(old || v);
                //             }
                //         }
                //     }
                // }
                // ret
            }
        }
    }
}

fn convert_program_to_graph(prog: &DatalogProgram) -> StratifiedGraph<&'_ Keyword> {
    prog.iter()
        .map(|(k, ruleset)| {
            let mut ret: BTreeMap<&Keyword, bool> = BTreeMap::default();
            for rule in &ruleset.rules {
                for atom in &rule.body {
                    let contained = atom.contained_rules();
                    for (found_key, negated) in contained {
                        match ret.entry(found_key) {
                            Entry::Vacant(e) => {
                                e.insert(negated);
                            }
                            Entry::Occupied(mut e) => {
                                let old = *e.get();
                                e.insert(old || negated);
                            }
                        }
                    }
                }
            }
            (k, ret)
        })
        .collect()
}

fn reduce_to_graph<'a>(g: &StratifiedGraph<&'a Keyword>) -> Graph<&'a Keyword> {
    g.iter()
        .map(|(k, s)| (*k, s.iter().map(|(sk, _)| *sk).collect_vec()))
        .collect()
}

fn verify_no_cycle(g: &StratifiedGraph<&'_ Keyword>, sccs: &[BTreeSet<&Keyword>]) -> Result<()> {
    for (k, vs) in g {
        for scc in sccs {
            if scc.contains(k) {
                for (v, negated) in vs {
                    if *negated && scc.contains(v) {
                        return Err(GraphError::GraphNotStratified(
                            scc.iter().cloned().cloned().collect(),
                        )
                        .into());
                    }
                }
            }
        }
    }
    Ok(())
}

fn make_scc_reduced_graph<'a>(
    sccs: &[BTreeSet<&'a Keyword>],
    graph: &StratifiedGraph<&Keyword>,
) -> (BTreeMap<&'a Keyword, usize>, StratifiedGraph<usize>) {
    let indices = sccs
        .iter()
        .enumerate()
        .flat_map(|(idx, scc)| scc.iter().map(move |k| (*k, idx)))
        .collect::<BTreeMap<_, _>>();
    let mut ret: BTreeMap<usize, BTreeMap<usize, bool>> = Default::default();
    for (from, tos) in graph {
        let from_idx = *indices.get(from).unwrap();
        let cur_entry = ret.entry(from_idx).or_default();
        for (to, poisoned) in tos {
            let to_idx = *indices.get(to).unwrap();
            if from_idx == to_idx {
                continue;
            }
            match cur_entry.entry(to_idx) {
                Entry::Vacant(e) => {
                    e.insert(*poisoned);
                }
                Entry::Occupied(mut e) => {
                    let old_p = *e.get();
                    e.insert(old_p || *poisoned);
                }
            }
        }
    }
    (indices, ret)
}

pub(crate) fn stratify_program(prog: &DatalogProgram) -> Result<Vec<DatalogProgram>> {
    // prerequisite: the program is already in disjunctive normal form
    // 0. build a graph of the program
    let prog_entry: &Keyword = &PROG_ENTRY;
    let stratified_graph = convert_program_to_graph(&prog);
    let graph = reduce_to_graph(&stratified_graph);
    if !graph.contains_key(prog_entry) {
        return Err(GraphError::EntryNotFound.into());
    }

    // 1. find reachable clauses starting from the query
    let reachable: BTreeSet<_> = reachable_components(&graph, &prog_entry)
        .into_iter()
        .map(|k| (*k).clone())
        .collect();
    // 2. prune the graph of unreachable clauses
    let stratified_graph: StratifiedGraph<_> = stratified_graph
        .into_iter()
        .filter(|(k, _)| reachable.contains(k))
        .collect();
    let graph: Graph<_> = graph
        .into_iter()
        .filter(|(k, _)| reachable.contains(k))
        .collect();
    // 3. find SCC of the clauses
    let sccs: Vec<BTreeSet<&Keyword>> = strongly_connected_components(&graph)
        .into_iter()
        .map(|scc| scc.into_iter().cloned().collect())
        .collect_vec();
    // 4. for each SCC, verify that no neg/agg edges are present so that it is really stratifiable
    verify_no_cycle(&stratified_graph, &sccs)?;
    // 5. build a reduced graph for the SCC's
    let (invert_indices, reduced_graph) = make_scc_reduced_graph(&sccs, &stratified_graph);
    // 6. topological sort the reduced graph to get a stratification
    let sort_result = generalized_kahn(&reduced_graph, stratified_graph.len());
    let n_strata  = sort_result.len();
    let invert_sort_result = sort_result.into_iter().enumerate().flat_map(|(stratum, indices)| {
        indices.into_iter().map(move |idx| (idx, stratum))
    }).collect::<BTreeMap<_, _>>();
    // 7. translate the stratification into datalog program
    let mut ret: Vec<DatalogProgram> = vec![Default::default(); n_strata];
    for (name, ruleset) in prog {
        if let Some(scc_idx) = invert_indices.get(&name) {
            let stratum_idx = *invert_sort_result.get(scc_idx).unwrap();
            let target = ret.get_mut(stratum_idx).unwrap();
            target.insert(name.clone(), ruleset.clone());
        }
    }

    Ok(ret)
}
