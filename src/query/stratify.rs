use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use itertools::Itertools;

use crate::data::keyword::{Keyword, PROG_ENTRY};
use crate::query::compile::{Atom, DatalogProgram, RuleSet};
use crate::query::graph::{reachable_components, strongly_connected_components, Graph, StratifiedGraph, generalized_kahn};

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
            Atom::Conjunction(args) | Atom::Disjunction(args) => {
                let mut ret: BTreeMap<&Keyword, bool> = Default::default();
                for arg in args {
                    for (k, v) in arg.contained_rules() {
                        match ret.entry(k) {
                            Entry::Vacant(e) => {
                                e.insert(v);
                            }
                            Entry::Occupied(mut e) => {
                                let old = *e.get();
                                e.insert(old || v);
                            }
                        }
                    }
                }
                ret
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

fn verify_no_cycle(g: &StratifiedGraph<&'_ Keyword>, sccs: Vec<BTreeSet<&Keyword>>) -> Result<()> {
    for (k, vs) in g {
        for scc in &sccs {
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

pub(crate) fn stratify_program(prog: DatalogProgram) -> Result<Vec<DatalogProgram>> {
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
        .filter(|(k, _)| !reachable.contains(k))
        .collect();
    let graph: Graph<_> = graph
        .into_iter()
        .filter(|(k, _)| !reachable.contains(k))
        .collect();
    // 3. find SCC of the clauses
    let sccs: Vec<BTreeSet<&Keyword>> = strongly_connected_components(&graph)
        .into_iter()
        .map(|scc| scc.into_iter().cloned().collect())
        .collect_vec();
    // 4. for each SCC, verify that no neg/agg edges are present so that it is really stratifiable
    verify_no_cycle(&stratified_graph, sccs)?;
    // 5. build a reduced graph for the SCC's
    // 6. topological sort the reduced graph to get a stratification
    // 7. translate the stratification into datalog program
    todo!()
}
