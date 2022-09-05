use std::collections::{BTreeMap, BTreeSet};

use miette::{ensure, miette, Result};

use crate::algo::AlgoImpl;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct Dfs;

impl AlgoImpl for Dfs {
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
        ensure!(
            rels.len() == 2 || rels.len() == 3,
            "'dfs' requires two or three input relations"
        );
        let edges = rels.get(0).unwrap();
        let nodes = rels.get(1).unwrap();
        let starting_nodes = if rels.len() == 3 {
            rels.get(2).unwrap()
        } else {
            nodes
        };
        let limit = if let Some(expr) = opts.get("limit") {
            let l = expr
                .get_const()
                .ok_or_else(|| {
                    miette!(
                        "argument 'limit' to 'dfs' must be a constant, got {:?}",
                        expr
                    )
                })?
                .get_int()
                .ok_or_else(|| {
                    miette!(
                        "argument 'limit' to 'dfs' must be an integer, got {:?}",
                        expr
                    )
                })?;
            ensure!(
                l > 0,
                "argument 'limit' to 'dfs' must be positive, got {}",
                l
            );
            l as usize
        } else {
            1
        };
        let mut condition = opts
            .get("condition")
            .ok_or_else(|| miette!("terminating 'condition' required for 'dfs'"))?
            .clone();
        let binding_map = nodes.get_binding_map(0);
        condition.fill_binding_indices(&binding_map)?;
        let binding_indices = condition.binding_indices();
        let skip_query_nodes = binding_indices.is_subset(&BTreeSet::from([0]));

        let mut visited: BTreeSet<DataValue> = Default::default();
        let mut backtrace: BTreeMap<DataValue, DataValue> = Default::default();
        let mut found: Vec<(DataValue, DataValue)> = vec![];

        'outer: for node_tuple in starting_nodes.iter(tx, stores)? {
            let node_tuple = node_tuple?;
            let starting_node = node_tuple
                .0
                .get(0)
                .ok_or_else(|| miette!("node tuple is empty"))?;
            if visited.contains(starting_node) {
                continue;
            }

            let mut stack: Vec<DataValue> = vec![];
            stack.push(starting_node.clone());

            while let Some(candidate) = stack.pop() {
                if visited.contains(&candidate) {
                    continue;
                }

                let cand_tuple = if skip_query_nodes {
                    Tuple(vec![candidate.clone()])
                } else {
                    nodes
                        .prefix_iter(&candidate, tx, stores)?
                        .next()
                        .ok_or_else(|| miette!("node with id {:?} not found", candidate))??
                };

                if condition.eval_pred(&cand_tuple)? {
                    found.push((starting_node.clone(), candidate.clone()));
                    if found.len() >= limit {
                        break 'outer;
                    }
                }

                visited.insert(candidate.clone());

                for edge in edges.prefix_iter(&candidate, tx, stores)? {
                    let edge = edge?;
                    let to_node = edge
                        .0
                        .get(1)
                        .ok_or_else(|| miette!("'edges' relation too short"))?;
                    if visited.contains(to_node) {
                        continue;
                    }
                    backtrace.insert(to_node.clone(), candidate.clone());
                    stack.push(to_node.clone());
                    poison.check()?;
                }
            }
        }

        for (starting, ending) in found {
            let mut route = vec![];
            let mut current = ending;
            while current != starting {
                route.push(current.clone());
                current = backtrace.get(&current).unwrap().clone();
            }
            route.push(starting);
            route.reverse();
            let tuple = Tuple(route);
            out.put(tuple, 0);
            poison.check()?;
        }
        Ok(())
    }
}
