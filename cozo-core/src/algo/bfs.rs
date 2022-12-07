/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use miette::{Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::{AlgoImpl, NodeNotFoundError};
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, NormalTempStore};
use crate::runtime::transact::SessionTx;

pub(crate) struct Bfs;

impl AlgoImpl for Bfs {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx<'_>,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, EpochStore>,
        out: &'a mut NormalTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation_with_min_len(0, 2, tx, stores)?;
        let nodes = algo.relation(1)?;
        let starting_nodes = algo.relation(2).unwrap_or(nodes);
        let limit = algo.pos_integer_option("limit", Some(1))?;
        let mut condition = algo.expr_option("condition", None)?;
        let binding_map = nodes.get_binding_map(0);
        condition.fill_binding_indices(&binding_map)?;
        let binding_indices = condition.binding_indices();
        let skip_query_nodes = binding_indices.is_subset(&BTreeSet::from([0]));

        let mut visited: BTreeSet<DataValue> = Default::default();
        let mut backtrace: BTreeMap<DataValue, DataValue> = Default::default();
        let mut found: Vec<(DataValue, DataValue)> = vec![];

        'outer: for node_tuple in starting_nodes.iter(tx, stores)? {
            let node_tuple = node_tuple?;
            let starting_node = &node_tuple[0];
            if visited.contains(starting_node) {
                continue;
            }
            visited.insert(starting_node.clone());

            let mut queue: VecDeque<DataValue> = VecDeque::default();
            queue.push_front(starting_node.clone());

            while let Some(candidate) = queue.pop_back() {
                for edge in edges.prefix_iter(&candidate, tx, stores)? {
                    let edge = edge?;
                    let to_node = &edge[1];
                    if visited.contains(to_node) {
                        continue;
                    }

                    visited.insert(to_node.clone());
                    backtrace.insert(to_node.clone(), candidate.clone());

                    let cand_tuple = if skip_query_nodes {
                        vec![to_node.clone()]
                    } else {
                        nodes
                            .prefix_iter(to_node, tx, stores)?
                            .next()
                            .ok_or_else(|| NodeNotFoundError {
                                missing: candidate.clone(),
                                span: nodes.span(),
                            })??
                    };

                    if condition.eval_pred(&cand_tuple)? {
                        found.push((starting_node.clone(), to_node.clone()));
                        if found.len() >= limit {
                            break 'outer;
                        }
                    }

                    queue.push_front(to_node.clone());
                    poison.check()?;
                }
            }
        }

        for (starting, ending) in found {
            let mut route = vec![];
            let mut current = ending.clone();
            while current != starting {
                route.push(current.clone());
                current = backtrace.get(&current).unwrap().clone();
            }
            route.push(starting.clone());
            route.reverse();
            let tuple = vec![starting, ending, DataValue::List(route)];
            out.put(tuple);
        }
        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(1)
    }
}
