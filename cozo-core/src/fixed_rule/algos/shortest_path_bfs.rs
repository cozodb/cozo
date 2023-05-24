/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use itertools::Itertools;
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct ShortestPathBFS;

impl FixedRule for ShortestPathBFS {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?.ensure_min_len(2)?;
        let starting_nodes: Vec<_> = payload
            .get_input(1)?
            .ensure_min_len(1)?
            .iter()?
            .map_ok(|n| n.into_iter().next().unwrap())
            .try_collect()?;
        let ending_nodes: BTreeSet<_> = payload
            .get_input(2)?
            .ensure_min_len(1)?
            .iter()?
            .map_ok(|n| n.into_iter().next().unwrap())
            .try_collect()?;

        for starting_node in starting_nodes.iter() {
            let mut pending: BTreeSet<_> = ending_nodes.clone();
            let mut visited: BTreeSet<DataValue> = Default::default();
            let mut backtrace: BTreeMap<DataValue, DataValue> = Default::default();

            visited.insert(starting_node.clone());

            let mut queue: VecDeque<DataValue> = VecDeque::default();
            queue.push_front(starting_node.clone());

            while let Some(candidate) = queue.pop_back() {
                for edge in edges.prefix_iter(&candidate)? {
                    let edge = edge?;
                    let to_node = &edge[1];
                    if visited.contains(to_node) {
                        continue;
                    }

                    visited.insert(to_node.clone());
                    backtrace.insert(to_node.clone(), candidate.clone());

                    pending.remove(to_node);

                    if pending.is_empty() {
                        break;
                    }

                    queue.push_front(to_node.clone());
                }
            }

            for ending_node in ending_nodes.iter() {
                if backtrace.contains_key(ending_node) {
                    let mut route = vec![];
                    let mut current = ending_node.clone();
                    while current != *starting_node {
                        route.push(current.clone());
                        current = backtrace.get(&current).unwrap().clone();
                    }
                    route.push(starting_node.clone());
                    route.reverse();
                    let tuple = vec![
                        starting_node.clone(),
                        ending_node.clone(),
                        DataValue::List(route),
                    ];
                    out.put(tuple);
                } else {
                    out.put(vec![
                        starting_node.clone(),
                        ending_node.clone(),
                        DataValue::Null,
                    ])
                }
            }
            poison.check()?;
        }
        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(3)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::value::DataValue;

    use crate::DbInstance;

    #[test]
    fn test_bfs_path() {
        let db = DbInstance::default();
        let res = db
            .run_default(
                r#"
        love[loving, loved] <- [['alice', 'eve'],
                                ['bob', 'alice'],
                                ['eve', 'alice'],
                                ['eve', 'bob'],
                                ['eve', 'charlie'],
                                ['charlie', 'eve'],
                                ['david', 'george'],
                                ['george', 'george']]
        start[] <- [['alice']]
        end[] <- [['bob']]
        ?[fr, to, path] <~ ShortestPathBFS(love[], start[], end[])
        "#,
            )
            .unwrap()
            .rows;
        println!("{:?}", res);
        assert_eq!(res[0][2].get_slice().unwrap().len(), 3);
        let res = db
            .run_default(
                r#"
        love[loving, loved] <- [['alice', 'eve'],
                                ['bob', 'alice'],
                                ['eve', 'alice'],
                                ['eve', 'bob'],
                                ['eve', 'charlie'],
                                ['charlie', 'eve'],
                                ['david', 'george'],
                                ['george', 'george']]
        start[] <- [['alice']]
        end[] <- [['george']]
        ?[fr, to, path] <~ ShortestPathBFS(love[], start[], end[])
        "#,
            )
            .unwrap()
            .rows;
        assert_eq!(res[0][2], DataValue::Null);
        println!("{:?}", res);
    }
}
