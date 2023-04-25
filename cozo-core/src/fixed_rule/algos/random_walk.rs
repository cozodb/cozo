/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{bail, ensure, Result};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::{eval_bytecode, Expr};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{BadExprValueError, FixedRule, FixedRulePayload, NodeNotFoundError};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct RandomWalk;

impl FixedRule for RandomWalk {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?.ensure_min_len(2)?;
        let nodes = payload.get_input(1)?;
        let starting = payload.get_input(2)?;
        let iterations = payload.pos_integer_option("iterations", Some(1))?;
        let steps = payload.pos_integer_option("steps", None)?;

        let mut maybe_weight = payload.expr_option("weight", None).ok();
        let mut maybe_weight_bytecode = None;
        if let Some(weight) = &mut maybe_weight {
            let mut nodes_binding = nodes.get_binding_map(0);
            let nodes_arity = nodes.arity()?;
            let edges_binding = edges.get_binding_map(nodes_arity);
            nodes_binding.extend(edges_binding);
            weight.fill_binding_indices(&nodes_binding)?;
            maybe_weight_bytecode = Some((weight.compile()?, weight.span()));
        }
        let maybe_weight_bytecode = maybe_weight_bytecode;
        let mut stack = vec![];

        let mut counter = 0i64;
        let mut rng = thread_rng();
        for start_node in starting.iter()? {
            let start_node = start_node?;
            let start_node_key = &start_node[0];
            let starting_tuple =
                nodes
                    .prefix_iter(start_node_key)?
                    .next()
                    .ok_or_else(|| NodeNotFoundError {
                        missing: start_node_key.clone(),
                        span: starting.span(),
                    })??;
            for _ in 0..iterations {
                counter += 1;
                let mut current_tuple = starting_tuple.clone();
                let mut path = vec![start_node_key.clone()];
                for _ in 0..steps {
                    let cur_node_key = &current_tuple[0];
                    let candidate_steps: Vec<_> = edges.prefix_iter(cur_node_key)?.try_collect()?;
                    if candidate_steps.is_empty() {
                        break;
                    }
                    let next_step = if let Some((weight_expr, span)) = &maybe_weight_bytecode {
                        let weights: Vec<_> = candidate_steps
                            .iter()
                            .map(|t| -> Result<f64> {
                                let mut cand = current_tuple.clone();
                                cand.extend_from_slice(t);
                                Ok(match eval_bytecode(weight_expr, &cand, &mut stack)? {
                                    DataValue::Num(n) => {
                                        let f = n.get_float();
                                        ensure!(
                                            f >= 0.,
                                            BadExprValueError(
                                                DataValue::from(f),
                                                *span,
                                                "'weight' must evaluate to a non-negative number"
                                                    .to_string()
                                            )
                                        );
                                        f
                                    }
                                    v => bail!(BadExprValueError(
                                        v,
                                        *span,
                                        "'weight' must evaluate to a non-negative number"
                                            .to_string()
                                    )),
                                })
                            })
                            .try_collect()?;
                        let dist = WeightedIndex::new(&weights).unwrap();
                        &candidate_steps[dist.sample(&mut rng)]
                    } else {
                        candidate_steps.choose(&mut rng).unwrap()
                    };
                    let next_node = &next_step[1];
                    path.push(next_node.clone());
                    current_tuple = nodes.prefix_iter(next_node)?.next().ok_or_else(|| {
                        NodeNotFoundError {
                            missing: next_node.clone(),
                            span: nodes.span(),
                        }
                    })??;
                    poison.check()?;
                }
                out.put(vec![
                    DataValue::from(counter),
                    start_node_key.clone(),
                    DataValue::List(path),
                ]);
            }
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
