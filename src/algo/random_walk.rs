use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{bail, ensure, Result};
use rand::distributions::WeightedIndex;
use rand::prelude::*;

use crate::algo::{AlgoImpl, BadExprValueError, NodeNotFoundError};
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct RandomWalk;

impl AlgoImpl for RandomWalk {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        out: &InMemRelation,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation_with_min_len(0, 2, tx, stores)?;
        let nodes = algo.relation(1)?;
        let starting = algo.relation(2)?;
        let iterations = algo.pos_integer_option("iterations", Some(1))?;
        let steps = algo.pos_integer_option("steps", None)?;

        let mut maybe_weight = algo.expr_option("weight", None).ok();
        if let Some(weight) = &mut maybe_weight {
            let mut nodes_binding = nodes.get_binding_map(0);
            let nodes_arity = nodes.arity(tx, stores)?;
            let edges_binding = edges.get_binding_map(nodes_arity);
            nodes_binding.extend(edges_binding);
            weight.fill_binding_indices(&nodes_binding)?;
        }

        let mut counter = 0i64;
        let mut rng = thread_rng();
        for start_node in starting.iter(tx, stores)? {
            let start_node = start_node?;
            let start_node_key = &start_node.0[0];
            let starting_tuple = nodes
                .prefix_iter(start_node_key, tx, stores)?
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
                    let cur_node_key = &current_tuple.0[0];
                    let candidate_steps: Vec<_> =
                        edges.prefix_iter(cur_node_key, tx, stores)?.try_collect()?;
                    if candidate_steps.is_empty() {
                        break;
                    }
                    let next_step = if let Some(weight_expr) = &maybe_weight {
                        let weights: Vec<_> = candidate_steps
                            .iter()
                            .map(|t| -> Result<f64> {
                                let mut cand = current_tuple.clone();
                                cand.0.extend_from_slice(&t.0);
                                Ok(match weight_expr.eval(&cand)? {
                                    DataValue::Num(n) => {
                                        let f = n.get_float();
                                        ensure!(
                                            f >= 0.,
                                            BadExprValueError(
                                                DataValue::from(f),
                                                weight_expr.span(),
                                                "'weight' must evaluate to a non-negative number"
                                                    .to_string()
                                            )
                                        );
                                        f
                                    }
                                    v => bail!(BadExprValueError(
                                        v,
                                        weight_expr.span(),
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
                    let next_node = &next_step.0[1];
                    path.push(next_node.clone());
                    current_tuple = nodes
                        .prefix_iter(next_node, tx, stores)?
                        .next()
                        .ok_or_else(|| NodeNotFoundError {
                            missing: next_node.clone(),
                            span: nodes.span(),
                        })??;
                    poison.check()?;
                }
                out.put(
                    Tuple(vec![
                        DataValue::from(counter),
                        start_node_key.clone(),
                        DataValue::List(path),
                    ]),
                    0,
                );
            }
        }
        Ok(())
    }
}
