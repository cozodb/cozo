use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct RandomWalk;

impl AlgoImpl for RandomWalk {
    fn run(
        &mut self,
        tx: &SessionTx,
        rels: &[MagicAlgoRuleArg],
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = rels
            .get(0)
            .ok_or_else(|| anyhow!("'random_walk' requires edges relation as first argument"))?;
        let nodes = rels
            .get(1)
            .ok_or_else(|| anyhow!("'random_walk' requires nodes relation as second argument"))?;
        let starting = rels
            .get(2)
            .ok_or_else(|| anyhow!("'random_walk' requires starting relation as third argument"))?;
        let iterations = match opts.get("iterations") {
            None => 1usize,
            Some(Expr::Const(DataValue::Number(n))) => {
                let n = n.get_int().ok_or_else(|| {
                    anyhow!(
                        "'iterations' for 'random_walk' requires an integer, got {}",
                        n
                    )
                })?;
                ensure!(
                    n > 0,
                    "'iterations' for 'random_walk' must be positive, got {}",
                    n
                );
                n as usize
            }
            Some(v) => bail!(
                "'iterations' for 'random_walk' requires an integer, got {:?}",
                v
            ),
        };
        let steps = match opts
            .get("steps")
            .ok_or_else(|| anyhow!("'random_walk' requires option 'steps'"))?
        {
            Expr::Const(DataValue::Number(n)) => {
                let n = n.get_int().ok_or_else(|| {
                    anyhow!("'steps' for 'random_walk' requires an integer, got {}", n)
                })?;
                ensure!(
                    n > 0,
                    "'iterations' for 'random_walk' must be positive, got {}",
                    n
                );
                n as usize
            }
            v => bail!(
                "'iterations' for 'random_walk' requires an integer, got {:?}",
                v
            ),
        };

        let mut maybe_weight = opts.get("weight").cloned();
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
            let start_node_key = start_node
                .0
                .get(0)
                .ok_or_else(|| anyhow!("starting node relation too short"))?;
            let starting_tuple = nodes
                .prefix_iter(start_node_key, tx, stores)?
                .next()
                .ok_or_else(|| anyhow!("node with key '{:?}' not found", start_node_key))??;
            for _ in 0..iterations {
                counter += 1;
                let mut current_tuple = starting_tuple.clone();
                let mut path = vec![start_node_key.clone()];
                for _ in 0..steps {
                    let cur_node_key = current_tuple
                        .0
                        .get(0)
                        .ok_or_else(|| anyhow!("node tuple too short"))?;
                    let candidate_steps: Vec<_> =
                        edges.prefix_iter(cur_node_key, tx, stores)?.try_collect()?;
                    if candidate_steps.is_empty() {
                        break;
                    }
                    let next_step = if let Some(weight_expr) = &maybe_weight {
                        let weights: Vec<_> = candidate_steps.iter().map(|t| -> Result<f64> {
                           let mut cand = current_tuple.clone();
                            cand.0.extend_from_slice(&t.0);
                            Ok(match weight_expr.eval(&cand)? {
                                DataValue::Number(n) => {
                                    let f = n.get_float();
                                    ensure!(f >= 0., "'weight' for 'random_walk' needs to be non-negative, got {:?}", f);
                                    f
                                }
                                v => bail!("'weight' for 'random_walk' must evaluate to a float, got {:?}", v)
                            })
                        }).try_collect()?;
                        let dist = WeightedIndex::new(&weights).unwrap();
                        &candidate_steps[dist.sample(&mut rng)]
                    } else {
                        candidate_steps.choose(&mut rng).unwrap()
                    };
                    let next_node = next_step
                        .0
                        .get(1)
                        .ok_or_else(|| anyhow!("edges relation for 'random_walk' too short"))?;
                    path.push(next_node.clone());
                    current_tuple = nodes
                        .prefix_iter(next_node, tx, stores)?
                        .next()
                        .ok_or_else(|| {
                            anyhow!("node with key '{:?}' not found", start_node_key)
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
