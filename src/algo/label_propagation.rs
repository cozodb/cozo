use std::collections::BTreeMap;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;
use rand::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::algo::{get_bool_option_required, AlgoImpl};
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct LabelPropagation;

impl AlgoImpl for LabelPropagation {
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
            .ok_or_else(|| anyhow!("'label_propagation' requires edges relation"))?;
        let undirected =
            get_bool_option_required("undirected", opts, Some(false), "label_propagation")?;
        let max_iter = match opts.get("max_iter") {
            None => 10,
            Some(Expr::Const(DataValue::Number(n))) => {
                let i = n.get_int().ok_or_else(|| {
                    anyhow!(
                        "'max_iter' for 'label_propagation' requires an integer, got {:?}",
                        n
                    )
                })?;
                ensure!(
                    i >= 0,
                    "'max_iter' for 'label_propagation' must be positive, got {}",
                    i
                );
                i as usize
            }
            Some(n) => bail!(
                "'max_iter' for 'label_propagation' requires an integer, got {:?}",
                n
            ),
        };
        let (graph, indices, _inv_indices, _) =
            edges.convert_edge_to_weighted_graph(undirected, true, tx, stores)?;
        let labels = label_propagation(&graph, max_iter, poison)?;
        for (idx, label) in labels.into_iter().enumerate() {
            let node = indices[idx].clone();
            out.put(Tuple(vec![DataValue::from(label as i64), node]), 0);
        }
        Ok(())
    }
}

fn label_propagation(graph: &[Vec<(usize, f64)>], max_iter: usize, poison: Poison) -> Result<Vec<usize>> {
    let n_nodes = graph.len();
    let mut labels = (0..n_nodes).collect_vec();
    let mut rng = thread_rng();
    let mut iter_order = (0..n_nodes).collect_vec();
    for _ in 0..max_iter {
        iter_order.shuffle(&mut rng);
        let mut changed = false;
        for node in &iter_order {
            let mut labels_for_node: BTreeMap<usize, f64> = BTreeMap::new();
            let neighbours = &graph[*node];
            if neighbours.is_empty() {
                continue;
            }
            for (to_node, weight) in neighbours {
                let label = labels[*to_node];
                *labels_for_node.entry(label).or_default() += *weight;
            }
            let mut labels_by_score = labels_for_node.into_iter().collect_vec();
            labels_by_score.sort_by(|a, b| a.1.total_cmp(&b.1).reverse());
            let max_score = labels_by_score[0].1;
            let candidate_labels = labels_by_score
                .into_iter()
                .take_while(|(_, score)| *score == max_score)
                .map(|(l, _)| l)
                .collect_vec();
            let new_label = *candidate_labels.choose(&mut rng).unwrap();
            if new_label != labels[*node] {
                changed = true;
                labels[*node] = new_label;
            }
            poison.check()?;
        }
        if !changed {
            break;
        }
    }
    Ok(labels)
}
