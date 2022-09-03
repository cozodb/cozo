use std::collections::BTreeMap;
use std::mem;

use anyhow::{anyhow, bail, ensure, Result};
use approx::AbsDiffEq;
use nalgebra::{Dynamic, OMatrix, U1};
use smartstring::{LazyCompact, SmartString};

use crate::algo::{get_bool_option_required, AlgoImpl};
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoRuleArg, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::{DataValue, Num};
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct PageRank;

impl AlgoImpl for PageRank {
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
            .ok_or_else(|| anyhow!("'pagerank' requires edges relation"))?;
        let undirected = get_bool_option_required("undirected", opts, Some(false), "pagerank")?;
        let theta = match opts.get("theta") {
            None => 0.8f32,
            Some(Expr::Const(DataValue::Num(n))) => n.get_float() as f32,
            Some(v) => bail!(
                "option 'theta' for 'pagerank' requires a float, got {:?}",
                v
            ),
        };
        ensure!(
            0. <= theta && theta <= 1.,
            "'theta' in 'pagerank' out of range [0, 1]: {}",
            theta
        );
        let epsilon = match opts.get("epsilon") {
            None => 0.001f32,
            Some(Expr::Const(DataValue::Num(n))) => n.get_float() as f32,
            Some(v) => bail!(
                "option 'epsilon' for 'pagerank' requires a float, got {:?}",
                v
            ),
        };
        let iterations = match opts.get("iterations") {
            None => 20,
            Some(Expr::Const(DataValue::Num(Num::I(i)))) => *i,
            Some(v) => bail!(
                "option 'iterations' for 'pagerank' requires an integer, got {:?}",
                v
            ),
        };
        ensure!(
            iterations > 0,
            "'iterations' for 'pagerank' must be positive, got {}",
            iterations
        );
        let iterations = iterations as usize;
        let (graph, indices, _) = edges.convert_edge_to_graph(undirected, tx, stores)?;
        let res = pagerank(&graph, theta, epsilon, iterations, poison)?;
        for (idx, score) in res.iter().enumerate() {
            out.put(
                Tuple(vec![indices[idx].clone(), DataValue::from(*score as f64)]),
                0,
            );
        }
        Ok(())
    }
}

fn pagerank(
    edges: &[Vec<usize>],
    theta: f32,
    epsilon: f32,
    iterations: usize,
    poison: Poison,
) -> Result<OMatrix<f32, Dynamic, U1>> {
    let init_val = (1. - theta) / edges.len() as f32;
    let mut g_mat = OMatrix::<f32, Dynamic, Dynamic>::repeat(edges.len(), edges.len(), init_val);
    let n = edges.len();
    let empty_score = theta / n as f32;
    for (node, to_nodes) in edges.iter().enumerate() {
        let l = to_nodes.len();
        if l == 0 {
            for to_node in 0..n {
                g_mat[(node, to_node)] = empty_score;
            }
        } else {
            let score = theta / n as f32;
            for to_node in to_nodes {
                g_mat[(node, *to_node)] = score;
            }
        }
    }
    let mut pi_vec = OMatrix::<f32, Dynamic, U1>::repeat(edges.len(), 1.);
    let scale_target = (n as f32).sqrt();
    let mut last_pi_vec = pi_vec.clone();
    for _ in 0..iterations {
        mem::swap(&mut pi_vec, &mut last_pi_vec);
        pi_vec = g_mat.tr_mul(&last_pi_vec);
        pi_vec.normalize_mut();
        let f = pi_vec.norm() / scale_target;
        pi_vec.unscale_mut(f);

        if pi_vec.abs_diff_eq(&last_pi_vec, epsilon) {
            break;
        }
        poison.check()?;
    }
    Ok(pi_vec)
}
