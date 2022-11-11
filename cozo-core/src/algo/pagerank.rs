/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::collections::BTreeMap;
use std::mem;

use approx::AbsDiffEq;
use miette::Result;
use nalgebra::{Dynamic, OMatrix, U1};
use smartstring::{LazyCompact, SmartString};

use crate::algo::AlgoImpl;
use crate::data::expr::Expr;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct PageRank;

impl AlgoImpl for PageRank {
    fn run(
        &mut self,
        tx: &SessionTx,
        algo: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        out: &InMemRelation,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation(0)?;
        let undirected = algo.bool_option("undirected", Some(false))?;
        let theta = algo.unit_interval_option("theta", Some(0.8))? as f32;
        let epsilon = algo.unit_interval_option("epsilon", Some(0.05))? as f32;
        let iterations = algo.pos_integer_option("iterations", Some(20))?;
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

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(2)
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
