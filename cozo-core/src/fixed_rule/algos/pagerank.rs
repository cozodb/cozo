/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

#[cfg(not(feature = "rayon"))]
use approx::AbsDiffEq;
use graph::prelude::{page_rank, PageRankConfig};
use miette::Result;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct PageRank;

impl FixedRule for PageRank {
    #[allow(unused_variables)]
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let undirected = payload.bool_option("undirected", Some(false))?;
        let theta = payload.unit_interval_option("theta", Some(0.85))? as f32;
        let epsilon = payload.unit_interval_option("epsilon", Some(0.0001))? as f32;
        let iterations = payload.pos_integer_option("iterations", Some(10))?;

        let (graph, indices, _) = edges.as_directed_graph(undirected)?;

        if indices.is_empty() {
            return Ok(());
        }

        let (ranks, _n_run, _) = page_rank(
            &graph,
            PageRankConfig::new(iterations, epsilon as f64, theta),
        );

        for (idx, score) in ranks.iter().enumerate() {
            out.put(vec![indices[idx].clone(), DataValue::from(*score as f64)]);
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

#[cfg(not(feature = "rayon"))]
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
        std::mem::swap(&mut pi_vec, &mut last_pi_vec);
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
