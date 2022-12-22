/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use graph::prelude::{DirectedCsrGraph, DirectedNeighbors, Graph};
use itertools::Itertools;
use miette::Result;
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct ClusteringCoefficients;

impl FixedRule for ClusteringCoefficients {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        poison: Poison,
    ) -> Result<()> {
        let edges = payload.get_input(0)?;
        let (graph, indices, _) = edges.as_directed_graph(true)?;
        let coefficients = clustering_coefficients(&graph, poison)?;
        for (idx, (cc, n_triangles, degree)) in coefficients.into_iter().enumerate() {
            out.put(vec![
                indices[idx].clone(),
                DataValue::from(cc),
                DataValue::from(n_triangles as i64),
                DataValue::from(degree as i64),
            ]);
        }

        Ok(())
    }

    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(4)
    }
}

fn clustering_coefficients(
    graph: &DirectedCsrGraph<u32>,
    poison: Poison,
) -> Result<Vec<(f64, usize, usize)>> {
    let node_size = graph.node_count();

    (0..node_size)
        .into_par_iter()
        .map(|node_idx| -> Result<(f64, usize, usize)> {
            let edges = graph.out_neighbors(node_idx).collect_vec();
            let degree = edges.len();
            if degree < 2 {
                Ok((0., 0, degree))
            } else {
                let n_triangles = edges
                    .iter()
                    .map(|e_src| {
                        edges
                            .iter()
                            .filter(|e_dst| {
                                if e_src <= e_dst {
                                    return false;
                                }
                                for nb in graph.out_neighbors(**e_src) {
                                    if nb == **e_dst {
                                        return true;
                                    }
                                }
                                false
                            })
                            .count()
                    })
                    .sum();
                let cc = 2. * n_triangles as f64 / ((degree as f64) * ((degree as f64) - 1.));
                poison.check()?;
                Ok((cc, n_triangles, degree))
            }
        })
        .collect::<Result<_>>()
}
