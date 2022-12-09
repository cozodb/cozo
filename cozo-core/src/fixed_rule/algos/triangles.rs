/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use miette::Result;
#[cfg(feature = "rayon")]
use rayon::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::fixed_rule::{FixedRule, FixedRulePayload};
use crate::data::expr::Expr;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
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
        let (graph, indices, _) = edges.convert_edge_to_graph(true)?;
        let graph: Vec<BTreeSet<usize>> =
            graph.into_iter().map(|e| e.into_iter().collect()).collect();
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
    graph: &[BTreeSet<usize>],
    poison: Poison,
) -> Result<Vec<(f64, usize, usize)>> {
    #[cfg(feature = "rayon")]
    let it = graph.par_iter();
    #[cfg(not(feature = "rayon"))]
    let it = graph.iter();

    it.map(|edges| -> Result<(f64, usize, usize)> {
        let degree = edges.len();
        if degree < 2 {
            Ok((0., 0, degree))
        } else {
            let n_triangles = edges
                .iter()
                .map(|e_src| {
                    edges
                        .iter()
                        .filter(|e_dst| e_src > e_dst && graph[*e_src].contains(*e_dst))
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
