/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::collections::{BTreeMap, BTreeSet};

use miette::Result;
use rayon::prelude::*;
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

pub(crate) struct ClusteringCoefficients;

impl AlgoImpl for ClusteringCoefficients {
    fn run<'a>(
        &mut self,
        tx: &'a SessionTx,
        algo: &'a MagicAlgoApply,
        stores: &'a BTreeMap<MagicSymbol, InMemRelation>,
        out: &'a InMemRelation,
        poison: Poison,
    ) -> Result<()> {
        let edges = algo.relation(0)?;
        let (graph, indices, _) = edges.convert_edge_to_graph(true, tx, stores)?;
        let graph: Vec<BTreeSet<usize>> =
            graph.into_iter().map(|e| e.into_iter().collect()).collect();
        let coefficients = clustering_coefficients(&graph, poison)?;
        for (idx, (cc, n_triangles, degree)) in coefficients.into_iter().enumerate() {
            out.put(
                Tuple(vec![
                    indices[idx].clone(),
                    DataValue::from(cc),
                    DataValue::from(n_triangles as i64),
                    DataValue::from(degree as i64),
                ]),
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
        Ok(4)
    }
}

fn clustering_coefficients(
    graph: &[BTreeSet<usize>],
    poison: Poison,
) -> Result<Vec<(f64, usize, usize)>> {
    graph
        .par_iter()
        .map(|edges| -> Result<(f64, usize, usize)> {
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
