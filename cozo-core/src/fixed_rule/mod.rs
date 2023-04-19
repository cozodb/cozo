/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::sync::Arc;

use crossbeam::channel::{bounded, Receiver, Sender};
#[allow(unused_imports)]
use either::{Left, Right};
#[cfg(feature = "graph-algo")]
use graph::prelude::{CsrLayout, DirectedCsrGraph, GraphBuilder};
use itertools::Itertools;
use lazy_static::lazy_static;
use miette::IntoDiagnostic;
#[allow(unused_imports)]
use miette::{bail, ensure, Diagnostic, Report, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::program::{
    FixedRuleOptionNotFoundError, MagicFixedRuleApply, MagicFixedRuleRuleArg, MagicSymbol,
    WrongFixedRuleOptionError,
};
use crate::data::symb::Symbol;
use crate::data::tuple::TupleIter;
use crate::data::value::DataValue;
#[cfg(feature = "graph-algo")]
use crate::fixed_rule::algos::*;
use crate::fixed_rule::utilities::*;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, RegularTempStore};
use crate::runtime::transact::SessionTx;
use crate::NamedRows;

#[cfg(feature = "graph-algo")]
pub(crate) mod algos;
pub(crate) mod utilities;

/// Passed into implementation of fixed rule, can be used to obtain relation inputs and options
pub struct FixedRulePayload<'a, 'b> {
    pub(crate) manifest: &'a MagicFixedRuleApply,
    pub(crate) stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    pub(crate) tx: &'a SessionTx<'b>,
}

/// Represents an input relation during the execution of a fixed rule
#[derive(Copy, Clone)]
pub struct FixedRuleInputRelation<'a, 'b> {
    arg_manifest: &'a MagicFixedRuleRuleArg,
    stores: &'a BTreeMap<MagicSymbol, EpochStore>,
    tx: &'a SessionTx<'b>,
}

impl<'a, 'b> FixedRuleInputRelation<'a, 'b> {
    /// The arity of the input relation
    pub fn arity(&self) -> Result<usize> {
        self.arg_manifest.arity(self.tx, self.stores)
    }
    /// Ensure the input relation contains tuples of the given minimal length.
    pub fn ensure_min_len(self, len: usize) -> Result<Self> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Input relation to algorithm has insufficient arity")]
        #[diagnostic(help("Arity should be at least {0} but is {1}"))]
        #[diagnostic(code(algo::input_relation_bad_arity))]
        struct InputRelationArityError(usize, usize, #[label] SourceSpan);

        let arity = self.arg_manifest.arity(self.tx, self.stores)?;
        ensure!(
            arity >= len,
            InputRelationArityError(len, arity, self.arg_manifest.span())
        );
        Ok(self)
    }
    /// Get the binding map of the input relation
    pub fn get_binding_map(&self, offset: usize) -> BTreeMap<Symbol, usize> {
        self.arg_manifest.get_binding_map(offset)
    }
    /// Iterate the input relation
    pub fn iter(&self) -> Result<TupleIter<'a>> {
        Ok(match &self.arg_manifest {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = self.stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                Box::new(store.all_iter().map(|t| Ok(t.into_tuple())))
            }
            MagicFixedRuleRuleArg::Stored { name, valid_at, .. } => {
                let relation = self.tx.get_relation(name, false)?;
                if let Some(valid_at) = valid_at {
                    Box::new(relation.skip_scan_all(self.tx, *valid_at))
                } else {
                    Box::new(relation.scan_all(self.tx))
                }
            }
        })
    }
    /// Iterate the relation with the given single-value prefix
    pub fn prefix_iter(&self, prefix: &DataValue) -> Result<TupleIter<'_>> {
        Ok(match self.arg_manifest {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = self.stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                let t = vec![prefix.clone()];
                Box::new(store.prefix_iter(&t).map(|t| Ok(t.into_tuple())))
            }
            MagicFixedRuleRuleArg::Stored { name, valid_at, .. } => {
                let relation = self.tx.get_relation(name, false)?;
                let t = vec![prefix.clone()];
                if let Some(valid_at) = valid_at {
                    Box::new(relation.skip_scan_prefix(self.tx, &t, *valid_at))
                } else {
                    Box::new(relation.scan_prefix(self.tx, &t))
                }
            }
        })
    }
    /// Get the source span of the input relation. Useful for generating informative error messages.
    pub fn span(&self) -> SourceSpan {
        self.arg_manifest.span()
    }
    /// Convert the input relation into a directed graph.
    /// If `undirected` is true, then each edge in the input relation is treated as a pair
    /// of edges, one for each direction.
    ///
    /// Returns the graph, the vertices in a vector with the index the same as used in the graph,
    /// and the inverse vertex mapping.
    #[cfg(feature = "graph-algo")]
    pub fn as_directed_graph(
        &self,
        undirected: bool,
    ) -> Result<(
        DirectedCsrGraph<u32>,
        Vec<DataValue>,
        BTreeMap<DataValue, u32>,
    )> {
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, u32> = Default::default();
        let mut error: Option<Report> = None;
        let it = self.iter()?.filter_map(|r_tuple| match r_tuple {
            Ok(tuple) => {
                let mut tuple = tuple.into_iter();
                let from = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let to = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let from_idx = if let Some(idx) = inv_indices.get(&from) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(from.clone(), idx);
                    indices.push(from.clone());
                    idx
                };
                let to_idx = if let Some(idx) = inv_indices.get(&to) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(to.clone(), idx);
                    indices.push(to.clone());
                    idx
                };
                Some((from_idx, to_idx))
            }
            Err(err) => {
                error = Some(err);
                None
            }
        });
        let it = if undirected {
            Right(it.flat_map(|(f, t)| [(f, t), (t, f)]))
        } else {
            Left(it)
        };
        let graph: DirectedCsrGraph<u32> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges(it)
            .build();
        if let Some(err) = error {
            bail!(err)
        }
        Ok((graph, indices, inv_indices))
    }
    /// Convert the input relation into a directed weighted graph.
    /// If `undirected` is true, then each edge in the input relation is treated as a pair
    /// of edges, one for each direction.
    ///
    /// Returns the graph, the vertices in a vector with the index the same as used in the graph,
    /// and the inverse vertex mapping.
    #[cfg(feature = "graph-algo")]
    pub fn as_directed_weighted_graph(
        &self,
        undirected: bool,
        allow_negative_weights: bool,
    ) -> Result<(
        DirectedCsrGraph<u32, (), f32>,
        Vec<DataValue>,
        BTreeMap<DataValue, u32>,
    )> {
        let mut indices: Vec<DataValue> = vec![];
        let mut inv_indices: BTreeMap<DataValue, u32> = Default::default();
        let mut error: Option<Report> = None;
        let it = self.iter()?.filter_map(|r_tuple| match r_tuple {
            Ok(tuple) => {
                let mut tuple = tuple.into_iter();
                let from = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let to = match tuple.next() {
                    None => {
                        error = Some(NotAnEdgeError(self.span()).into());
                        return None;
                    }
                    Some(f) => f,
                };
                let from_idx = if let Some(idx) = inv_indices.get(&from) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(from.clone(), idx);
                    indices.push(from.clone());
                    idx
                };
                let to_idx = if let Some(idx) = inv_indices.get(&to) {
                    *idx
                } else {
                    let idx = indices.len() as u32;
                    inv_indices.insert(to.clone(), idx);
                    indices.push(to.clone());
                    idx
                };

                let weight = match tuple.next() {
                    None => 1.0,
                    Some(d) => match d.get_float() {
                        Some(f) => {
                            if !f.is_finite() {
                                error = Some(
                                    BadEdgeWeightError(
                                        d,
                                        self.arg_manifest
                                            .bindings()
                                            .get(2)
                                            .map(|s| s.span)
                                            .unwrap_or_else(|| self.span()),
                                    )
                                    .into(),
                                );
                                return None;
                            };

                            if f < 0. && !allow_negative_weights {
                                error = Some(
                                    BadEdgeWeightError(
                                        d,
                                        self.arg_manifest
                                            .bindings()
                                            .get(2)
                                            .map(|s| s.span)
                                            .unwrap_or_else(|| self.span()),
                                    )
                                    .into(),
                                );
                                return None;
                            }
                            f
                        }
                        None => {
                            error = Some(
                                BadEdgeWeightError(
                                    d,
                                    self.arg_manifest
                                        .bindings()
                                        .get(2)
                                        .map(|s| s.span)
                                        .unwrap_or_else(|| self.span()),
                                )
                                .into(),
                            );
                            return None;
                        }
                    },
                };

                Some((from_idx, to_idx, weight as f32))
            }
            Err(err) => {
                error = Some(err);
                None
            }
        });
        let it = if undirected {
            Right(it.flat_map(|(f, t, w)| [(f, t, w), (t, f, w)]))
        } else {
            Left(it)
        };
        let graph: DirectedCsrGraph<u32, (), f32> = GraphBuilder::new()
            .csr_layout(CsrLayout::Sorted)
            .edges_with_values(it)
            .build();

        if let Some(err) = error {
            bail!(err)
        }

        Ok((graph, indices, inv_indices))
    }
}

impl<'a, 'b> FixedRulePayload<'a, 'b> {
    /// Get the total number of input relations.
    pub fn inputs_count(&self) -> usize {
        self.manifest.relations_count()
    }
    /// Get the input relation at `idx`.
    pub fn get_input(&self, idx: usize) -> Result<FixedRuleInputRelation<'a, 'b>> {
        let arg_manifest = self.manifest.relation(idx)?;
        Ok(FixedRuleInputRelation {
            arg_manifest,
            stores: self.stores,
            tx: self.tx,
        })
    }
    /// Get the name of the current fixed rule
    pub fn name(&self) -> &str {
        &self.manifest.fixed_handle.name
    }
    /// Get the source span of the payloads. Useful for generating informative errors.
    pub fn span(&self) -> SourceSpan {
        self.manifest.span
    }
    /// Extract an expression option
    pub fn expr_option(&self, name: &str, default: Option<Expr>) -> Result<Expr> {
        match self.manifest.options.get(name) {
            Some(ex) => Ok(ex.clone()),
            None => match default {
                Some(ex) => Ok(ex),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }

    /// Extract a string option
    pub fn string_option(
        &self,
        name: &str,
        default: Option<&str>,
    ) -> Result<SmartString<LazyCompact>> {
        match self.manifest.options.get(name) {
            Some(ex) => match ex.clone().eval_to_const()? {
                DataValue::Str(s) => Ok(s),
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: ex.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a string is required".to_string(),
                }
                .into()),
            },
            None => match default {
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
                Some(s) => Ok(SmartString::from(s)),
            },
        }
    }

    /// Get the source span of the named option. Useful for generating informative error messages.
    pub fn option_span(&self, name: &str) -> Result<SourceSpan> {
        match self.manifest.options.get(name) {
            None => Err(FixedRuleOptionNotFoundError {
                name: name.to_string(),
                span: self.manifest.span,
                rule_name: self.manifest.fixed_handle.name.to_string(),
            }
            .into()),
            Some(v) => Ok(v.span()),
        }
    }
    /// Extract an integer option
    pub fn integer_option(&self, name: &str, default: Option<i64>) -> Result<i64> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => match n.get_int() {
                    Some(i) => Ok(i),
                    None => Err(FixedRuleOptionNotFoundError {
                        name: name.to_string(),
                        span: self.manifest.span,
                        rule_name: self.manifest.fixed_handle.name.to_string(),
                    }
                    .into()),
                },
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "an integer is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
    /// Extract a positive integer option
    pub fn pos_integer_option(&self, name: &str, default: Option<usize>) -> Result<usize> {
        let i = self.integer_option(name, default.map(|i| i as i64))?;
        ensure!(
            i > 0,
            WrongFixedRuleOptionError {
                name: name.to_string(),
                span: self.option_span(name)?,
                rule_name: self.manifest.fixed_handle.name.to_string(),
                help: "a positive integer is required".to_string(),
            }
        );
        Ok(i as usize)
    }
    /// Extract a non-negative integer option
    pub fn non_neg_integer_option(&self, name: &str, default: Option<usize>) -> Result<usize> {
        let i = self.integer_option(name, default.map(|i| i as i64))?;
        ensure!(
            i >= 0,
            WrongFixedRuleOptionError {
                name: name.to_string(),
                span: self.option_span(name)?,
                rule_name: self.manifest.fixed_handle.name.to_string(),
                help: "a non-negative integer is required".to_string(),
            }
        );
        Ok(i as usize)
    }
    /// Extract a floating point option
    pub fn float_option(&self, name: &str, default: Option<f64>) -> Result<f64> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Num(n)) => {
                    let f = n.get_float();
                    Ok(f)
                }
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a floating number is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
    /// Extract a floating point option between 0. and 1.
    pub fn unit_interval_option(&self, name: &str, default: Option<f64>) -> Result<f64> {
        let f = self.float_option(name, default)?;
        ensure!(
            (0. ..=1.).contains(&f),
            WrongFixedRuleOptionError {
                name: name.to_string(),
                span: self.option_span(name)?,
                rule_name: self.manifest.fixed_handle.name.to_string(),
                help: "a number between 0. and 1. is required".to_string(),
            }
        );
        Ok(f)
    }
    /// Extract a boolean option
    pub fn bool_option(&self, name: &str, default: Option<bool>) -> Result<bool> {
        match self.manifest.options.get(name) {
            Some(v) => match v.clone().eval_to_const() {
                Ok(DataValue::Bool(b)) => Ok(b),
                _ => Err(WrongFixedRuleOptionError {
                    name: name.to_string(),
                    span: v.span(),
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                    help: "a boolean value is required".to_string(),
                }
                .into()),
            },
            None => match default {
                Some(v) => Ok(v),
                None => Err(FixedRuleOptionNotFoundError {
                    name: name.to_string(),
                    span: self.manifest.span,
                    rule_name: self.manifest.fixed_handle.name.to_string(),
                }
                .into()),
            },
        }
    }
}

/// Trait for an implementation of an algorithm or a utility
pub trait FixedRule: Send + Sync {
    /// Called to initialize the options given.
    /// Will always be called once, before anything else.
    /// You can mutate the options if you need to.
    /// The default implementation does nothing.
    fn init_options(
        &self,
        _options: &mut BTreeMap<SmartString<LazyCompact>, Expr>,
        _span: SourceSpan,
    ) -> Result<()> {
        Ok(())
    }
    /// You must return the row width of the returned relation and it must be accurate.
    /// This function may be called multiple times.
    fn arity(
        &self,
        options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize>;
    /// You should implement the logic of your algorithm/utility in this function.
    /// The outputs are written to `out`. You should check `poison` periodically
    /// for user-initiated termination.
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &'_ mut RegularTempStore,
        poison: Poison,
    ) -> Result<()>;
}

/// Simple wrapper for custom fixed rule. You have less control than implementing [FixedRule] directly,
/// but implementation is simpler.
pub struct SimpleFixedRule {
    return_arity: usize,
    rule: Box<
        dyn Fn(Vec<NamedRows>, BTreeMap<String, DataValue>) -> Result<NamedRows>
            + Send
            + Sync
            + 'static,
    >,
}

impl SimpleFixedRule {
    /// Construct a SimpleFixedRule.
    ///
    /// * `return_arity`: The return arity of this rule.
    /// * `rule`:  The rule implementation as a closure.
    //    The first argument is a vector of input relations, realized into NamedRows,
    //    and the second argument is a JSON object of passed in options.
    //    The returned NamedRows is the return relation of the application of this rule.
    //    Every row of the returned relation must have length equal to `return_arity`.
    pub fn new<R>(return_arity: usize, rule: R) -> Self
    where
        R: Fn(Vec<NamedRows>, BTreeMap<String, DataValue>) -> Result<NamedRows>
            + Send
            + Sync
            + 'static,
    {
        Self {
            return_arity,
            rule: Box::new(rule),
        }
    }
    /// Construct a SimpleFixedRule that uses channels for communication.
    pub fn rule_with_channel(
        return_arity: usize,
    ) -> (
        Self,
        Receiver<(
            Vec<NamedRows>,
            BTreeMap<String, DataValue>,
            Sender<Result<NamedRows>>,
        )>,
    ) {
        let (db2app_sender, db2app_receiver) = bounded(0);
        (
            Self {
                return_arity,
                rule: Box::new(move |inputs, options| -> Result<NamedRows> {
                    let (app2db_sender, app2db_receiver) = bounded(0);
                    db2app_sender
                        .send((inputs, options, app2db_sender))
                        .into_diagnostic()?;
                    app2db_receiver.recv().into_diagnostic()?
                }),
            },
            db2app_receiver,
        )
    }
}

impl FixedRule for SimpleFixedRule {
    fn arity(
        &self,
        _options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        _span: SourceSpan,
    ) -> Result<usize> {
        Ok(self.return_arity)
    }

    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &'_ mut RegularTempStore,
        _poison: Poison,
    ) -> Result<()> {
        let options: BTreeMap<_, _> = payload
            .manifest
            .options
            .iter()
            .map(|(k, v)| -> Result<_> {
                let val = v.clone().eval_to_const()?;
                Ok((k.to_string(), val))
            })
            .try_collect()?;
        let input_arity = payload.manifest.rule_args.len();
        let inputs: Vec<_> = (0..input_arity)
            .map(|i| -> Result<_> {
                let input = payload.get_input(i).unwrap();
                let rows: Vec<_> = input.iter()?.try_collect()?;
                let mut headers = input
                    .arg_manifest
                    .bindings()
                    .iter()
                    .map(|s| s.name.to_string())
                    .collect_vec();
                let l = headers.len();
                let m = input.arg_manifest.arity(payload.tx, payload.stores)?;
                for i in l..m {
                    headers.push(format!("_{i}"));
                }
                Ok(NamedRows::new(headers, rows))
            })
            .try_collect()?;
        let results: NamedRows = (self.rule)(inputs, options)?;
        for row in results.rows {
            #[derive(Debug, Error, Diagnostic)]
            #[error("arity mismatch: expect {1}, got {2}")]
            #[diagnostic(code(parser::simple_fixed_rule_arity_mismatch))]
            struct ArityMismatch(#[label] SourceSpan, usize, usize);

            ensure!(
                row.len() == self.return_arity,
                ArityMismatch(payload.span(), self.return_arity, row.len())
            );
            out.put(row);
        }
        Ok(())
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Cannot determine arity for algo {0} since {1}")]
#[diagnostic(code(parser::no_algo_arity))]
pub(crate) struct CannotDetermineArity(
    pub(crate) String,
    pub(crate) String,
    #[label] pub(crate) SourceSpan,
);

#[derive(Clone, Debug)]
pub(crate) struct FixedRuleHandle {
    pub(crate) name: Symbol,
}

lazy_static! {
    pub(crate) static ref DEFAULT_FIXED_RULES: BTreeMap<String, Arc<Box<dyn FixedRule>>> = {
        BTreeMap::from([
            #[cfg(feature = "graph-algo")]
            (
                "ClusteringCoefficients".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ClusteringCoefficients)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DegreeCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(DegreeCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ClosenessCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ClosenessCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BetweennessCentrality".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(BetweennessCentrality)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DepthFirstSearch".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Dfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "DFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Dfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BreadthFirstSearch".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Bfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "BFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Bfs)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathBFS".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathBFS)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathDijkstra".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathDijkstra)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ShortestPathAStar".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ShortestPathAStar)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "KShortestPathYen".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(KShortestPathYen)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "MinimumSpanningTreePrim".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(MinimumSpanningTreePrim)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "MinimumSpanningForestKruskal".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(MinimumSpanningForestKruskal)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "TopSort".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(TopSort)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "ConnectedComponents".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(false))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "StronglyConnectedComponents".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(true))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "SCC".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(StronglyConnectedComponent::new(true))),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "PageRank".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(PageRank)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "CommunityDetectionLouvain".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(CommunityDetectionLouvain)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "LabelPropagation".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(LabelPropagation)),
            ),
            #[cfg(feature = "graph-algo")]
            (
                "RandomWalk".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(RandomWalk)),
            ),
            (
                "ReorderSort".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(ReorderSort)),
            ),
            (
                "JsonReader".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(JsonReader)),
            ),
            (
                "CsvReader".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(CsvReader)),
            ),
            (
                "Constant".to_string(),
                Arc::<Box<dyn FixedRule>>::new(Box::new(Constant)),
            ),
        ])
    };
}

impl FixedRuleHandle {
    pub(crate) fn new(name: &str, span: SourceSpan) -> Self {
        FixedRuleHandle {
            name: Symbol::new(name, span),
        }
    }
}

#[derive(Error, Diagnostic, Debug)]
#[error("The relation cannot be interpreted as an edge")]
#[diagnostic(code(algo::not_an_edge))]
#[diagnostic(help("Edge relation requires tuples of length at least two"))]
struct NotAnEdgeError(#[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error(
    "The value {0:?} at the third position in the relation cannot be interpreted as edge weights"
)]
#[diagnostic(code(algo::invalid_edge_weight))]
#[diagnostic(help(
    "Edge weights must be finite numbers. Some algorithm also requires positivity."
))]
struct BadEdgeWeightError(DataValue, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("The requested rule '{0}' cannot be found")]
#[diagnostic(code(algo::rule_not_found))]
struct RuleNotFoundError(String, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Invalid reverse scanning of triples")]
#[diagnostic(code(algo::invalid_reverse_triple_scan))]
#[diagnostic(help(
    "Inverse scanning of triples requires the type to be 'ref', or the value be indexed"
))]
struct InvalidInverseTripleUse(String, #[label] SourceSpan);

#[derive(Error, Diagnostic, Debug)]
#[error("Required node with key {missing:?} not found")]
#[diagnostic(code(algo::node_with_key_not_found))]
#[diagnostic(help(
    "The relation is interpreted as a relation of nodes, but the required key is missing"
))]
pub(crate) struct NodeNotFoundError {
    pub(crate) missing: DataValue,
    #[label]
    pub(crate) span: SourceSpan,
}

#[derive(Error, Diagnostic, Debug)]
#[error("Unacceptable value {0:?} encountered")]
#[diagnostic(code(algo::unacceptable_value))]
pub(crate) struct BadExprValueError(
    pub(crate) DataValue,
    #[label] pub(crate) SourceSpan,
    #[help] pub(crate) String,
);

#[derive(Error, Diagnostic, Debug)]
#[error("The requested fixed rule '{0}' is not found")]
#[diagnostic(code(parser::fixed_rule_not_found))]
pub(crate) struct FixedRuleNotFoundError(pub(crate) String, #[label] pub(crate) SourceSpan);

impl MagicFixedRuleRuleArg {
    pub(crate) fn arity(
        &self,
        tx: &SessionTx<'_>,
        stores: &BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<usize> {
        Ok(match self {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                store.arity
            }
            MagicFixedRuleRuleArg::Stored { name, .. } => {
                let handle = tx.get_relation(name, false)?;
                handle.arity()
            }
        })
    }
}
