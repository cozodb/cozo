/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use miette::{bail, ensure, Context, Diagnostic, Result};
use thiserror::Error;

use crate::data::aggr::Aggregation;
use crate::data::expr::Expr;
use crate::data::program::{
    MagicAtom, MagicFixedRuleApply, MagicInlineRule, MagicRulesOrFixed, MagicSymbol,
    StratifiedMagicProgram,
};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::query::ra::RelAlgebra;
use crate::runtime::relation::{AccessLevel, InsufficientAccessLevel};
use crate::runtime::transact::SessionTx;

pub(crate) type CompiledProgram = BTreeMap<MagicSymbol, CompiledRuleSet>;

#[derive(Debug)]
pub(crate) enum CompiledRuleSet {
    Rules(Vec<CompiledRule>),
    Fixed(MagicFixedRuleApply),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum AggrKind {
    None,
    Normal,
    Meet,
}

impl CompiledRuleSet {
    pub(crate) fn arity(&self) -> usize {
        match self {
            CompiledRuleSet::Rules(rs) => rs[0].aggr.len(),
            CompiledRuleSet::Fixed(fixed) => fixed.arity,
        }
    }
    pub(crate) fn aggr_kind(&self) -> AggrKind {
        match self {
            CompiledRuleSet::Rules(rules) => {
                let mut has_non_meet = false;
                let mut has_aggr = false;
                for maybe_aggr in rules[0].aggr.iter() {
                    match maybe_aggr {
                        None => {
                            // meet aggregations must all be at the last positions
                            if has_aggr {
                                has_non_meet = true
                            }
                        }
                        Some((aggr, _)) => {
                            has_aggr = true;
                            has_non_meet = has_non_meet || !aggr.is_meet
                        }
                    }
                }
                match (has_aggr, has_non_meet) {
                    (false, _) => AggrKind::None,
                    (true, true) => AggrKind::Normal,
                    (true, false) => AggrKind::Meet,
                }
            }
            CompiledRuleSet::Fixed(_) => AggrKind::None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ContainedRuleMultiplicity {
    One,
    Many,
}

#[derive(Debug)]
pub(crate) struct CompiledRule {
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) relation: RelAlgebra,
    pub(crate) contained_rules: BTreeMap<MagicSymbol, ContainedRuleMultiplicity>,
}

#[derive(Debug, Error, Diagnostic)]
#[error("Requested rule {0} not found")]
#[diagnostic(code(eval::rule_not_found))]
struct RuleNotFound(String, #[label] SourceSpan);

#[derive(Debug, Error, Diagnostic)]
#[error("Arity mismatch for rule application {0}")]
#[diagnostic(code(eval::rule_arity_mismatch))]
#[diagnostic(help("Required arity: {1}, number of arguments given: {2}"))]
struct ArityMismatch(String, usize, usize, #[label] SourceSpan);

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum IndexPositionUse {
    Join,
    BindForLater,
    Ignored,
}

impl<'a> SessionTx<'a> {
    pub(crate) fn stratified_magic_compile(
        &mut self,
        prog: StratifiedMagicProgram,
    ) -> Result<Vec<CompiledProgram>> {
        let mut store_arities: BTreeMap<MagicSymbol, usize> = Default::default();

        for stratum in prog.0.iter() {
            for (name, ruleset) in &stratum.prog {
                store_arities.insert(name.clone(), ruleset.arity()?);
            }
        }

        let compiled: Vec<_> = prog
            .0
            .into_iter()
            .rev()
            .map(|cur_prog| -> Result<CompiledProgram> {
                cur_prog
                    .prog
                    .into_iter()
                    .map(|(k, body)| -> Result<(MagicSymbol, CompiledRuleSet)> {
                        match body {
                            MagicRulesOrFixed::Rules { rules: body } => {
                                let mut collected = Vec::with_capacity(body.len());
                                for rule in body.iter() {
                                    let header = &rule.head;
                                    let mut relation =
                                        self.compile_magic_rule_body(rule, &k, &store_arities, header)?;
                                    relation.fill_binding_indices_and_compile().with_context(|| {
                                        format!(
                                            "error encountered when filling binding indices for {relation:#?}"
                                        )
                                    })?;
                                    collected.push(CompiledRule {
                                        aggr: rule.aggr.clone(),
                                        relation,
                                        contained_rules: rule.contained_rules(),
                                    })
                                }
                                Ok((k, CompiledRuleSet::Rules(collected)))
                            }

                            MagicRulesOrFixed::Fixed { fixed } => {
                                Ok((k, CompiledRuleSet::Fixed(fixed)))
                            }
                        }
                    })
                    .try_collect()
            })
            .try_collect()?;
        Ok(compiled)
    }
    pub(crate) fn compile_magic_rule_body(
        &mut self,
        rule: &MagicInlineRule,
        rule_name: &MagicSymbol,
        store_arities: &BTreeMap<MagicSymbol, usize>,
        ret_vars: &[Symbol],
    ) -> Result<RelAlgebra> {
        let mut ret = RelAlgebra::unit(rule_name.symbol().span);
        let mut seen_variables = BTreeSet::new();
        let mut serial_id = 0;
        let mut gen_symb = |span| {
            let ret = Symbol::new(&format!("**{serial_id}") as &str, span);
            serial_id += 1;
            ret
        };
        for atom in &rule.body {
            match atom {
                MagicAtom::Rule(rule_app) => {
                    let store_arity = store_arities.get(&rule_app.name).ok_or_else(|| {
                        RuleNotFound(
                            rule_app.name.symbol().to_string(),
                            rule_app.name.symbol().span,
                        )
                    })?;

                    ensure!(
                        *store_arity == rule_app.args.len(),
                        ArityMismatch(
                            rule_app.name.symbol().to_string(),
                            *store_arity,
                            rule_app.args.len(),
                            rule_app.span
                        )
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                        }
                    }

                    let right =
                        RelAlgebra::derived(right_vars, rule_app.name.clone(), rule_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.join(right, prev_joiner_vars, right_joiner_vars, rule_app.span);
                }
                MagicAtom::Relation(rel_app) => {
                    let store = self.get_relation(&rel_app.name, false)?;
                    if store.access_level < AccessLevel::ReadOnly {
                        bail!(InsufficientAccessLevel(
                            store.name.to_string(),
                            "reading rows".to_string(),
                            store.access_level
                        ));
                    }
                    ensure!(
                        store.arity() == rel_app.args.len(),
                        ArityMismatch(
                            rel_app.name.to_string(),
                            store.arity(),
                            rel_app.args.len(),
                            rel_app.span
                        )
                    );
                    // already existing vars
                    let mut prev_joiner_vars = vec![];
                    // vars introduced by right and joined
                    let mut right_joiner_vars = vec![];
                    // used to split in case we need to join again
                    let mut right_joiner_vars_pos = vec![];
                    // vars introduced by right, regardless of joining
                    let mut right_vars = vec![];
                    // used for choosing indices
                    let mut join_indices = vec![];

                    for (i, var) in rel_app.args.iter().enumerate() {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                            right_joiner_vars_pos.push(i);
                            join_indices.push(IndexPositionUse::Join)
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                            if var.is_generated_ignored_symbol() {
                                join_indices.push(IndexPositionUse::Ignored)
                            } else {
                                join_indices.push(IndexPositionUse::BindForLater)
                            }
                        }
                    }

                    let chosen_index =
                        store.choose_index(&join_indices, rel_app.valid_at.is_some());

                    match chosen_index {
                        None => {
                            // scan original relation
                            let right = RelAlgebra::relation(
                                right_vars,
                                store,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                            ret =
                                ret.join(right, prev_joiner_vars, right_joiner_vars, rel_app.span);
                        }
                        Some((chosen_index, mapper, false)) => {
                            // index-only
                            let new_right_vars = mapper
                                .into_iter()
                                .map(|i| right_vars[i].clone())
                                .collect_vec();
                            let right = RelAlgebra::relation(
                                new_right_vars,
                                chosen_index,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                            ret =
                                ret.join(right, prev_joiner_vars, right_joiner_vars, rel_app.span);
                        }
                        Some((chosen_index, mapper, true)) => {
                            // index-with-join
                            let mut prev_joiner_first_vars = vec![];
                            let mut middle_joiner_left_vars = vec![];
                            let mut middle_vars = vec![];
                            for i in mapper.iter() {
                                let tv = gen_symb(right_vars[*i].span);
                                if let Some(j) = right_joiner_vars_pos.iter().position(|el| el == i)
                                {
                                    prev_joiner_first_vars.push(prev_joiner_vars[j].clone());
                                    middle_joiner_left_vars.push(tv.clone());
                                }
                                middle_vars.push(tv);
                            }
                            let middle_joiner_right_vars = mapper
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, orig_idx)| {
                                    if *orig_idx < store.metadata.keys.len() {
                                        Some(middle_vars[idx].clone())
                                    } else {
                                        None
                                    }
                                })
                                .collect_vec();

                            let mut final_joiner_vars = vec![];
                            for idx in mapper.iter() {
                                final_joiner_vars.push(right_vars[*idx].clone());
                            }

                            let middle = RelAlgebra::relation(
                                middle_vars,
                                chosen_index,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            ret = ret.join(
                                middle,
                                prev_joiner_first_vars,
                                middle_joiner_left_vars,
                                rel_app.span,
                            );
                            let final_alg = RelAlgebra::relation(
                                right_vars,
                                store,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            ret = ret.join(
                                final_alg,
                                middle_joiner_right_vars,
                                final_joiner_vars,
                                rel_app.span,
                            );
                        }
                    }
                }
                MagicAtom::NegatedRule(rule_app) => {
                    let store_arity = store_arities.get(&rule_app.name).ok_or_else(|| {
                        RuleNotFound(
                            rule_app.name.symbol().to_string(),
                            rule_app.name.symbol().span,
                        )
                    })?;
                    ensure!(
                        *store_arity == rule_app.args.len(),
                        ArityMismatch(
                            rule_app.name.symbol().to_string(),
                            *store_arity,
                            rule_app.args.len(),
                            rule_app.span
                        )
                    );

                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            right_vars.push(var.clone());
                        }
                    }

                    let right =
                        RelAlgebra::derived(right_vars, rule_app.name.clone(), rule_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.neg_join(right, prev_joiner_vars, right_joiner_vars, rule_app.span);
                }
                MagicAtom::NegatedRelation(rel_app) => {
                    let store = self.get_relation(&rel_app.name, false)?;
                    ensure!(
                        store.arity() == rel_app.args.len(),
                        ArityMismatch(
                            rel_app.name.to_string(),
                            store.arity(),
                            rel_app.args.len(),
                            rel_app.span
                        )
                    );

                    // already existing vars
                    let mut prev_joiner_vars = vec![];
                    // vars introduced by right and joined
                    let mut right_joiner_vars = vec![];
                    // used to split in case we need to join again
                    let mut right_joiner_vars_pos = vec![];
                    // vars introduced by right, regardless of joining
                    let mut right_vars = vec![];
                    // used for choosing indices
                    let mut join_indices = vec![];

                    for (i, var) in rel_app.args.iter().enumerate() {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                            right_joiner_vars_pos.push(i);
                            join_indices.push(IndexPositionUse::Join)
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                            if var.is_generated_ignored_symbol() {
                                join_indices.push(IndexPositionUse::Ignored)
                            } else {
                                join_indices.push(IndexPositionUse::BindForLater)
                            }
                        }
                    }

                    let chosen_index =
                        store.choose_index(&join_indices, rel_app.valid_at.is_some());

                    match chosen_index {
                        None | Some((_, _, true)) => {
                            let right = RelAlgebra::relation(
                                right_vars,
                                store,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                            ret = ret.neg_join(
                                right,
                                prev_joiner_vars,
                                right_joiner_vars,
                                rel_app.span,
                            );
                        }
                        Some((chosen_index, mapper, false)) => {
                            // index-only
                            let new_right_vars = mapper
                                .into_iter()
                                .map(|i| right_vars[i].clone())
                                .collect_vec();
                            let right = RelAlgebra::relation(
                                new_right_vars,
                                chosen_index,
                                rel_app.span,
                                rel_app.valid_at,
                            )?;
                            debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                            ret = ret.neg_join(
                                right,
                                prev_joiner_vars,
                                right_joiner_vars,
                                rel_app.span,
                            );
                        }
                    }
                }
                MagicAtom::Predicate(p) => {
                    ret = ret.filter(p.clone())?;
                }
                MagicAtom::HnswSearch(s) => {
                    debug_assert!(
                        seen_variables.contains(&s.query),
                        "HNSW search query must be bound"
                    );
                    let mut own_bindings = vec![];
                    let mut post_filters = vec![];
                    for var in s.all_bindings() {
                        if seen_variables.contains(var) {
                            let rk = gen_symb(var.span);
                            post_filters.push(Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: var.clone(),
                                        tuple_pos: None,
                                    },
                                    Expr::Binding {
                                        var: rk.clone(),
                                        tuple_pos: None,
                                    },
                                ],
                                var.span,
                            ));
                            own_bindings.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            own_bindings.push(var.clone());
                        }
                    }
                    ret = ret.hnsw_search(s.clone(), own_bindings)?;
                    if !post_filters.is_empty() {
                        ret = ret.filter(Expr::build_and(post_filters, s.span))?;
                    }
                }
                MagicAtom::FtsSearch(s) => {
                    debug_assert!(
                        seen_variables.contains(&s.query),
                        "FTS search query must be bound"
                    );
                    let mut own_bindings = vec![];
                    let mut post_filters = vec![];
                    for var in s.all_bindings() {
                        if seen_variables.contains(var) {
                            let rk = gen_symb(var.span);
                            post_filters.push(Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: var.clone(),
                                        tuple_pos: None,
                                    },
                                    Expr::Binding {
                                        var: rk.clone(),
                                        tuple_pos: None,
                                    },
                                ],
                                var.span,
                            ));
                            own_bindings.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            own_bindings.push(var.clone());
                        }
                    }
                    ret = ret.fts_search(s.clone(), own_bindings)?;
                    if !post_filters.is_empty() {
                        ret = ret.filter(Expr::build_and(post_filters, s.span))?;
                    }
                }
                MagicAtom::LshSearch(s) => {
                    debug_assert!(
                        seen_variables.contains(&s.query),
                        "FTS search query must be bound"
                    );
                    let mut own_bindings = vec![];
                    let mut post_filters = vec![];
                    for var in s.all_bindings() {
                        if seen_variables.contains(var) {
                            let rk = gen_symb(var.span);
                            post_filters.push(Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: var.clone(),
                                        tuple_pos: None,
                                    },
                                    Expr::Binding {
                                        var: rk.clone(),
                                        tuple_pos: None,
                                    },
                                ],
                                var.span,
                            ));
                            own_bindings.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            own_bindings.push(var.clone());
                        }
                    }
                    ret = ret.lsh_search(s.clone(), own_bindings)?;
                    if !post_filters.is_empty() {
                        ret = ret.filter(Expr::build_and(post_filters, s.span))?;
                    }
                }
                MagicAtom::Unification(u) => {
                    if seen_variables.contains(&u.binding) {
                        let expr = if u.one_many_unif {
                            Expr::build_is_in(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        } else {
                            Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        };
                        ret = ret.filter(expr)?;
                    } else {
                        seen_variables.insert(u.binding.clone());
                        ret = ret.unify(u.binding.clone(), u.expr.clone(), u.one_many_unif, u.span);
                    }
                }
            }
        }

        let ret_vars_set = ret_vars.iter().cloned().collect();
        ret.eliminate_temp_vars(&ret_vars_set)?;
        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            let ret_span = ret.span();
            ret = ret.cartesian_join(RelAlgebra::unit(ret_span), ret_span);
            ret.eliminate_temp_vars(&ret_vars_set)?;
        }

        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        #[derive(Debug, Error, Diagnostic)]
        #[error("Symbol '{0}' in rule head is unbound")]
        #[diagnostic(code(eval::unbound_symb_in_head))]
        #[diagnostic(help(
            "Note that symbols occurring only in negated positions are not considered bound"
        ))]
        struct UnboundSymbolInRuleHead(String, #[label] SourceSpan);

        ensure!(cur_ret_set == ret_vars_set, {
            let unbound = ret_vars_set.difference(&cur_ret_set).next().unwrap();
            UnboundSymbolInRuleHead(unbound.to_string(), unbound.span)
        });
        let cur_ret_bindings = ret.bindings_after_eliminate();
        if ret_vars != cur_ret_bindings {
            ret = ret.reorder(ret_vars.to_vec());
        }

        Ok(ret)
    }
}
