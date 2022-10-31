/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use log::{debug, trace};
use miette::Result;

use crate::data::program::{MagicAlgoApply, MagicSymbol, NoEntryError};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::parse::SourceSpan;
use crate::query::compile::{AggrKind, CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct QueryLimiter {
    total: Option<usize>,
    skip: Option<usize>,
    counter: usize,
}

impl QueryLimiter {
    pub(crate) fn incr_and_should_stop(&mut self) -> bool {
        if let Some(limit) = self.total {
            self.counter += 1;
            self.counter >= limit
        } else {
            false
        }
    }
    pub(crate) fn should_skip_next(&self) -> bool {
        match self.skip {
            None => false,
            Some(i) => i > self.counter,
        }
    }
}

impl SessionTx {
    pub(crate) fn stratified_magic_evaluate(
        &self,
        strata: &[CompiledProgram],
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        total_num_to_take: Option<usize>,
        num_to_skip: Option<usize>,
        poison: Poison,
    ) -> Result<(InMemRelation, bool)> {
        let ret_area = stores
            .get(&MagicSymbol::Muggle {
                inner: Symbol::new(PROG_ENTRY, SourceSpan(0, 0)),
            })
            .ok_or(NoEntryError)?
            .clone();
        let mut early_return = false;
        for (idx, cur_prog) in strata.iter().enumerate() {
            debug!("stratum {}", idx);
            early_return = self.semi_naive_magic_evaluate(
                cur_prog,
                stores,
                total_num_to_take,
                num_to_skip,
                poison.clone(),
            )?;
        }
        Ok((ret_area, early_return))
    }
    fn semi_naive_magic_evaluate(
        &self,
        prog: &CompiledProgram,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        total_num_to_take: Option<usize>,
        num_to_skip: Option<usize>,
        poison: Poison,
    ) -> Result<bool> {
        let mut changed: BTreeMap<_, _> = prog.keys().map(|k| (k, false)).collect();
        let mut prev_changed = changed.clone();
        let mut limiter = QueryLimiter {
            total: total_num_to_take,
            skip: num_to_skip,
            counter: 0,
        };

        let mut used_limiter = false;

        for epoch in 0u32.. {
            debug!("epoch {}", epoch);
            if epoch == 0 {
                for (k, compiled_ruleset) in prog.iter() {
                    match compiled_ruleset {
                        CompiledRuleSet::Rules(ruleset) => {
                            let aggr_kind = compiled_ruleset.aggr_kind();
                            used_limiter = self.initial_rule_eval(
                                k,
                                ruleset,
                                aggr_kind,
                                stores,
                                &mut changed,
                                &mut limiter,
                                poison.clone(),
                            )? || used_limiter;
                        }
                        CompiledRuleSet::Algo(algo_apply) => {
                            self.algo_application_eval(k, algo_apply, stores, poison.clone())?;
                        }
                    }
                }
            } else {
                mem::swap(&mut changed, &mut prev_changed);
                for (_k, v) in changed.iter_mut() {
                    *v = false;
                }

                for (k, compiled_ruleset) in prog.iter() {
                    match compiled_ruleset {
                        CompiledRuleSet::Rules(ruleset) => {
                            let is_meet_aggr = match compiled_ruleset.aggr_kind() {
                                AggrKind::None => false,
                                AggrKind::Normal => false,
                                AggrKind::Meet => true,
                            };
                            used_limiter = self.incremental_rule_eval(
                                k,
                                ruleset,
                                epoch,
                                is_meet_aggr,
                                stores,
                                &prev_changed,
                                &mut changed,
                                &mut limiter,
                                poison.clone(),
                            )? || used_limiter;
                        }

                        CompiledRuleSet::Algo(_) => unreachable!(),
                    }
                }
            }
            if changed.values().all(|rule_changed| !*rule_changed) {
                break;
            }
        }
        Ok(used_limiter)
    }
    fn algo_application_eval(
        &self,
        rule_symb: &MagicSymbol,
        algo_apply: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        poison: Poison,
    ) -> Result<()> {
        let mut algo_impl = algo_apply.algo.get_impl()?;
        let out = stores.get(rule_symb).unwrap();
        algo_impl.run(self, algo_apply, stores, out, poison)
    }
    fn initial_rule_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        aggr_kind: AggrKind,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        changed: &mut BTreeMap<&MagicSymbol, bool>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<bool> {
        let store = stores.get(rule_symb).unwrap();
        let use_delta = BTreeSet::default();
        let should_check_limit =
            limiter.total.is_some() && rule_symb.is_prog_entry() && aggr_kind != AggrKind::Meet;
        match aggr_kind {
            AggrKind::None | AggrKind::Meet => {
                let is_meet = aggr_kind == AggrKind::Meet;
                for (rule_n, rule) in ruleset.iter().enumerate() {
                    debug!("initial calculation for rule {:?}.{}", rule_symb, rule_n);
                    let mut aggr = rule.aggr.clone();
                    for (aggr, args) in aggr.iter_mut().flatten() {
                        aggr.meet_init(args)?;
                    }
                    for item_res in rule.relation.iter(self, Some(0), &use_delta)? {
                        let item = item_res?;
                        trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);
                        if is_meet {
                            store.aggr_meet_put(&item, &mut aggr, 0)?;
                        } else if should_check_limit {
                            if !store.exists(&item, 0) {
                                store.put_with_skip(item, limiter.should_skip_next());
                                if limiter.incr_and_should_stop() {
                                    trace!("early stopping due to result count limit exceeded");
                                    return Ok(true);
                                }
                            }
                        } else {
                            store.put(item, 0);
                        }
                        *changed.get_mut(rule_symb).unwrap() = true;
                        poison.check()?;
                    }
                }
            }
            AggrKind::Normal => {
                let store_to_use = self.new_temp_store(rule_symb.symbol().span);
                for (rule_n, rule) in ruleset.iter().enumerate() {
                    debug!(
                        "Calculation for normal aggr rule {:?}.{}",
                        rule_symb, rule_n
                    );
                    for (serial, item_res) in
                        rule.relation.iter(self, Some(0), &use_delta)?.enumerate()
                    {
                        let item = item_res?;
                        trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);
                        store_to_use.normal_aggr_put(&item, &rule.aggr, serial);
                        *changed.get_mut(rule_symb).unwrap() = true;
                        poison.check()?;
                    }
                }
                if store_to_use.normal_aggr_scan_and_put(
                    &ruleset[0].aggr,
                    store,
                    if should_check_limit {
                        Some(limiter)
                    } else {
                        None
                    },
                    poison,
                )? {
                    return Ok(true);
                }
            }
        }
        Ok(should_check_limit)
    }
    fn incremental_rule_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        epoch: u32,
        is_meet_aggr: bool,
        stores: &BTreeMap<MagicSymbol, InMemRelation>,
        prev_changed: &BTreeMap<&MagicSymbol, bool>,
        changed: &mut BTreeMap<&MagicSymbol, bool>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<bool> {
        let store = stores.get(rule_symb).unwrap();
        let should_check_limit =
            limiter.total.is_some() && rule_symb.is_prog_entry() && !is_meet_aggr;
        for (rule_n, rule) in ruleset.iter().enumerate() {
            let mut should_do_calculation = false;
            for d_rule in &rule.contained_rules {
                if let Some(changed) = prev_changed.get(d_rule) {
                    if *changed {
                        should_do_calculation = true;
                        break;
                    }
                }
            }
            if !should_do_calculation {
                continue;
            }

            let mut aggr = rule.aggr.clone();
            for (aggr, args) in aggr.iter_mut().flatten() {
                aggr.meet_init(args)?;
            }

            for (delta_key, delta_store) in stores.iter() {
                if !rule.contained_rules.contains(delta_key) {
                    continue;
                }
                debug!(
                    "with delta {:?} for rule {:?}.{}",
                    delta_key, rule_symb, rule_n
                );
                let use_delta = BTreeSet::from([delta_store.id]);
                for item_res in rule.relation.iter(self, Some(epoch), &use_delta)? {
                    let item = item_res?;
                    // improvement: the clauses can actually be evaluated in parallel
                    if is_meet_aggr {
                        let aggr_changed = store.aggr_meet_put(&item, &mut aggr, epoch)?;
                        if aggr_changed {
                            *changed.get_mut(rule_symb).unwrap() = true;
                        }
                    } else if store.exists(&item, 0) {
                        trace!(
                            "item for {:?}.{}: {:?} at {}, rederived",
                            rule_symb,
                            rule_n,
                            item,
                            epoch
                        );
                    } else {
                        trace!(
                            "item for {:?}.{}: {:?} at {}",
                            rule_symb,
                            rule_n,
                            item,
                            epoch
                        );
                        *changed.get_mut(rule_symb).unwrap() = true;
                        store.put(item.clone(), epoch);
                        store.put_with_skip(item, limiter.should_skip_next());
                        if should_check_limit && limiter.incr_and_should_stop() {
                            trace!("early stopping due to result count limit exceeded");
                            return Ok(true);
                        }
                    }
                    poison.check()?;
                }
            }
        }
        Ok(should_check_limit)
    }
}
