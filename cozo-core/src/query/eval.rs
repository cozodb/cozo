/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

use itertools::Itertools;
use log::{debug, trace};
use miette::Result;

use crate::data::aggr::Aggregation;
use crate::data::program::{MagicSymbol, NoEntryError};
use crate::data::symb::{Symbol, PROG_ENTRY};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::query::compile::{AggrKind, CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::runtime::db::Poison;
use crate::runtime::temp_store::{EpochStore, MeetAggrStore, NormalTempStore};
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

impl<'a> SessionTx<'a> {
    pub(crate) fn stratified_magic_evaluate(
        &self,
        strata: &[CompiledProgram],
        store_lifetimes: BTreeMap<MagicSymbol, usize>,
        total_num_to_take: Option<usize>,
        num_to_skip: Option<usize>,
        poison: Poison,
    ) -> Result<(EpochStore, bool)> {
        let mut stores: BTreeMap<MagicSymbol, EpochStore> = BTreeMap::new();
        let mut early_return = false;
        for (stratum, cur_prog) in strata.iter().enumerate() {
            if stratum > 0 {
                // remove stores that have outlived their usefulness!
                stores.retain(|name, _| match store_lifetimes.get(name) {
                    None => false,
                    Some(n) => *n >= stratum,
                });
                trace!("{:?}", stores);
            }
            for (rule_name, rule_set) in cur_prog {
                let store = match rule_set.aggr_kind() {
                    AggrKind::None | AggrKind::Normal => EpochStore::new_normal(rule_set.arity()),
                    AggrKind::Meet => {
                        let rs = match rule_set {
                            CompiledRuleSet::Rules(rs) => rs,
                            _ => unreachable!(),
                        };
                        EpochStore::new_meet(&rs[0].aggr)?
                    }
                };
                stores.insert(rule_name.clone(), store);
            }
            debug!("stratum {}", stratum);
            early_return = self.semi_naive_magic_evaluate(
                cur_prog,
                &mut stores,
                total_num_to_take,
                num_to_skip,
                poison.clone(),
            )?;
        }
        let entry_symbol = MagicSymbol::Muggle {
            inner: Symbol::new(PROG_ENTRY, SourceSpan(0, 0)),
        };
        let ret_area = stores.remove(&entry_symbol).ok_or(NoEntryError)?;
        Ok((ret_area, early_return))
    }
    /// returns true if early return is activated
    fn semi_naive_magic_evaluate(
        &self,
        prog: &CompiledProgram,
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        total_num_to_take: Option<usize>,
        num_to_skip: Option<usize>,
        poison: Poison,
    ) -> Result<bool> {
        let mut limiter = QueryLimiter {
            total: total_num_to_take,
            skip: num_to_skip,
            counter: 0,
        };

        let mut used_limiter = false;

        for epoch in 0u32.. {
            debug!("epoch {}", epoch);
            let mut to_merge = BTreeMap::new();
            if epoch == 0 {
                for (k, compiled_ruleset) in prog.iter().rev() {
                    let new_store = match compiled_ruleset {
                        CompiledRuleSet::Rules(ruleset) => match compiled_ruleset.aggr_kind() {
                            AggrKind::None => {
                                let res = self.initial_rule_non_aggr_eval(
                                    k,
                                    ruleset,
                                    stores,
                                    &mut limiter,
                                    poison.clone(),
                                )?;
                                used_limiter = res.0 || used_limiter;
                                res.1.wrap()
                            }
                            AggrKind::Normal => {
                                let res = self.initial_rule_aggr_eval(
                                    k,
                                    ruleset,
                                    stores,
                                    &mut limiter,
                                    poison.clone(),
                                )?;
                                used_limiter = res.0 || used_limiter;
                                res.1.wrap()
                            }
                            AggrKind::Meet => {
                                let new = self.initial_rule_meet_eval(
                                    k,
                                    ruleset,
                                    stores,
                                    poison.clone(),
                                )?;
                                new.wrap()
                            }
                        },
                        CompiledRuleSet::Algo(algo_apply) => {
                            let mut algo_impl = algo_apply.algo.get_impl()?;
                            let mut out = NormalTempStore::default();
                            algo_impl.run(self, algo_apply, stores, &mut out, poison.clone())?;
                            out.wrap()
                        }
                    };
                    to_merge.insert(k, new_store);
                }
            } else {
                for (k, compiled_ruleset) in prog.iter().rev() {
                    let new_store = match compiled_ruleset {
                        CompiledRuleSet::Rules(ruleset) => {
                            match compiled_ruleset.aggr_kind() {
                                AggrKind::None => {
                                    let res = self.incremental_rule_non_aggr_eval(
                                        k,
                                        ruleset,
                                        epoch,
                                        stores,
                                        &mut limiter,
                                        poison.clone(),
                                    )?;
                                    used_limiter = res.0 || used_limiter;
                                    res.1.wrap()
                                }
                                AggrKind::Meet => {
                                    let new = self.incremental_rule_meet_eval(
                                        k,
                                        ruleset,
                                        stores,
                                        poison.clone(),
                                    )?;
                                    new.wrap()
                                }
                                AggrKind::Normal => {
                                    // not doing anything
                                    NormalTempStore::default().wrap()
                                }
                            }
                        }

                        CompiledRuleSet::Algo(_) => {
                            // no need to do anything, algos are only calculated once
                            NormalTempStore::default().wrap()
                        }
                    };
                    to_merge.insert(k, new_store);
                }
            }
            let mut changed = false;
            for (k, new_store) in to_merge {
                let old_store = stores.get_mut(k).unwrap();
                old_store.merge_in(new_store)?;
                trace!("delta for {}: {}", k, old_store.has_delta());
                changed |= old_store.has_delta();
            }
            if !changed {
                break;
            }
        }
        Ok(used_limiter)
    }
    /// returns true is early return is activated
    fn initial_rule_non_aggr_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<(bool, NormalTempStore)> {
        let mut out_store = NormalTempStore::default();
        let should_check_limit = limiter.total.is_some() && rule_symb.is_prog_entry();

        for (rule_n, rule) in ruleset.iter().enumerate() {
            debug!("initial calculation for rule {:?}.{}", rule_symb, rule_n);
            for item_res in rule.relation.iter(self, None, stores)? {
                let item = item_res?;
                trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);
                if should_check_limit {
                    if !out_store.exists(&item) {
                        if limiter.should_skip_next() {
                            out_store.put_with_skip(item);
                        } else {
                            out_store.put(item);
                        }
                        if limiter.incr_and_should_stop() {
                            trace!("early stopping due to result count limit exceeded");
                            return Ok((true, out_store));
                        }
                    }
                } else {
                    out_store.put(item);
                }
            }
            poison.check()?;
        }

        Ok((should_check_limit, out_store))
    }
    fn initial_rule_meet_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        poison: Poison,
    ) -> Result<MeetAggrStore> {
        let mut out_store = MeetAggrStore::new(ruleset[0].aggr.clone())?;

        for (rule_n, rule) in ruleset.iter().enumerate() {
            debug!("initial calculation for rule {:?}.{}", rule_symb, rule_n);
            let mut aggr = rule.aggr.clone();
            for (aggr, args) in aggr.iter_mut().flatten() {
                aggr.meet_init(args)?;
            }
            for item_res in rule.relation.iter(self, None, stores)? {
                let item = item_res?;
                trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);
                out_store.meet_put(item)?;
            }
            poison.check()?;
        }
        if out_store.is_empty() && ruleset[0].aggr.iter().all(|a| a.is_some()) {
            let mut aggr = ruleset[0].aggr.clone();
            for (aggr, args) in aggr.iter_mut().flatten() {
                aggr.meet_init(args)?;
            }
            let value: Vec<_> = aggr
                .iter()
                .map(|a| -> Result<DataValue> {
                    let (aggr, _) = a.as_ref().unwrap();
                    let op = aggr.meet_op.as_ref().unwrap();
                    Ok(op.init_val())
                })
                .try_collect()?;
            out_store.meet_put(value)?;
        }
        Ok(out_store)
    }
    fn initial_rule_aggr_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<(bool, NormalTempStore)> {
        let mut out_store = NormalTempStore::default();
        let should_check_limit = limiter.total.is_some() && rule_symb.is_prog_entry();
        let mut aggr_work: BTreeMap<Vec<DataValue>, Vec<Aggregation>> = BTreeMap::new();

        for (rule_n, rule) in ruleset.iter().enumerate() {
            debug!(
                "Calculation for normal aggr rule {:?}.{}",
                rule_symb, rule_n
            );
            trace!("{:?}", rule);

            let keys_indices = rule
                .aggr
                .iter()
                .enumerate()
                .filter_map(|(i, a)| if a.is_none() { Some(i) } else { None })
                .collect_vec();
            let extract_keys = |t: &Tuple| -> Vec<DataValue> {
                keys_indices.iter().map(|i| t[*i].clone()).collect_vec()
            };

            let val_indices_and_aggrs = rule
                .aggr
                .iter()
                .enumerate()
                .filter_map(|(i, a)| a.as_ref().map(|aggr| (i, aggr.clone())))
                .collect_vec();

            for item_res in rule.relation.iter(self, None, stores)? {
                let item = item_res?;
                trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);

                let keys = extract_keys(&item);

                match aggr_work.entry(keys) {
                    Entry::Occupied(mut ent) => {
                        let aggr_ops = ent.get_mut();
                        for (aggr_idx, (tuple_idx, _)) in val_indices_and_aggrs.iter().enumerate() {
                            aggr_ops[aggr_idx]
                                .normal_op
                                .as_mut()
                                .unwrap()
                                .set(&item[*tuple_idx])?;
                        }
                    }
                    Entry::Vacant(ent) => {
                        let mut aggr_ops = Vec::with_capacity(val_indices_and_aggrs.len());
                        for (i, (aggr, params)) in &val_indices_and_aggrs {
                            let mut cur_aggr = aggr.clone();
                            cur_aggr.normal_init(params)?;
                            cur_aggr.normal_op.as_mut().unwrap().set(&item[*i])?;
                            aggr_ops.push(cur_aggr)
                        }
                        ent.insert(aggr_ops);
                    }
                }
            }
            poison.check()?;
        }

        let mut inv_indices = Vec::with_capacity(ruleset[0].aggr.len());
        let mut seen_keys = 0usize;
        let mut seen_aggrs = 0usize;
        for aggr in ruleset[0].aggr.iter() {
            if aggr.is_some() {
                inv_indices.push((true, seen_aggrs));
                seen_aggrs += 1;
            } else {
                inv_indices.push((false, seen_keys));
                seen_keys += 1;
            }
        }

        if aggr_work.is_empty() && ruleset[0].aggr.iter().all(|v| v.is_some()) {
            let empty_result: Vec<_> = ruleset[0]
                .aggr
                .iter()
                .map(|a| {
                    let (aggr, args) = a.as_ref().unwrap();
                    let mut aggr = aggr.clone();
                    aggr.normal_init(args)?;
                    let op = aggr.normal_op.unwrap();
                    op.get()
                })
                .try_collect()?;
            out_store.put(empty_result);
        }

        for (keys, aggrs) in aggr_work {
            let tuple_data: Vec<_> = inv_indices
                .iter()
                .map(|(is_aggr, idx)| {
                    if *is_aggr {
                        aggrs[*idx].normal_op.as_ref().unwrap().get()
                    } else {
                        Ok(keys[*idx].clone())
                    }
                })
                .try_collect()?;
            let tuple = tuple_data;
            if should_check_limit {
                if !out_store.exists(&tuple) {
                    if limiter.should_skip_next() {
                        out_store.put_with_skip(tuple);
                    } else {
                        out_store.put(tuple);
                    }
                    if limiter.incr_and_should_stop() {
                        return Ok((true, out_store));
                    }
                }
                // else, do nothing
            } else {
                out_store.put(tuple);
            }
        }
        Ok((should_check_limit, out_store))
    }
    fn incremental_rule_non_aggr_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        epoch: u32,
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<(bool, NormalTempStore)> {
        let prev_store = stores.get(rule_symb).unwrap();
        let mut out_store = NormalTempStore::default();
        let should_check_limit = limiter.total.is_some() && rule_symb.is_prog_entry();
        for (rule_n, rule) in ruleset.iter().enumerate() {
            let dependencies_changed = rule
                .contained_rules
                .iter()
                .map(|symb| stores.get(symb).unwrap().has_delta())
                .any(|v| v);
            if !dependencies_changed {
                continue;
            }

            for (delta_key, _) in stores.iter() {
                if !rule.contained_rules.contains(delta_key) {
                    continue;
                }
                debug!(
                    "with delta {:?} for rule {:?}.{}",
                    delta_key, rule_symb, rule_n
                );
                for item_res in rule.relation.iter(self, Some(delta_key), stores)? {
                    let item = item_res?;
                    // improvement: the clauses can actually be evaluated in parallel
                    if prev_store.exists(&item) {
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
                        if limiter.should_skip_next() {
                            out_store.put_with_skip(item);
                        } else {
                            out_store.put(item);
                        }
                        if should_check_limit && limiter.incr_and_should_stop() {
                            trace!("early stopping due to result count limit exceeded");
                            return Ok((true, out_store));
                        }
                    }
                }
                poison.check()?;
            }
        }
        Ok((should_check_limit, out_store))
    }
    fn incremental_rule_meet_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        stores: &mut BTreeMap<MagicSymbol, EpochStore>,
        poison: Poison,
    ) -> Result<MeetAggrStore> {
        let mut out_store = MeetAggrStore::new(ruleset[0].aggr.clone())?;
        for (rule_n, rule) in ruleset.iter().enumerate() {
            let dependencies_changed = rule
                .contained_rules
                .iter()
                .map(|symb| stores.get(symb).unwrap().has_delta())
                .any(|v| v);
            if !dependencies_changed {
                continue;
            }

            let mut aggr = rule.aggr.clone();
            for (aggr, args) in aggr.iter_mut().flatten() {
                aggr.meet_init(args)?;
            }

            for (delta_key, _) in stores.iter() {
                if !rule.contained_rules.contains(delta_key) {
                    continue;
                }
                debug!(
                    "with delta {:?} for rule {:?}.{}",
                    delta_key, rule_symb, rule_n
                );
                for item_res in rule.relation.iter(self, Some(delta_key), stores)? {
                    out_store.meet_put(item_res?)?;
                }
                poison.check()?;
            }
        }
        Ok(out_store)
    }
}
