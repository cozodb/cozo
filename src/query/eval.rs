use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use miette::{miette, Result};
use log::{debug, log_enabled, trace, Level};

use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::symb::PROG_ENTRY;
use crate::query::compile::{AggrKind, CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct QueryLimiter {
    limit: Option<usize>,
    counter: usize,
}

impl QueryLimiter {
    pub(crate) fn incr(&mut self) -> bool {
        if let Some(limit) = self.limit {
            self.counter += 1;
            self.counter >= limit
        } else {
            false
        }
    }
}

impl SessionTx {
    pub(crate) fn stratified_magic_evaluate(
        &self,
        strata: &[CompiledProgram],
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        num_to_take: Option<usize>,
        poison: Poison,
    ) -> Result<DerivedRelStore> {
        let ret_area = stores
            .get(&MagicSymbol::Muggle {
                inner: PROG_ENTRY.clone(),
            })
            .ok_or_else(|| miette!("program entry not found in rules"))?
            .clone();

        for (idx, cur_prog) in strata.iter().enumerate() {
            debug!("stratum {}", idx);
            self.semi_naive_magic_evaluate(cur_prog, stores, num_to_take, poison.clone())?;
        }
        Ok(ret_area)
    }
    fn semi_naive_magic_evaluate(
        &self,
        prog: &CompiledProgram,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        num_to_take: Option<usize>,
        poison: Poison,
    ) -> Result<()> {
        if log_enabled!(Level::Debug) {
            for (k, vs) in prog.iter() {
                match vs {
                    CompiledRuleSet::Rules(vs) => {
                        for (i, compiled) in vs.iter().enumerate() {
                            debug!("{:?}.{} {:#?}", k, i, compiled)
                        }
                    }
                    CompiledRuleSet::Algo(algo_apply) => {
                        debug!("{:?} {:?}", k, algo_apply)
                    }
                }
            }
        }

        let mut changed: BTreeMap<_, _> = prog.keys().map(|k| (k, false)).collect();
        let mut prev_changed = changed.clone();
        let mut limiter = QueryLimiter {
            limit: num_to_take,
            counter: 0,
        };

        for epoch in 0u32.. {
            debug!("epoch {}", epoch);
            if epoch == 0 {
                for (k, compiled_ruleset) in prog.iter() {
                    match compiled_ruleset {
                        CompiledRuleSet::Rules(ruleset) => {
                            let aggr_kind = compiled_ruleset.aggr_kind();
                            self.initial_rule_eval(
                                k,
                                ruleset,
                                aggr_kind,
                                stores,
                                &mut changed,
                                &mut limiter,
                                poison.clone(),
                            )?;
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
                            self.incremental_rule_eval(
                                k,
                                ruleset,
                                epoch,
                                is_meet_aggr,
                                stores,
                                &prev_changed,
                                &mut changed,
                                &mut limiter,
                                poison.clone(),
                            )?;
                        }

                        CompiledRuleSet::Algo(_) => unreachable!(),
                    }
                }
            }
            if changed.values().all(|rule_changed| !*rule_changed) {
                break;
            }
        }
        Ok(())
    }
    fn algo_application_eval(
        &self,
        rule_symb: &MagicSymbol,
        algo_apply: &MagicAlgoApply,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        poison: Poison,
    ) -> Result<()> {
        let mut algo_impl = algo_apply.algo.get_impl()?;
        let out = stores
            .get(rule_symb)
            .ok_or_else(|| miette!("cannot find algo store {:?}", rule_symb))?;
        algo_impl.run(
            self,
            &algo_apply.rule_args,
            &algo_apply.options,
            stores,
            out,
            poison
        )
    }
    fn initial_rule_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        aggr_kind: AggrKind,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        changed: &mut BTreeMap<&MagicSymbol, bool>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<()> {
        let store = stores.get(rule_symb).unwrap();
        let use_delta = BTreeSet::default();
        let should_check_limit = limiter.limit.is_some() && rule_symb.is_prog_entry();
        match aggr_kind {
            AggrKind::None | AggrKind::Meet => {
                let is_meet = aggr_kind == AggrKind::Meet;
                for (rule_n, rule) in ruleset.iter().enumerate() {
                    debug!("initial calculation for rule {:?}.{}", rule_symb, rule_n);
                    let mut aggr = rule.aggr.clone();
                    for el in aggr.iter_mut() {
                        if let Some((aggr, args)) = el {
                            aggr.meet_init(args)?;
                        }
                    }
                    for item_res in rule.relation.iter(self, Some(0), &use_delta)? {
                        let item = item_res?;
                        trace!("item for {:?}.{}: {:?} at {}", rule_symb, rule_n, item, 0);
                        if is_meet {
                            store.aggr_meet_put(&item, &mut aggr, 0)?;
                        } else if should_check_limit {
                            if !store.exists(&item, 0) {
                                store.put(item, 0);
                                if limiter.incr() {
                                    trace!("early stopping due to result count limit exceeded");
                                    return Ok(());
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
                let store_to_use = self.new_temp_store();
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
                    poison
                )? {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    fn incremental_rule_eval(
        &self,
        rule_symb: &MagicSymbol,
        ruleset: &[CompiledRule],
        epoch: u32,
        is_meet_aggr: bool,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        prev_changed: &BTreeMap<&MagicSymbol, bool>,
        changed: &mut BTreeMap<&MagicSymbol, bool>,
        limiter: &mut QueryLimiter,
        poison: Poison,
    ) -> Result<()> {
        let store = stores.get(rule_symb).unwrap();
        let should_check_limit = limiter.limit.is_some() && rule_symb.is_prog_entry();
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
            for el in aggr.iter_mut() {
                if let Some((aggr, args)) = el {
                    aggr.meet_init(args)?;
                }
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
                        store.put(item, 0);
                        if should_check_limit && limiter.incr() {
                            trace!("early stopping due to result count limit exceeded");
                            return Ok(());
                        }
                    }
                    poison.check()?;
                }
            }
        }
        Ok(())
    }
}
