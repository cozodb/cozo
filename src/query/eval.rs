use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use anyhow::{anyhow, Result};
use log::{debug, log_enabled, trace, Level};

use crate::data::program::MagicSymbol;
use crate::data::symb::PROG_ENTRY;
use crate::query::compile::{AggrKind, CompiledProgram, CompiledRuleSet};
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
        &mut self,
        strata: &[CompiledProgram],
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        num_to_take: Option<usize>,
    ) -> Result<DerivedRelStore> {
        let ret_area = stores
            .get(&MagicSymbol::Muggle {
                inner: PROG_ENTRY.clone(),
            })
            .ok_or_else(|| anyhow!("program entry not found in rules"))?
            .clone();

        for (idx, cur_prog) in strata.iter().enumerate() {
            debug!("stratum {}", idx);
            self.semi_naive_magic_evaluate(cur_prog, &stores, num_to_take)?;
        }
        Ok(ret_area)
    }
    fn semi_naive_magic_evaluate(
        &mut self,
        prog: &CompiledProgram,
        stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        num_to_take: Option<usize>,
    ) -> Result<()> {
        if log_enabled!(Level::Debug) {
            for (k, vs) in prog.iter() {
                match vs {
                    CompiledRuleSet::Rules(vs) => {
                        for (i, compiled) in vs.iter().enumerate() {
                            debug!("{:?}.{} {:#?}", k, i, compiled)
                        }
                    }
                    CompiledRuleSet::Algo => {
                        todo!()
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
                            let store = stores.get(k).unwrap();
                            let use_delta = BTreeSet::default();
                            let should_check_limit = num_to_take.is_some() && k.is_prog_entry();
                            match aggr_kind {
                                AggrKind::None | AggrKind::Meet => {
                                    let is_meet = aggr_kind == AggrKind::Meet;
                                    for (rule_n, rule) in ruleset.iter().enumerate() {
                                        debug!("initial calculation for rule {:?}.{}", k, rule_n);
                                        for item_res in
                                            rule.relation.iter(self, Some(0), &use_delta)?
                                        {
                                            let item = item_res?;
                                            trace!(
                                                "item for {:?}.{}: {:?} at {}",
                                                k,
                                                rule_n,
                                                item,
                                                epoch
                                            );
                                            if is_meet {
                                                store.aggr_meet_put(&item, &rule.aggr, 0)?;
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
                                            *changed.get_mut(k).unwrap() = true;
                                        }
                                    }
                                }
                                AggrKind::Normal => {
                                    let store_to_use = self.new_temp_store();
                                    for (rule_n, rule) in ruleset.iter().enumerate() {
                                        debug!(
                                            "Calculation for normal aggr rule {:?}.{}",
                                            k, rule_n
                                        );
                                        for (serial, item_res) in rule
                                            .relation
                                            .iter(self, Some(0), &use_delta)?
                                            .enumerate()
                                        {
                                            let item = item_res?;
                                            trace!(
                                                "item for {:?}.{}: {:?} at {}",
                                                k,
                                                rule_n,
                                                item,
                                                epoch
                                            );
                                            store_to_use.normal_aggr_put(&item, &rule.aggr, serial);
                                            *changed.get_mut(k).unwrap() = true;
                                        }
                                    }
                                    if store_to_use.normal_aggr_scan_and_put(
                                        &ruleset[0].aggr,
                                        store,
                                        if should_check_limit {
                                            Some(&mut limiter)
                                        } else {
                                            None
                                        },
                                    )? {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        CompiledRuleSet::Algo => {
                            todo!()
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
                            let store = stores.get(k).unwrap();
                            let should_check_limit = num_to_take.is_some() && k.is_prog_entry();
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
                                for (delta_key, delta_store) in stores.iter() {
                                    if !rule.contained_rules.contains(delta_key) {
                                        continue;
                                    }
                                    let is_meet_aggr = match compiled_ruleset.aggr_kind() {
                                        AggrKind::None => false,
                                        AggrKind::Normal => unreachable!(),
                                        AggrKind::Meet => true,
                                    };

                                    debug!(
                                        "with delta {:?} for rule {:?}.{}",
                                        delta_key, k, rule_n
                                    );
                                    let use_delta = BTreeSet::from([delta_store.id]);
                                    for item_res in
                                        rule.relation.iter(self, Some(epoch), &use_delta)?
                                    {
                                        let item = item_res?;
                                        // improvement: the clauses can actually be evaluated in parallel
                                        if is_meet_aggr {
                                            let aggr_changed =
                                                store.aggr_meet_put(&item, &rule.aggr, epoch)?;
                                            if aggr_changed {
                                                *changed.get_mut(k).unwrap() = true;
                                            }
                                        } else if store.exists(&item, 0) {
                                            trace!(
                                                "item for {:?}.{}: {:?} at {}, rederived",
                                                k,
                                                rule_n,
                                                item,
                                                epoch
                                            );
                                        } else {
                                            trace!(
                                                "item for {:?}.{}: {:?} at {}",
                                                k,
                                                rule_n,
                                                item,
                                                epoch
                                            );
                                            *changed.get_mut(k).unwrap() = true;
                                            store.put(item.clone(), epoch);
                                            store.put(item, 0);
                                            if should_check_limit && limiter.incr() {
                                                trace!("early stopping due to result count limit exceeded");
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        CompiledRuleSet::Algo => unreachable!(),
                    }
                }
            }
            if changed.values().all(|rule_changed| !*rule_changed) {
                break;
            }
        }
        Ok(())
    }
}
