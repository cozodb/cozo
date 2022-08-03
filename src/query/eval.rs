use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use anyhow::{anyhow, Result};
use log::{debug, log_enabled, trace, Level};

use crate::data::keyword::PROG_ENTRY;
use crate::data::program::MagicKeyword;
use crate::query::compile::CompiledProgram;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn stratified_magic_evaluate(
        &mut self,
        strata: &[CompiledProgram],
        stores: &BTreeMap<MagicKeyword, TempStore>,
    ) -> Result<TempStore> {
        let ret_area = stores
            .get(&MagicKeyword::Muggle {
                inner: PROG_ENTRY.clone(),
            })
            .ok_or_else(|| anyhow!("program entry not found in rules"))?
            .clone();

        for (idx, cur_prog) in strata.iter().rev().enumerate() {
            debug!("stratum {}", idx);
            self.semi_naive_magic_evaluate(cur_prog, &stores)?;
        }
        Ok(ret_area)
    }
    fn semi_naive_magic_evaluate(
        &mut self,
        prog: &CompiledProgram,
        stores: &BTreeMap<MagicKeyword, TempStore>,
    ) -> Result<()> {
        if log_enabled!(Level::Debug) {
            for (k, vs) in prog.iter() {
                for (i, (binding, _, rel)) in vs.iter().enumerate() {
                    debug!("{:?}.{} {:?}: {:#?}", k, i, binding, rel)
                }
            }
        }

        let mut changed: BTreeMap<_, _> = prog.keys().map(|k| (k, false)).collect();
        let mut prev_changed = changed.clone();

        for epoch in 0u32.. {
            debug!("epoch {}", epoch);
            if epoch == 0 {
                for (k, rules) in prog.iter() {
                    let store = stores.get(k).unwrap();
                    let use_delta = BTreeSet::default();
                    for (rule_n, (_head, _deriving_rules, relation)) in rules.iter().enumerate() {
                        debug!("initial calculation for rule {:?}.{}", k, rule_n);
                        for item_res in relation.iter(self, Some(0), &use_delta) {
                            let item = item_res?;
                            trace!("item for {:?}.{}: {:?} at {}", k, rule_n, item, epoch);
                            store.put(&item, 0)?;
                            *changed.get_mut(k).unwrap() = true;
                        }
                    }
                }
            } else {
                mem::swap(&mut changed, &mut prev_changed);
                for (_k, v) in changed.iter_mut() {
                    *v = false;
                }

                for (k, rules) in prog.iter() {
                    let store = stores.get(k).unwrap();
                    for (rule_n, (_head, deriving_rules, relation)) in rules.iter().enumerate() {
                        let mut should_do_calculation = false;
                        for d_rule in deriving_rules {
                            if let Some(changed) = prev_changed.get(d_rule) {
                                if *changed {
                                    should_do_calculation = true;
                                    break;
                                }
                            }
                        }
                        if !should_do_calculation {
                            // debug!("skip {}.{}", k, rule_n);
                            continue;
                        }
                        for (delta_key, delta_store) in stores.iter() {
                            if !deriving_rules.contains(delta_key) {
                                continue;
                            }
                            debug!("with delta {:?} for rule {:?}.{}", delta_key, k, rule_n);
                            let use_delta = BTreeSet::from([delta_store.id]);
                            for item_res in relation.iter(self, Some(epoch), &use_delta) {
                                let item = item_res?;
                                // improvement: the clauses can actually be evaluated in parallel
                                if store.exists(&item, 0)? {
                                    trace!(
                                        "item for {:?}.{}: {:?} at {}, rederived",
                                        k,
                                        rule_n,
                                        item,
                                        epoch
                                    );
                                } else {
                                    trace!("item for {:?}.{}: {:?} at {}", k, rule_n, item, epoch);
                                    *changed.get_mut(k).unwrap() = true;
                                    store.put(&item, epoch)?;
                                    store.put(&item, 0)?;
                                }
                            }
                        }
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
