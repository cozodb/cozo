use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use anyhow::Result;
use itertools::Itertools;
use log::{debug, log_enabled, trace, Level};

use crate::data::keyword::Keyword;
use crate::query::compile::{
    BindingHeadFormatter, BindingHeadTerm, DatalogProgram, QueryCompilationError,
};
use crate::query::relation::Relation;
use crate::runtime::temp_store::TempStore;
use crate::runtime::transact::SessionTx;

impl SessionTx {
    pub(crate) fn semi_naive_evaluate(&mut self, prog: &DatalogProgram) -> Result<TempStore> {
        let stores = prog
            .iter()
            .map(|(k, s)| (k.clone(), (self.new_throwaway(), s.arity)))
            .collect::<BTreeMap<_, _>>();
        let ret_area = stores
            .get(&Keyword::from("?"))
            .ok_or(QueryCompilationError::EntryNotFound)?
            .0
            .clone();
        let compiled: BTreeMap<_, _> = prog
            .iter()
            .map(
                |(k, body)| -> Result<(
                    Keyword,
                    Vec<(Vec<BindingHeadTerm>, BTreeSet<Keyword>, Relation)>,
                )> {
                    let mut collected = Vec::with_capacity(body.rules.len());
                    for rule in &body.rules {
                        let header = rule.head.iter().map(|t| &t.name).cloned().collect_vec();
                        let mut relation =
                            self.compile_rule_body(&rule.body, rule.vld, &stores, &header)?;
                        relation.fill_predicate_binding_indices();
                        collected.push((rule.head.clone(), rule.contained_rules(), relation));
                    }
                    Ok((k.clone(), collected))
                },
            )
            .try_collect()?;

        if log_enabled!(Level::Debug) {
            for (k, vs) in compiled.iter() {
                for (i, (binding, _, rel)) in vs.iter().enumerate() {
                    debug!(
                        "{}.{} {:?}: {:#?}",
                        k,
                        i,
                        BindingHeadFormatter(binding),
                        rel
                    )
                }
            }
        }

        let mut changed: BTreeMap<_, _> = compiled.keys().map(|k| (k, false)).collect();
        let mut prev_changed = changed.clone();

        for epoch in 0u32.. {
            debug!("epoch {}", epoch);
            if epoch == 0 {
                for (k, rules) in compiled.iter() {
                    let (store, _arity) = stores.get(k).unwrap();
                    let use_delta = BTreeSet::default();
                    for (rule_n, (_head, _deriving_rules, relation)) in rules.iter().enumerate() {
                        debug!("initial calculation for rule {}.{}", k, rule_n);
                        for item_res in relation.iter(self, Some(0), &use_delta) {
                            let item = item_res?;
                            trace!("item for {}.{}: {:?} at {}", k, rule_n, item, epoch);
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

                for (k, rules) in compiled.iter() {
                    let (store, _arity) = stores.get(k).unwrap();
                    for (rule_n, (_head, deriving_rules, relation)) in rules.iter().enumerate() {
                        let mut should_do_calculation = false;
                        for d_rule in deriving_rules {
                            if *prev_changed.get(d_rule).unwrap() {
                                should_do_calculation = true;
                                break;
                            }
                        }
                        if !should_do_calculation {
                            debug!("skipping rule {}.{} as none of its dependencies changed in the last iteration", k, rule_n);
                            continue;
                        }
                        for (delta_key, (delta_store, _)) in stores.iter() {
                            if !deriving_rules.contains(delta_key) {
                                continue;
                            }
                            debug!("with delta {} for rule {}.{}", delta_key, k, rule_n);
                            let use_delta = BTreeSet::from([delta_store.id]);
                            for item_res in relation.iter(self, Some(epoch), &use_delta) {
                                let item = item_res?;
                                // improvement: the clauses can actually be evaluated in parallel
                                if store.exists(&item, 0)? {
                                    trace!(
                                        "item for {}.{}: {:?} at {}, rederived",
                                        k,
                                        rule_n,
                                        item,
                                        epoch
                                    );
                                } else {
                                    trace!("item for {}.{}: {:?} at {}", k, rule_n, item, epoch);
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
        Ok(ret_area)
    }
}
