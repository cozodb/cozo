/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use miette::Result;

use crate::data::aggr::Aggregation;
use crate::data::program::MagicSymbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct StoredRelationId(pub(crate) u32);

impl Debug for StoredRelationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

#[derive(Clone)]
pub(crate) struct InMemRelation {
    mem_db: Rc<RefCell<Vec<Rc<RefCell<BTreeMap<Tuple, Tuple>>>>>>,
    epoch_size: Arc<AtomicU32>,
    pub(crate) id: StoredRelationId,
    pub(crate) rule_name: MagicSymbol,
    pub(crate) arity: usize,
}

impl Debug for InMemRelation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TempStore<{}>", self.id.0)
    }
}

impl InMemRelation {
    pub(crate) fn new(id: StoredRelationId, rule_name: MagicSymbol, arity: usize) -> InMemRelation {
        Self {
            epoch_size: Default::default(),
            mem_db: Default::default(),
            id,
            rule_name,
            arity,
        }
    }
    pub(crate) fn ensure_mem_db_for_epoch(&self, epoch: u32) {
        if self.epoch_size.load(Ordering::Relaxed) > epoch {
            return;
        }

        let mem_db: &RefCell<_> = self.mem_db.borrow();

        let l = mem_db.borrow().len() as i32;
        let want = (epoch + 1) as i32;
        let diff = want - l;
        if diff > 0 {
            let mut db = mem_db.borrow_mut();
            for _ in 0..diff {
                db.push(Default::default());
            }
        }
        self.epoch_size.store(epoch, Ordering::Relaxed);
    }
    pub(crate) fn aggr_meet_put(
        &self,
        tuple: &Tuple,
        aggrs: &mut [Option<(Aggregation, Vec<DataValue>)>],
        epoch: u32,
    ) -> Result<bool> {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let zero_maps = mem_db.borrow();
        let zero_map = zero_maps.get(0).unwrap();

        let zero_target: &RefCell<BTreeMap<_, _>> = zero_map.borrow();
        let mut zero_target = zero_target.borrow_mut();

        let key = aggrs
            .iter()
            .enumerate()
            .map(|(i, ma)| {
                if ma.is_none() {
                    tuple[i].clone()
                } else {
                    // placeholder for meet aggregation
                    DataValue::Guard
                }
            })
            .collect_vec();
        let prev_aggr = zero_target.get_mut(&key);

        if let Some(prev_aggr) = prev_aggr {
            let mut changed = false;
            for (i, aggr) in aggrs.iter_mut().enumerate() {
                if let Some((aggr_op, _aggr_args)) = aggr {
                    let op = aggr_op.meet_op.as_mut().unwrap();
                    changed |= op.update(&mut prev_aggr[i], &tuple[i])?;
                }
            }
            if changed && epoch != 0 {
                let epoch_maps = mem_db.borrow();
                let epoch_map = epoch_maps.get(epoch as usize).unwrap();
                let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
                let mut epoch_map = epoch_map.borrow_mut();
                epoch_map.insert(key, prev_aggr.clone());
            }
            Ok(changed)
        } else {
            let tuple_to_store: Tuple = aggrs
                .iter()
                .enumerate()
                .map(|(i, aggr)| -> Result<DataValue> {
                    if aggr.is_some() {
                        Ok(tuple[i].clone())
                    } else {
                        // placeholder for key part
                        Ok(DataValue::Guard)
                    }
                })
                .try_collect()?;
            zero_target.insert(key.clone(), tuple_to_store.clone());
            if epoch != 0 {
                let epoch_maps = mem_db.borrow();
                let epoch_map = epoch_maps.get(epoch as usize).unwrap();
                let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
                let mut epoch_map = epoch_map.borrow_mut();
                epoch_map.insert(key, tuple_to_store);
            }
            Ok(true)
        }
    }
    pub(crate) fn put(&self, tuple: Tuple, epoch: u32) {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(epoch as usize).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let mut epoch_map = epoch_map.borrow_mut();
        epoch_map.insert(tuple, Tuple::default());
    }
    pub(crate) fn put_with_skip(&self, tuple: Tuple, should_skip: bool) {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(0).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let mut epoch_map = epoch_map.borrow_mut();

        if should_skip {
            // put guard, so that when iterating results, those with guards are ignored
            epoch_map.insert(tuple, vec![DataValue::Guard]);
        } else {
            epoch_map.insert(tuple, Tuple::default());
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(0).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();
        epoch_map.is_empty()
    }
    pub(crate) fn exists(&self, tuple: &Tuple, epoch: u32) -> bool {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(epoch as usize).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();

        epoch_map.contains_key(tuple)
    }

    pub(crate) fn scan_all_for_epoch<'a>(
        &'a self,
        epoch: u32,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(epoch as usize).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();

        let collected = epoch_map
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    let combined = k
                        .iter()
                        .zip(v.iter())
                        .map(|(kel, vel)| {
                            // merge meet aggregation kv
                            if matches!(kel, DataValue::Guard) {
                                vel.clone()
                            } else {
                                kel.clone()
                            }
                        })
                        .collect_vec();
                    combined
                }
            })
            .collect_vec();
        collected.into_iter().map(Ok)
    }
    pub(crate) fn scan_all<'a>(&'a self) -> impl Iterator<Item = Result<Tuple>> + 'a {
        self.scan_all_for_epoch(0)
    }
    pub(crate) fn scan_early_returned<'a>(&'a self) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(0).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();

        let collected = epoch_map
            .iter()
            .filter_map(|(k, v)| {
                if v.is_empty() {
                    Some(k.clone())
                } else if v.last() == Some(&DataValue::Guard) {
                    // ignore since we are using :offset
                    None
                } else {
                    let combined = k
                        .iter()
                        .zip(v.iter())
                        .map(|(kel, vel)| {
                            // merge kv parts of meet aggr
                            if matches!(kel, DataValue::Guard) {
                                vel.clone()
                            } else {
                                kel.clone()
                            }
                        })
                        .collect_vec();
                    Some(combined)
                }
            })
            .collect_vec();
        collected.into_iter().map(Ok)
    }
    pub(crate) fn scan_prefix(&self, prefix: &Tuple) -> impl Iterator<Item = Result<Tuple>> {
        self.scan_prefix_for_epoch(prefix, 0)
    }
    pub(crate) fn scan_prefix_for_epoch(
        &self,
        prefix: &Tuple,
        epoch: u32,
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut upper = prefix.clone();
        upper.push(DataValue::Bot);
        let upper = upper;
        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(epoch as usize).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();

        let collected = epoch_map
            .range(prefix.clone()..=upper)
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    let combined = k
                        .iter()
                        .zip(v.iter())
                        .map(|(kel, vel)| {
                            // merge kv parts of meet aggr
                            if matches!(kel, DataValue::Guard) {
                                vel.clone()
                            } else {
                                kel.clone()
                            }
                        })
                        .collect_vec();
                    combined
                }
            })
            .collect_vec();
        collected.into_iter().map(Ok)
    }
    pub(crate) fn scan_bounded_prefix_for_epoch(
        &self,
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
        epoch: u32,
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut prefix_bound = prefix.clone();
        prefix_bound.extend_from_slice(lower);
        let mut upper_bound = prefix.clone();
        upper_bound.extend_from_slice(upper);

        let mem_db: &RefCell<_> = self.mem_db.borrow();
        let epoch_maps = mem_db.borrow();
        let epoch_map = epoch_maps.get(epoch as usize).unwrap();
        let epoch_map: &RefCell<BTreeMap<_, _>> = epoch_map.borrow();
        let epoch_map = epoch_map.borrow();

        let res = epoch_map
            .range(prefix_bound..=upper_bound)
            .map(|(k, _v)| k.clone())
            .collect_vec();
        res.into_iter().map(Ok)
    }
}

// meet put
