/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::borrow::BorrowMut;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use either::{Left, Right};
use itertools::Itertools;
use miette::Result;

use crate::data::aggr::Aggregation;
use crate::data::program::MagicSymbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::query::eval::QueryLimiter;
use crate::runtime::db::Poison;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) struct StoredRelationId(pub(crate) u32);

impl Debug for StoredRelationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

#[derive(Clone)]
pub(crate) struct InMemRelation {
    mem_db: Arc<RwLock<Vec<Arc<RwLock<BTreeMap<Tuple, Tuple>>>>>>,
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
    fn ensure_mem_db_for_epoch(&self, epoch: u32) {
        if self.epoch_size.load(Ordering::Relaxed) > epoch {
            return;
        }
        let l = self.mem_db.try_read().unwrap().len() as i32;
        let want = (epoch + 1) as i32;
        let diff = want - l;
        if diff > 0 {
            let mut db = self.mem_db.try_write().unwrap();
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
        self.ensure_mem_db_for_epoch(epoch);
        let db_target = self.mem_db.try_read().unwrap();
        let mut zero_target = db_target.get(0).unwrap().try_write().unwrap();
        let key = Tuple(
            aggrs
                .iter()
                .enumerate()
                .map(|(i, ma)| {
                    if ma.is_none() {
                        tuple.0[i].clone()
                    } else {
                        DataValue::Guard
                    }
                })
                .collect_vec(),
        );
        let prev_aggr = zero_target.get_mut(&key);

        if let Some(prev_aggr) = prev_aggr {
            let mut changed = false;
            for (i, aggr) in aggrs.iter_mut().enumerate() {
                if let Some((aggr_op, _aggr_args)) = aggr {
                    let op = aggr_op.meet_op.as_mut().unwrap();
                    changed |= op.update(&mut prev_aggr.0[i], &tuple.0[i])?;
                }
            }
            if changed && epoch != 0 {
                let mut epoch_target = db_target.get(epoch as usize).unwrap().try_write().unwrap();
                epoch_target.insert(key, prev_aggr.clone());
            }
            Ok(changed)
        } else {
            let tuple_to_store = Tuple(
                aggrs
                    .iter()
                    .enumerate()
                    .map(|(i, aggr)| -> Result<DataValue> {
                        if aggr.is_some() {
                            Ok(tuple.0[i].clone())
                        } else {
                            Ok(DataValue::Guard)
                        }
                    })
                    .try_collect()?,
            );
            zero_target.insert(key.clone(), tuple_to_store.clone());
            if epoch != 0 {
                let mut zero = db_target.get(epoch as usize).unwrap().try_write().unwrap();
                zero.insert(key, tuple_to_store);
            }
            Ok(true)
        }
    }
    pub(crate) fn put(&self, tuple: Tuple, epoch: u32) {
        self.ensure_mem_db_for_epoch(epoch);
        let db = self.mem_db.try_read().unwrap();
        let mut target = db.get(epoch as usize).unwrap().try_write().unwrap();
        target.insert(tuple, Tuple::default());
    }
    pub(crate) fn put_with_skip(&self, tuple: Tuple, should_skip: bool) {
        self.ensure_mem_db_for_epoch(0);
        let db = self.mem_db.try_read().unwrap();
        let mut target = db.get(0).unwrap().try_write().unwrap();
        if should_skip {
            target.insert(tuple, Tuple(vec![DataValue::Guard]));
        } else {
            target.insert(tuple, Tuple::default());
        }
    }
    pub(crate) fn normal_aggr_put(
        &self,
        tuple: &Tuple,
        aggrs: &[Option<(Aggregation, Vec<DataValue>)>],
        serial: usize,
    ) {
        self.ensure_mem_db_for_epoch(0);
        let mut vals = vec![];
        for (idx, agg) in aggrs.iter().enumerate() {
            if agg.is_none() {
                vals.push(tuple.0[idx].clone());
            }
        }
        for (idx, agg) in aggrs.iter().enumerate() {
            if agg.is_some() {
                vals.push(tuple.0[idx].clone());
            }
        }
        vals.push(DataValue::from(serial as i64));

        let target = self.mem_db.try_read().unwrap();
        let mut target = target.get(0).unwrap().try_write().unwrap();
        target.insert(Tuple(vals), Tuple::default());
    }
    pub(crate) fn exists(&self, tuple: &Tuple, epoch: u32) -> bool {
        self.ensure_mem_db_for_epoch(epoch);
        let target = self.mem_db.try_read().unwrap();
        let target = target.get(epoch as usize).unwrap().try_read().unwrap();
        target.contains_key(tuple)
    }

    pub(crate) fn normal_aggr_scan_and_put(
        &self,
        aggrs: &[Option<(Aggregation, Vec<DataValue>)>],
        store: &InMemRelation,
        mut limiter: Option<&mut QueryLimiter>,
        poison: Poison,
    ) -> Result<bool> {
        let db_target = self.mem_db.try_read().unwrap();
        let target = db_target.get(0);
        let it = match target {
            None => Left(iter::empty()),
            Some(target) => {
                let target = target.try_read().unwrap();
                Right(target.clone().into_iter().map(|(k, v)| {
                    if v.0.is_empty() {
                        k
                    } else {
                        let combined =
                            k.0.into_iter()
                                .zip(v.0.into_iter())
                                .map(|(kel, vel)| {
                                    if matches!(kel, DataValue::Guard) {
                                        vel
                                    } else {
                                        kel
                                    }
                                })
                                .collect_vec();
                        Tuple(combined)
                    }
                }))
            }
        };

        let mut aggrs = aggrs.to_vec();
        let n_keys = aggrs.iter().filter(|aggr| aggr.is_none()).count();
        let grouped = it.group_by(move |tuple| tuple.0[..n_keys].to_vec());
        let mut invert_indices = vec![];
        for (idx, aggr) in aggrs.iter().enumerate() {
            if aggr.is_none() {
                invert_indices.push(idx);
            }
        }
        for (idx, aggr) in aggrs.iter().enumerate() {
            if aggr.is_some() {
                invert_indices.push(idx);
            }
        }
        let invert_indices = invert_indices
            .into_iter()
            .enumerate()
            .sorted_by_key(|(_a, b)| *b)
            .map(|(a, _b)| a)
            .collect_vec();
        for (_key, mut group_iter) in grouped.into_iter() {
            for (aggr, args) in aggrs.iter_mut().flatten() {
                aggr.normal_init(args)?;
            }
            let mut aggr_res = vec![DataValue::Guard; aggrs.len()];
            let first_tuple = group_iter.next().unwrap();
            for (idx, aggr) in aggrs.iter_mut().enumerate() {
                let val = &first_tuple.0[invert_indices[idx]];
                if let Some((aggr_op, _aggr_args)) = aggr {
                    let op = aggr_op.normal_op.as_mut().unwrap();
                    op.set(val)?;
                } else {
                    aggr_res[idx] = first_tuple.0[invert_indices[idx]].clone();
                }
            }
            for tuple in group_iter {
                for (idx, aggr) in aggrs.iter_mut().enumerate() {
                    let val = &tuple.0[invert_indices[idx]];
                    if let Some((aggr_op, _aggr_args)) = aggr {
                        let op = aggr_op.normal_op.as_mut().unwrap();
                        op.set(val)?;
                    }
                }
                poison.check()?;
            }
            for (i, aggr) in aggrs.iter().enumerate() {
                if let Some((aggr_op, _aggr_args)) = aggr {
                    let op = aggr_op.normal_op.as_ref().unwrap();
                    aggr_res[i] = op.get()?;
                }
            }
            let res_tpl = Tuple(aggr_res);
            if let Some(lmt) = limiter.borrow_mut() {
                if !store.exists(&res_tpl, 0) {
                    store.put_with_skip(res_tpl, lmt.should_skip_next());
                    if lmt.incr_and_should_stop() {
                        return Ok(true);
                    }
                }
            } else {
                store.put(res_tpl, 0);
            }
        }
        Ok(false)
    }

    pub(crate) fn scan_all_for_epoch(&self, epoch: u32) -> impl Iterator<Item = Result<Tuple>> {
        self.ensure_mem_db_for_epoch(epoch);
        let db = self
            .mem_db
            .try_read()
            .unwrap()
            .get(epoch as usize)
            .unwrap()
            .clone()
            .try_read()
            .unwrap()
            .clone();
        db.into_iter().map(|(k, v)| {
            if v.0.is_empty() {
                Ok(k)
            } else {
                let combined =
                    k.0.into_iter()
                        .zip(v.0.into_iter())
                        .map(|(kel, vel)| {
                            if matches!(kel, DataValue::Guard) {
                                vel
                            } else {
                                kel
                            }
                        })
                        .collect_vec();
                Ok(Tuple(combined))
            }
        })
    }
    pub(crate) fn scan_all(&self) -> impl Iterator<Item = Result<Tuple>> {
        self.scan_all_for_epoch(0)
    }
    pub(crate) fn scan_early_returned(&self) -> impl Iterator<Item = Result<Tuple>> {
        self.ensure_mem_db_for_epoch(0);
        let db = self
            .mem_db
            .try_read()
            .unwrap()
            .get(0)
            .unwrap()
            .clone()
            .try_read()
            .unwrap()
            .clone();
        db.into_iter().filter_map(|(k, v)| {
            if v.0.is_empty() {
                Some(Ok(k))
            } else if v.0.last() == Some(&DataValue::Guard) {
                None
            } else {
                let combined =
                    k.0.iter()
                        .zip(v.0.iter())
                        .map(|(kel, vel)| {
                            if matches!(kel, DataValue::Guard) {
                                vel.clone()
                            } else {
                                kel.clone()
                            }
                        })
                        .collect_vec();
                Some(Ok(Tuple(combined)))
            }
        })
    }
    pub(crate) fn scan_prefix(&self, prefix: &Tuple) -> impl Iterator<Item = Result<Tuple>> {
        self.scan_prefix_for_epoch(prefix, 0)
    }
    pub(crate) fn scan_prefix_for_epoch(
        &self,
        prefix: &Tuple,
        epoch: u32,
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut upper = prefix.0.clone();
        upper.push(DataValue::Bot);
        let upper = Tuple(upper);
        self.ensure_mem_db_for_epoch(epoch);
        let target = self.mem_db.try_read().unwrap();
        let target = target.get(epoch as usize).unwrap().try_read().unwrap();
        let res = target
            .range((Included(prefix), Included(&upper)))
            .map(|(k, v)| {
                if v.0.is_empty() {
                    Ok(k.clone())
                } else {
                    let combined =
                        k.0.iter()
                            .zip(v.0.iter())
                            .map(|(kel, vel)| {
                                if matches!(kel, DataValue::Guard) {
                                    vel.clone()
                                } else {
                                    kel.clone()
                                }
                            })
                            .collect_vec();
                    Ok(Tuple(combined))
                }
            })
            .collect_vec();
        res.into_iter()
    }
    pub(crate) fn scan_bounded_prefix_for_epoch(
        &self,
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
        epoch: u32,
    ) -> impl Iterator<Item = Result<Tuple>> {
        self.ensure_mem_db_for_epoch(epoch);
        let mut prefix_bound = prefix.clone();
        prefix_bound.0.extend_from_slice(lower);
        let mut upper_bound = prefix.clone();
        upper_bound.0.extend_from_slice(upper);
        let target = self.mem_db.try_read().unwrap();
        let target = target.get(epoch as usize).unwrap().try_read().unwrap();
        let res = target
            .range((Included(&prefix_bound), Included(&upper_bound)))
            .map(|(k, _v)| Ok(k.clone()))
            .collect_vec();
        res.into_iter()
    }
}
