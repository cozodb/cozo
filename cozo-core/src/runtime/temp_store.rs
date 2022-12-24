/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::Bound::Included;
use std::mem;
use std::ops::Bound::Excluded;

use either::{Left, Right};
use itertools::Itertools;
use miette::Result;

use crate::data::aggr::Aggregation;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

/// A store holding temp data during evaluation of queries.
/// The public interface is used in custom implementations of algorithms/utilities.
#[derive(Default, Debug)]
pub struct RegularTempStore {
    inner: BTreeMap<Tuple, bool>,
}

const EMPTY_TUPLE_REF: &Tuple = &vec![];

impl RegularTempStore {
    pub(crate) fn wrap(self) -> TempStore {
        TempStore::Normal(self)
    }
    /// Tests if a key already exists in the store.
    pub fn exists(&self, key: &Tuple) -> bool {
        self.inner.contains_key(key)
    }

    fn range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        let lower_bound = Included(lower.to_vec());
        let upper_bound = if upper_inclusive {
            Included(upper.to_vec())
        } else {
            Excluded(upper.to_vec())
        };
        self.inner
            .range((lower_bound, upper_bound))
            .map(|(t, skip)| TupleInIter(t, EMPTY_TUPLE_REF, *skip))
    }
    /// Add a tuple to the store
    pub fn put(&mut self, tuple: Tuple) {
        self.inner.insert(tuple, false);
    }
    pub(crate) fn put_with_skip(&mut self, tuple: Tuple) {
        self.inner.insert(tuple, true);
    }
    // returns true if prev is guaranteed to be the same as self after this function call,
    // false if we are not sure.
    pub(crate) fn merge_in(&mut self, prev: &mut Self, mut new: Self) -> bool {
        prev.inner.clear();
        if new.inner.is_empty() {
            return false;
        }
        if self.inner.is_empty() {
            mem::swap(&mut new, self);
            return true;
        }
        for (k, v) in new.inner {
            match self.inner.entry(k) {
                Entry::Vacant(ent) => {
                    prev.inner.insert(ent.key().clone(), v);
                    ent.insert(v);
                }
                Entry::Occupied(mut ent) => {
                    ent.insert(v);
                }
            }
        }
        false
    }
}

#[derive(Debug)]
pub(crate) struct MeetAggrStore {
    inner: BTreeMap<Tuple, Tuple>,
    aggregations: Vec<(Aggregation, Vec<DataValue>)>,
    grouping_len: usize,
}

impl MeetAggrStore {
    pub(crate) fn wrap(self) -> TempStore {
        TempStore::MeetAggr(self)
    }
    pub(crate) fn exists(&self, key: &Tuple) -> bool {
        let truncated = &key[0..self.grouping_len];
        self.inner.contains_key(truncated)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub(crate) fn new(aggrs: Vec<Option<(Aggregation, Vec<DataValue>)>>) -> Result<Self> {
        let total_key_len = aggrs.len();
        let mut aggregations = aggrs.into_iter().flatten().collect_vec();
        for (aggr, args) in aggregations.iter_mut() {
            aggr.meet_init(args)?;
        }
        let grouping_len = total_key_len - aggregations.len();
        Ok(Self {
            inner: Default::default(),
            aggregations,
            grouping_len,
        })
    }
    // also need to check if value exists beforehand! use the idempotency!
    // need to think this through more carefully.
    pub(crate) fn meet_put(&mut self, tuple: Tuple) -> Result<bool> {
        let (key_part, val_part) = tuple.split_at(self.grouping_len);
        match self.inner.get_mut(key_part) {
            Some(prev_aggr) => {
                let mut changed = false;
                for (i, (aggr_op, _)) in self.aggregations.iter().enumerate() {
                    let op = aggr_op.meet_op.as_ref().unwrap();
                    changed |= op.update(&mut prev_aggr[i], &val_part[i])?;
                }
                Ok(changed)
            }
            None => {
                self.inner.insert(key_part.to_vec(), val_part.to_vec());
                Ok(true)
            }
        }
    }
    fn range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        let lower_key = if lower.len() > self.grouping_len {
            lower[0..self.grouping_len].to_vec()
        } else {
            lower.to_vec()
        };
        let upper_key = if upper.len() > self.grouping_len {
            upper[0..self.grouping_len].to_vec()
        } else {
            upper.to_vec()
        };
        let lower = lower.to_vec();
        let upper = upper.to_vec();
        self.inner
            .range(lower_key..=upper_key)
            .filter_map(move |(k, v)| {
                let ret = TupleInIter(k, v, false);
                if ret.partial_cmp(&lower as &[DataValue]) == Some(Ordering::Less) {
                    None
                } else {
                    match ret.partial_cmp(&upper as &[DataValue]).unwrap() {
                        Ordering::Less => Some(ret),
                        Ordering::Equal => {
                            if upper_inclusive {
                                Some(ret)
                            } else {
                                None
                            }
                        }
                        Ordering::Greater => None,
                    }
                }
            })
    }
    /// returns true if prev is guaranteed to be the same as self after this function call,
    /// false if we are not sure.
    pub(crate) fn merge_in(&mut self, prev: &mut Self, mut new: Self) -> Result<bool> {
        prev.inner.clear();
        if new.inner.is_empty() {
            return Ok(false);
        }
        if self.inner.is_empty() {
            mem::swap(self, &mut new);
            return Ok(true);
        }
        for (k, v) in new.inner {
            match self.inner.entry(k) {
                Entry::Vacant(ent) => {
                    prev.inner.insert(ent.key().clone(), v.clone());
                    ent.insert(v);
                }
                Entry::Occupied(mut ent) => {
                    let mut changed = false;
                    {
                        let target = ent.get_mut();
                        for (i, (aggr_op, _)) in self.aggregations.iter().enumerate() {
                            let op = aggr_op.meet_op.as_ref().unwrap();
                            changed |= op.update(&mut target[i], &v[i])?;
                        }
                    }
                    if changed {
                        prev.inner.insert(ent.key().clone(), ent.get().clone());
                    }
                }
            }
        }
        Ok(false)
    }
}

#[derive(Debug)]
pub(crate) enum TempStore {
    Normal(RegularTempStore),
    MeetAggr(MeetAggrStore),
}

impl TempStore {
    fn exists(&self, key: &Tuple) -> bool {
        match self {
            TempStore::Normal(n) => n.exists(key),
            TempStore::MeetAggr(m) => m.exists(key),
        }
    }
    fn range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        match self {
            TempStore::Normal(n) => Left(n.range_iter(lower, upper, upper_inclusive)),
            TempStore::MeetAggr(m) => Right(m.range_iter(lower, upper, upper_inclusive)),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            TempStore::Normal(n) => n.inner.is_empty(),
            TempStore::MeetAggr(m) => m.inner.is_empty(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct EpochStore {
    total: TempStore,
    delta: TempStore,
    use_total_for_delta: bool,
    pub(crate) arity: usize,
}

impl EpochStore {
    pub(crate) fn exists(&self, key: &Tuple) -> bool {
        self.total.exists(key)
    }
    pub(crate) fn new_normal(arity: usize) -> Self {
        Self {
            total: TempStore::Normal(RegularTempStore::default()),
            delta: TempStore::Normal(RegularTempStore::default()),
            use_total_for_delta: true,
            arity,
        }
    }
    pub(crate) fn new_meet(aggrs: &[Option<(Aggregation, Vec<DataValue>)>]) -> Result<Self> {
        Ok(Self {
            total: TempStore::MeetAggr(MeetAggrStore::new(aggrs.to_vec())?),
            delta: TempStore::MeetAggr(MeetAggrStore::new(aggrs.to_vec())?),
            use_total_for_delta: true,
            arity: aggrs.len(),
        })
    }
    pub(crate) fn merge_in(&mut self, new: TempStore) -> Result<()> {
        match (&mut self.total, &mut self.delta, new) {
            (TempStore::Normal(total), TempStore::Normal(prev), TempStore::Normal(new)) => {
                self.use_total_for_delta = total.merge_in(prev, new);
            }
            (TempStore::MeetAggr(total), TempStore::MeetAggr(prev), TempStore::MeetAggr(new)) => {
                self.use_total_for_delta = total.merge_in(prev, new)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
    pub(crate) fn has_delta(&self) -> bool {
        if self.use_total_for_delta {
            !self.total.is_empty()
        } else {
            !self.delta.is_empty()
        }
    }
    pub(crate) fn range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        self.total.range_iter(lower, upper, upper_inclusive)
    }
    pub(crate) fn delta_range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        if self.use_total_for_delta {
            self.total.range_iter(lower, upper, upper_inclusive)
        } else {
            self.delta.range_iter(lower, upper, upper_inclusive)
        }
    }
    pub(crate) fn prefix_iter(&self, prefix: &Tuple) -> impl Iterator<Item = TupleInIter<'_>> {
        let mut upper = prefix.to_vec();
        upper.push(DataValue::Bot);
        self.range_iter(prefix, &upper, true)
    }
    pub(crate) fn delta_prefix_iter(
        &self,
        prefix: &Tuple,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        let mut upper = prefix.to_vec();
        upper.push(DataValue::Bot);
        self.delta_range_iter(prefix, &upper, true)
    }
    pub(crate) fn all_iter(&self) -> impl Iterator<Item = TupleInIter<'_>> {
        self.prefix_iter(&vec![])
    }
    pub(crate) fn delta_all_iter(&self) -> impl Iterator<Item = TupleInIter<'_>> {
        self.delta_prefix_iter(&vec![])
    }
    pub(crate) fn early_returned_iter(&self) -> impl Iterator<Item = TupleInIter<'_>> {
        self.all_iter().filter(|t| !t.should_skip())
    }
}

#[derive(Copy, Clone)]
pub(crate) struct TupleInIter<'a>(&'a Tuple, &'a Tuple, bool);

impl<'a> TupleInIter<'a> {
    pub(crate) fn get(self, idx: usize) -> &'a DataValue {
        self.0
            .get(idx)
            .unwrap_or_else(|| self.1.get(idx - self.0.len()).unwrap())
    }
    fn should_skip(&self) -> bool {
        self.2
    }
    pub(crate) fn into_tuple(self) -> Tuple {
        self.into_iter().cloned().collect_vec()
    }
}

impl<'a> IntoIterator for TupleInIter<'a> {
    type Item = &'a DataValue;
    type IntoIter = TupleInIterIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TupleInIterIterator {
            inner: self,
            idx: 0,
        }
    }
}

pub(crate) struct TupleInIterIterator<'a> {
    inner: TupleInIter<'a>,
    idx: usize,
}

impl PartialEq for TupleInIter<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.into_iter().eq(other.into_iter())
    }
}

impl Eq for TupleInIter<'_> {}

impl Ord for TupleInIter<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.into_iter().cmp(other.into_iter())
    }
}

impl PartialOrd for TupleInIter<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq<[DataValue]> for TupleInIter<'_> {
    fn eq(&self, other: &'_ [DataValue]) -> bool {
        self.into_iter().eq(other.iter())
    }
}

impl PartialOrd<[DataValue]> for TupleInIter<'_> {
    fn partial_cmp(&self, other: &'_ [DataValue]) -> Option<Ordering> {
        self.into_iter().partial_cmp(other.iter())
    }
}

impl<'a> Iterator for TupleInIterIterator<'a> {
    type Item = &'a DataValue;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = match self.inner.0.get(self.idx) {
            Some(d) => d,
            None => match self.inner.1.get(self.idx - self.inner.0.len()) {
                None => return None,
                Some(d) => d,
            },
        };
        self.idx += 1;
        Some(ret)
    }
}
