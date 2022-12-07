/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::Bound::Included;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::ops::Bound::Excluded;

use either::{Left, Right};
use itertools::Itertools;
use miette::{Diagnostic, Result};
use thiserror::Error;

use crate::data::aggr::Aggregation;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

type Store = BTreeMap<Tuple, Tuple>;

#[derive(Default)]
pub(crate) struct NormalTempStore {
    inner: BTreeMap<Tuple, bool>,
}

const EMPTY_TUPLE_REF: &Tuple = &vec![];

impl NormalTempStore {
    pub(crate) fn wrap(self) -> TempStore {
        TempStore::Normal(self)
    }
    pub(crate) fn exists(&self, old: Option<&EpochStore>, key: &Tuple) -> bool {
        // if let Some(old_store) = old {
        //     if old_store.
        // }
        todo!()
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
    // must check prev_store for existence before putting here!
    pub(crate) fn put(&mut self, tuple: Tuple) {
        self.inner.insert(tuple, false);
    }
    // must check prev_store for existence before putting here!
    pub(crate) fn put_with_skip(&mut self, tuple: Tuple) {
        self.inner.insert(tuple, true);
    }
    fn merge(&mut self, mut other: Self) {
        if self.inner.len() < other.inner.len() {
            mem::swap(&mut self.inner, &mut other.inner);
        }
        if other.inner.is_empty() {
            return;
        }
        self.inner.extend(other.inner)
    }
}

pub(crate) struct MeetAggrStore {
    inner: BTreeMap<Tuple, Tuple>,
    aggregations: Vec<(Aggregation, Vec<DataValue>)>,
    grouping_len: usize,
}

// optimization: MeetAggrStore can be used to simulate functional dependency

impl MeetAggrStore {
    pub(crate) fn wrap(self) -> TempStore {
        TempStore::MeetAggr(self)
    }
    pub(crate) fn exists(&self, old: Option<&EpochStore>, key: &Tuple) -> bool {
        // if let Some(old_store) = old {
        //     if old_store.
        // }
        todo!()
    }
    pub(crate) fn is_empty(&self, old: Option<&EpochStore>) -> bool {
        todo!()
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
    fn merge(&mut self, mut other: Self) -> Result<()> {
        if self.inner.len() < other.inner.len() {
            mem::swap(&mut self.inner, &mut other.inner);
        }
        if other.inner.is_empty() {
            return Ok(());
        }
        for (k, v) in other.inner {
            match self.inner.entry(k) {
                Entry::Vacant(ent) => {
                    ent.insert(v);
                }
                Entry::Occupied(mut ent) => {
                    let current = ent.get_mut();
                    for (i, (aggr, _)) in self.aggregations.iter().enumerate() {
                        let op = aggr.meet_op.as_ref().unwrap();
                        op.update(&mut current[i], &v[i])?;
                    }
                }
            }
        }
        Ok(())
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
}

pub(crate) enum TempStore {
    Normal(NormalTempStore),
    MeetAggr(MeetAggrStore),
}

impl TempStore {
    // TODO
    // pub(crate) fn new() -> Self {
    //     Self::Normal(NormalTempStore::default())
    // }
    fn merge(&mut self, other: Self) -> Result<()> {
        match (self, other) {
            (TempStore::Normal(s), TempStore::Normal(o)) => {
                s.merge(o);
                Ok(())
            }
            (TempStore::MeetAggr(s), TempStore::MeetAggr(o)) => s.merge(o),
            _ => unreachable!(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            TempStore::Normal(n) => n.inner.is_empty(),
            TempStore::MeetAggr(m) => m.inner.is_empty(),
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
}

pub(crate) struct EpochStore {
    prev: TempStore,
    delta: TempStore,
}

impl EpochStore {
    pub(crate) fn merge(&mut self, mut new: TempStore) -> Result<()> {
        mem::swap(&mut new, &mut self.delta);
        self.prev.merge(new)
    }
    pub(crate) fn range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        self.delta
            .range_iter(lower, upper, upper_inclusive)
            .merge(self.prev.range_iter(lower, upper, upper_inclusive))
    }
    pub(crate) fn delta_range_iter(
        &self,
        lower: &Tuple,
        upper: &Tuple,
        upper_inclusive: bool,
    ) -> impl Iterator<Item = TupleInIter<'_>> {
        self.delta.range_iter(lower, upper, upper_inclusive)
    }
    pub(crate) fn prefix_iter(&self, prefix: &Tuple) -> impl Iterator<Item = TupleInIter<'_>> {
        let mut upper = prefix.to_vec();
        upper.push(DataValue::Bot);
        self.range_iter(prefix, &upper, true)
    }
    pub(crate) fn delta_prefix_iter(&self, prefix: &Tuple) -> impl Iterator<Item = TupleInIter<'_>> {
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
}

#[derive(Copy, Clone)]
pub(crate) struct TupleInIter<'a>(&'a Tuple, &'a Tuple, bool);

impl<'a> TupleInIter<'a> {
    pub(crate) fn get(self, idx: usize) -> &'a DataValue {
        self.0
            .get(idx)
            .unwrap_or_else(|| self.1.get(idx - self.0.len()).unwrap())
    }
    pub(crate) fn should_skip(&self) -> bool {
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
        self.into_iter().partial_cmp(other.into_iter())
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
