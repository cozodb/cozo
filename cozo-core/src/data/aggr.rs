/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use miette::{bail, ensure, miette, Result};
use rand::prelude::*;

use crate::data::value::DataValue;

pub(crate) struct Aggregation {
    pub(crate) name: &'static str,
    pub(crate) is_meet: bool,
    pub(crate) meet_op: Option<Box<dyn MeetAggrObj>>,
    pub(crate) normal_op: Option<Box<dyn NormalAggrObj>>,
}

impl Clone for Aggregation {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            is_meet: self.is_meet,
            meet_op: None,
            normal_op: None,
        }
    }
}

pub(crate) trait NormalAggrObj: Send + Sync {
    fn set(&mut self, value: &DataValue) -> Result<()>;
    fn get(&self) -> Result<DataValue>;
}

pub(crate) trait MeetAggrObj: Send + Sync {
    fn init_val(&self) -> DataValue;
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool>;
}

impl PartialEq for Aggregation {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Debug for Aggregation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggr<{}>", self.name)
    }
}

macro_rules! define_aggr {
    ($name:ident, $is_meet:expr) => {
        const $name: Aggregation = Aggregation {
            name: stringify!($name),
            is_meet: $is_meet,
            meet_op: None,
            normal_op: None,
        };
    };
}

define_aggr!(AGGR_AND, true);

pub(crate) struct AggrAnd {
    accum: bool,
}

impl Default for AggrAnd {
    fn default() -> Self {
        Self { accum: true }
    }
}

impl NormalAggrObj for AggrAnd {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Bool(v) => self.accum &= *v,
            v => bail!("cannot compute 'and' for {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.accum))
    }
}

pub(crate) struct MeetAggrAnd;

impl MeetAggrObj for MeetAggrAnd {
    fn init_val(&self) -> DataValue {
        DataValue::from(true)
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bool(l), DataValue::Bool(r)) => {
                let old = *l;
                *l &= *r;
                Ok(old == *l)
            }
            (u, v) => bail!("cannot compute 'and' for {:?} and {:?}", u, v),
        }
    }
}

define_aggr!(AGGR_OR, true);

#[derive(Default)]
pub(crate) struct AggrOr {
    accum: bool,
}

impl NormalAggrObj for AggrOr {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Bool(v) => self.accum |= *v,
            v => bail!("cannot compute 'or' for {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.accum))
    }
}

pub(crate) struct MeetAggrOr;

impl MeetAggrObj for MeetAggrOr {
    fn init_val(&self) -> DataValue {
        DataValue::from(false)
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bool(l), DataValue::Bool(r)) => {
                let old = *l;
                *l |= *r;
                Ok(old == *l)
            }
            (u, v) => bail!("cannot compute 'or' for {:?} and {:?}", u, v),
        }
    }
}

define_aggr!(AGGR_UNIQUE, false);

#[derive(Default)]
pub(crate) struct AggrUnique {
    accum: BTreeSet<DataValue>,
}

impl NormalAggrObj for AggrUnique {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        self.accum.insert(value.clone());
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(self.accum.iter().cloned().collect()))
    }
}

define_aggr!(AGGR_GROUP_COUNT, false);

#[derive(Default)]
pub(crate) struct AggrGroupCount {
    accum: BTreeMap<DataValue, i64>,
}

impl NormalAggrObj for AggrGroupCount {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        let entry = self.accum.entry(value.clone()).or_default();
        *entry += 1;
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(
            self.accum
                .iter()
                .map(|(k, v)| DataValue::List(vec![k.clone(), DataValue::from(*v)]))
                .collect(),
        ))
    }
}

define_aggr!(AGGR_COUNT_UNIQUE, false);

#[derive(Default)]
pub(crate) struct AggrCountUnique {
    count: i64,
    accum: BTreeSet<DataValue>,
}

impl NormalAggrObj for AggrCountUnique {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if !self.accum.contains(value) {
            self.accum.insert(value.clone());
            self.count += 1;
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.count))
    }
}

define_aggr!(AGGR_UNION, true);

#[derive(Default)]
pub(crate) struct AggrUnion {
    accum: BTreeSet<DataValue>,
}

impl NormalAggrObj for AggrUnion {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(v) => self.accum.extend(v.iter().cloned()),
            v => bail!("cannot compute 'union' for value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(self.accum.iter().cloned().collect()))
    }
}

pub(crate) struct MeetAggrUnion;

impl MeetAggrObj for MeetAggrUnion {
    fn init_val(&self) -> DataValue {
        DataValue::Set(BTreeSet::new())
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        loop {
            if let DataValue::List(l) = left {
                let s = l.iter().cloned().collect();
                *left = DataValue::Set(s);
                continue;
            }
            return Ok(match (left, right) {
                (DataValue::Set(l), DataValue::Set(s)) => {
                    let mut inserted = false;
                    for v in s.iter() {
                        inserted |= l.insert(v.clone());
                    }
                    inserted
                }
                (DataValue::Set(l), DataValue::List(s)) => {
                    let mut inserted = false;
                    for v in s.iter() {
                        inserted |= l.insert(v.clone());
                    }
                    inserted
                }
                (_, v) => bail!("cannot compute 'union' for value {:?}", v),
            });
        }
    }
}

define_aggr!(AGGR_INTERSECTION, true);

#[derive(Default)]
pub(crate) struct AggrIntersection {
    accum: Option<BTreeSet<DataValue>>,
}

impl NormalAggrObj for AggrIntersection {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(v) => {
                if let Some(accum) = &mut self.accum {
                    let new = accum
                        .intersection(&v.iter().cloned().collect())
                        .cloned()
                        .collect();
                    *accum = new;
                } else {
                    self.accum = Some(v.iter().cloned().collect())
                }
            }
            v => bail!("cannot compute 'intersection' for value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        match &self.accum {
            None => Ok(DataValue::List(vec![])),
            Some(l) => Ok(DataValue::List(l.iter().cloned().collect())),
        }
    }
}

pub(crate) struct MeetAggrIntersection;

impl MeetAggrObj for MeetAggrIntersection {
    fn init_val(&self) -> DataValue {
        DataValue::Null
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        if *left == DataValue::Null && *right != DataValue::Null {
            *left = right.clone();
            return Ok(true);
        } else if *right == DataValue::Null {
            return Ok(false);
        }
        loop {
            if let DataValue::List(l) = left {
                let s = l.iter().cloned().collect();
                *left = DataValue::Set(s);
                continue;
            }
            return Ok(match (left, right) {
                (DataValue::Set(l), DataValue::Set(s)) => {
                    let old_len = l.len();
                    let new_set = l.intersection(s).cloned().collect::<BTreeSet<_>>();
                    if old_len == new_set.len() {
                        false
                    } else {
                        *l = new_set;
                        true
                    }
                }
                (DataValue::Set(l), DataValue::List(s)) => {
                    let old_len = l.len();
                    let s: BTreeSet<_> = s.iter().cloned().collect();
                    let new_set = l.intersection(&s).cloned().collect::<BTreeSet<_>>();
                    if old_len == new_set.len() {
                        false
                    } else {
                        *l = new_set;
                        true
                    }
                }
                (_, v) => bail!("cannot compute 'union' for value {:?}", v),
            });
        }
    }
}

define_aggr!(AGGR_COLLECT, false);

#[derive(Default)]
pub(crate) struct AggrCollect {
    limit: Option<usize>,
    accum: Vec<DataValue>,
}

impl AggrCollect {
    fn new(limit: usize) -> Self {
        Self {
            limit: Some(limit),
            accum: vec![],
        }
    }
}

impl NormalAggrObj for AggrCollect {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if let Some(limit) = self.limit {
            if self.accum.len() >= limit {
                return Ok(());
            }
        }
        self.accum.push(value.clone());
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(self.accum.clone()))
    }
}

define_aggr!(AGGR_CHOICE_RAND, false);

pub(crate) struct AggrChoiceRand {
    count: usize,
    value: DataValue,
}

impl Default for AggrChoiceRand {
    fn default() -> Self {
        Self {
            count: 0,
            value: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrChoiceRand {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        self.count += 1;
        let prob = 1. / (self.count as f64);
        let rd = thread_rng().gen::<f64>();
        if rd < prob {
            self.value = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.value.clone())
    }
}

define_aggr!(AGGR_COUNT, false);

#[derive(Default)]
pub(crate) struct AggrCount {
    count: i64,
}

impl NormalAggrObj for AggrCount {
    fn set(&mut self, _value: &DataValue) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.count))
    }
}

define_aggr!(AGGR_VARIANCE, false);

#[derive(Default)]
pub(crate) struct AggrVariance {
    count: i64,
    sum: f64,
    sum_sq: f64,
}

impl NormalAggrObj for AggrVariance {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Num(n) => {
                let f = n.get_float();
                self.sum += f;
                self.sum_sq += f * f;
                self.count += 1;
            }
            v => bail!("cannot compute 'variance': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        let ct = self.count as f64;
        Ok(DataValue::from(
            (self.sum_sq - self.sum * self.sum / ct) / (ct - 1.),
        ))
    }
}

define_aggr!(AGGR_STD_DEV, false);

#[derive(Default)]
pub(crate) struct AggrStdDev {
    count: i64,
    sum: f64,
    sum_sq: f64,
}

impl NormalAggrObj for AggrStdDev {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Num(n) => {
                let f = n.get_float();
                self.sum += f;
                self.sum_sq += f * f;
                self.count += 1;
            }
            v => bail!("cannot compute 'std_dev': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        let ct = self.count as f64;
        let var = (self.sum_sq - self.sum * self.sum / ct) / (ct - 1.);
        Ok(DataValue::from(var.sqrt()))
    }
}

define_aggr!(AGGR_MEAN, false);

#[derive(Default)]
pub(crate) struct AggrMean {
    count: i64,
    sum: f64,
}

impl NormalAggrObj for AggrMean {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Num(n) => {
                self.sum += n.get_float();
                self.count += 1;
            }
            v => bail!("cannot compute 'mean': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.sum / (self.count as f64)))
    }
}

define_aggr!(AGGR_SUM, false);

#[derive(Default)]
pub(crate) struct AggrSum {
    sum: f64,
}

impl NormalAggrObj for AggrSum {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Num(n) => {
                self.sum += n.get_float();
            }
            v => bail!("cannot compute 'sum': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.sum))
    }
}

define_aggr!(AGGR_PRODUCT, false);

pub(crate) struct AggrProduct {
    product: f64,
}

impl Default for AggrProduct {
    fn default() -> Self {
        Self { product: 1.0 }
    }
}

impl NormalAggrObj for AggrProduct {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Num(n) => {
                self.product *= n.get_float();
            }
            v => bail!("cannot compute 'product': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.product))
    }
}

define_aggr!(AGGR_MIN, true);

pub(crate) struct AggrMin {
    found: DataValue,
}

impl Default for AggrMin {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrMin {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if *value == DataValue::Null {
            return Ok(());
        }
        if self.found == DataValue::Null {
            self.found = value.clone();
            return Ok(());
        }
        let f1 = self
            .found
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        let f2 = value
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        if f1 > f2 {
            self.found = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

pub(crate) struct MeetAggrMin;

impl MeetAggrObj for MeetAggrMin {
    fn init_val(&self) -> DataValue {
        DataValue::Null
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        if *right == DataValue::Null {
            return Ok(false);
        }
        if *left == DataValue::Null {
            *left = right.clone();
            return Ok(true);
        }
        let f1 = left
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        let f2 = right
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;

        Ok(if f1 > f2 {
            *left = right.clone();
            true
        } else {
            false
        })
    }
}

define_aggr!(AGGR_MAX, true);

pub(crate) struct AggrMax {
    found: DataValue,
}

impl Default for AggrMax {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrMax {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if *value == DataValue::Null {
            return Ok(());
        }
        if self.found == DataValue::Null {
            self.found = value.clone();
            return Ok(());
        }
        let f1 = self
            .found
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        let f2 = value
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        if f1 < f2 {
            self.found = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

pub(crate) struct MeetAggrMax;

impl MeetAggrObj for MeetAggrMax {
    fn init_val(&self) -> DataValue {
        DataValue::Null
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        if *right == DataValue::Null {
            return Ok(false);
        }
        if *left == DataValue::Null {
            *left = right.clone();
            return Ok(true);
        }
        let f1 = left
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;
        let f2 = right
            .get_float()
            .ok_or_else(|| miette!("'min' applied to non-numerical values"))?;

        Ok(if f1 < f2 {
            *left = right.clone();
            true
        } else {
            false
        })
    }
}

define_aggr!(AGGR_LATEST_BY, false);

pub(crate) struct AggrLatestBy {
    found: DataValue,
    cost: DataValue,
}

impl Default for AggrLatestBy {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
            cost: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrLatestBy {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(l) => {
                ensure!(
                    l.len() == 2,
                    "'latest_by' requires a list of exactly two items as argument"
                );
                let c = &l[1];
                if *c > self.cost {
                    self.cost = c.clone();
                    self.found = l[0].clone();
                }
                Ok(())
            }
            v => bail!("cannot compute 'latest_by' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

define_aggr!(AGGR_SMALLEST_BY, false);

pub(crate) struct AggrSmallestBy {
    found: DataValue,
    cost: DataValue,
}

impl Default for AggrSmallestBy {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
            cost: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrSmallestBy {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(l) => {
                ensure!(
                    l.len() == 2,
                    "'smallest_by' requires a list of exactly two items as argument"
                );
                let c = &l[1];
                if self.cost == DataValue::Null || *c < self.cost {
                    self.cost = c.clone();
                    self.found = l[0].clone();
                }
                Ok(())
            }
            v => bail!("cannot compute 'smallest_by' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

define_aggr!(AGGR_MIN_COST, true);

pub(crate) struct AggrMinCost {
    found: DataValue,
    cost: f64,
}

impl Default for AggrMinCost {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
            cost: f64::INFINITY,
        }
    }
}

impl NormalAggrObj for AggrMinCost {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(l) => {
                ensure!(
                    l.len() == 2,
                    "'min_cost' requires a list of exactly two items as argument"
                );
                let c = &l[1];
                let cost = c
                    .get_float()
                    .ok_or_else(|| miette!("Cost must be numeric"))?;
                if cost < self.cost {
                    self.cost = cost;
                    self.found = l[0].clone();
                }
                Ok(())
            }
            v => bail!("cannot compute 'min_cost' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(vec![
            self.found.clone(),
            DataValue::from(self.cost),
        ]))
    }
}

pub(crate) struct MeetAggrMinCost;

impl MeetAggrObj for MeetAggrMinCost {
    fn init_val(&self) -> DataValue {
        DataValue::List(vec![DataValue::Null, DataValue::from(f64::INFINITY)])
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(match (left, right) {
            (DataValue::List(prev), DataValue::List(l)) => {
                ensure!(
                    l.len() == 2 && prev.len() == 2,
                    "'min_cost' requires a list of length 2 as argument, got {:?}, {:?}",
                    prev,
                    l
                );
                let cur_cost = l.get(1).unwrap();
                let cur_cost = cur_cost
                    .get_float()
                    .ok_or_else(|| miette!("'min_cost' must have numerical costs"))?;
                let prev_cost = prev.get(1).unwrap();
                let prev_cost = prev_cost
                    .get_float()
                    .ok_or_else(|| miette!("'prev_cost' must have numerical costs"))?;

                if prev_cost <= cur_cost {
                    false
                } else {
                    *prev = l.clone();
                    true
                }
            }
            (u, v) => bail!("cannot compute 'min_cost' on {:?}, {:?}", u, v),
        })
    }
}

define_aggr!(AGGR_SHORTEST, true);

#[derive(Default)]
pub(crate) struct AggrShortest {
    found: Option<Vec<DataValue>>,
}

impl NormalAggrObj for AggrShortest {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(l) => {
                match self.found {
                    None => self.found = Some(l.clone()),
                    Some(ref mut found) => {
                        if l.len() < found.len() {
                            *found = l.clone();
                        }
                    }
                }
                Ok(())
            }
            v => bail!("cannot compute 'shortest' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(match self.found {
            None => DataValue::Null,
            Some(ref l) => DataValue::List(l.clone()),
        })
    }
}

pub(crate) struct MeetAggrShortest;

impl MeetAggrObj for MeetAggrShortest {
    fn init_val(&self) -> DataValue {
        DataValue::Null
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        if *left == DataValue::Null && *right != DataValue::Null {
            *left = right.clone();
            return Ok(true);
        } else if *right == DataValue::Null {
            return Ok(false);
        }
        match (left, right) {
            (DataValue::List(l), DataValue::List(r)) => Ok(if r.len() < l.len() {
                *l = r.clone();
                true
            } else {
                false
            }),
            (l, v) => bail!("cannot compute 'shortest' on {:?} and {:?}", l, v),
        }
    }
}

define_aggr!(AGGR_CHOICE, true);

pub(crate) struct AggrChoice {
    found: DataValue,
}

impl Default for AggrChoice {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrChoice {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if self.found == DataValue::Null {
            self.found = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

pub(crate) struct MeetAggrChoice;

impl MeetAggrObj for MeetAggrChoice {
    fn init_val(&self) -> DataValue {
        DataValue::Null
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(if *left == DataValue::Null && *right != DataValue::Null {
            *left = right.clone();
            true
        } else {
            false
        })
    }
}

define_aggr!(AGGR_BIT_AND, true);

#[derive(Default)]
pub(crate) struct AggrBitAnd {
    res: Vec<u8>,
}

impl NormalAggrObj for AggrBitAnd {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Bytes(bs) => {
                if self.res.is_empty() {
                    self.res = bs.to_vec();
                } else {
                    ensure!(
                        self.res.len() == bs.len(),
                        "operands of 'bit_and' must have the same lengths, got {:x?} and {:x?}",
                        self.res,
                        bs
                    );
                    for (l, r) in self.res.iter_mut().zip(bs.iter()) {
                        *l &= *r;
                    }
                }
                Ok(())
            }
            v => bail!("cannot apply 'bit_and' to {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::Bytes(self.res.clone()))
    }
}

pub(crate) struct MeetAggrBitAnd;

impl MeetAggrObj for MeetAggrBitAnd {
    fn init_val(&self) -> DataValue {
        DataValue::Bytes(vec![])
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bytes(left), DataValue::Bytes(right)) => {
                if left == right {
                    return Ok(false);
                }
                if left.is_empty() {
                    *left = right.clone();
                    return Ok(true);
                }
                ensure!(
                    left.len() == right.len(),
                    "operands of 'bit_and' must have the same lengths, got {:x?} and {:x?}",
                    left,
                    right
                );
                for (l, r) in left.iter_mut().zip(right.iter()) {
                    *l &= *r;
                }

                Ok(true)
            }
            v => bail!("cannot apply 'bit_and' to {:?}", v),
        }
    }
}

define_aggr!(AGGR_BIT_OR, true);

#[derive(Default)]
pub(crate) struct AggrBitOr {
    res: Vec<u8>,
}

impl NormalAggrObj for AggrBitOr {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Bytes(bs) => {
                if self.res.is_empty() {
                    self.res = bs.to_vec();
                } else {
                    ensure!(
                        self.res.len() == bs.len(),
                        "operands of 'bit_or' must have the same lengths, got {:x?} and {:x?}",
                        self.res,
                        bs
                    );
                    for (l, r) in self.res.iter_mut().zip(bs.iter()) {
                        *l |= *r;
                    }
                }
                Ok(())
            }
            v => bail!("cannot apply 'bit_or' to {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::Bytes(self.res.clone()))
    }
}

pub(crate) struct MeetAggrBitOr;

impl MeetAggrObj for MeetAggrBitOr {
    fn init_val(&self) -> DataValue {
        DataValue::Bytes(vec![])
    }

    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bytes(left), DataValue::Bytes(right)) => {
                if left == right {
                    return Ok(false);
                }
                if left.is_empty() {
                    *left = right.clone();
                    return Ok(true);
                }
                ensure!(
                    left.len() == right.len(),
                    "operands of 'bit_or' must have the same lengths, got {:x?} and {:x?}",
                    left,
                    right
                );
                for (l, r) in left.iter_mut().zip(right.iter()) {
                    *l |= *r;
                }

                Ok(true)
            }
            v => bail!("cannot apply 'bit_or' to {:?}", v),
        }
    }
}

define_aggr!(AGGR_BIT_XOR, false);

#[derive(Default)]
pub(crate) struct AggrBitXor {
    res: Vec<u8>,
}

impl NormalAggrObj for AggrBitXor {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Bytes(bs) => {
                if self.res.is_empty() {
                    self.res = bs.to_vec();
                } else {
                    ensure!(
                        self.res.len() == bs.len(),
                        "operands of 'bit_xor' must have the same lengths, got {:x?} and {:x?}",
                        self.res,
                        bs
                    );
                    for (l, r) in self.res.iter_mut().zip(bs.iter()) {
                        *l ^= *r;
                    }
                }
                Ok(())
            }
            v => bail!("cannot apply 'bit_xor' to {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::Bytes(self.res.clone()))
    }
}

pub(crate) fn parse_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "and" => &AGGR_AND,
        "or" => &AGGR_OR,
        "unique" => &AGGR_UNIQUE,
        "group_count" => &AGGR_GROUP_COUNT,
        "union" => &AGGR_UNION,
        "intersection" => &AGGR_INTERSECTION,
        "count" => &AGGR_COUNT,
        "count_unique" => &AGGR_COUNT_UNIQUE,
        "variance" => &AGGR_VARIANCE,
        "std_dev" => &AGGR_STD_DEV,
        "sum" => &AGGR_SUM,
        "product" => &AGGR_PRODUCT,
        "min" => &AGGR_MIN,
        "max" => &AGGR_MAX,
        "mean" => &AGGR_MEAN,
        "choice" => &AGGR_CHOICE,
        "collect" => &AGGR_COLLECT,
        "shortest" => &AGGR_SHORTEST,
        "min_cost" => &AGGR_MIN_COST,
        "bit_and" => &AGGR_BIT_AND,
        "bit_or" => &AGGR_BIT_OR,
        "bit_xor" => &AGGR_BIT_XOR,
        "latest_by" => &AGGR_LATEST_BY,
        "smallest_by" => &AGGR_SMALLEST_BY,
        "choice_rand" => &AGGR_CHOICE_RAND,
        _ => return None,
    })
}

impl Aggregation {
    pub(crate) fn meet_init(&mut self, _args: &[DataValue]) -> Result<()> {
        self.meet_op.replace(match self.name {
            name if name == AGGR_AND.name => Box::new(MeetAggrAnd),
            name if name == AGGR_OR.name => Box::new(MeetAggrOr),
            name if name == AGGR_MIN.name => Box::new(MeetAggrMin),
            name if name == AGGR_MAX.name => Box::new(MeetAggrMax),
            name if name == AGGR_CHOICE.name => Box::new(MeetAggrChoice),
            name if name == AGGR_BIT_AND.name => Box::new(MeetAggrBitAnd),
            name if name == AGGR_BIT_OR.name => Box::new(MeetAggrBitOr),
            name if name == AGGR_UNION.name => Box::new(MeetAggrUnion),
            name if name == AGGR_INTERSECTION.name => Box::new(MeetAggrIntersection),
            name if name == AGGR_SHORTEST.name => Box::new(MeetAggrShortest),
            name if name == AGGR_MIN_COST.name => Box::new(MeetAggrMinCost),
            name => unreachable!("{}", name),
        });
        Ok(())
    }
    pub(crate) fn normal_init(&mut self, args: &[DataValue]) -> Result<()> {
        #[allow(clippy::box_default)]
        self.normal_op.replace(match self.name {
            name if name == AGGR_AND.name => Box::new(AggrAnd::default()),
            name if name == AGGR_OR.name => Box::new(AggrOr::default()),
            name if name == AGGR_COUNT.name => Box::new(AggrCount::default()),
            name if name == AGGR_GROUP_COUNT.name => Box::new(AggrGroupCount::default()),
            name if name == AGGR_COUNT_UNIQUE.name => Box::new(AggrCountUnique::default()),
            name if name == AGGR_SUM.name => Box::new(AggrSum::default()),
            name if name == AGGR_PRODUCT.name => Box::new(AggrProduct::default()),
            name if name == AGGR_MIN.name => Box::new(AggrMin::default()),
            name if name == AGGR_MAX.name => Box::new(AggrMax::default()),
            name if name == AGGR_MEAN.name => Box::new(AggrMean::default()),
            name if name == AGGR_VARIANCE.name => Box::new(AggrVariance::default()),
            name if name == AGGR_STD_DEV.name => Box::new(AggrStdDev::default()),
            name if name == AGGR_CHOICE.name => Box::new(AggrChoice::default()),
            name if name == AGGR_BIT_AND.name => Box::new(AggrBitAnd::default()),
            name if name == AGGR_BIT_OR.name => Box::new(AggrBitOr::default()),
            name if name == AGGR_BIT_XOR.name => Box::new(AggrBitXor::default()),
            name if name == AGGR_UNIQUE.name => Box::new(AggrUnique::default()),
            name if name == AGGR_UNION.name => Box::new(AggrUnion::default()),
            name if name == AGGR_INTERSECTION.name => Box::new(AggrIntersection::default()),
            name if name == AGGR_SHORTEST.name => Box::new(AggrShortest::default()),
            name if name == AGGR_MIN_COST.name => Box::new(AggrMinCost::default()),
            name if name == AGGR_LATEST_BY.name => Box::new(AggrLatestBy::default()),
            name if name == AGGR_SMALLEST_BY.name => Box::new(AggrSmallestBy::default()),
            name if name == AGGR_CHOICE_RAND.name => Box::new(AggrChoiceRand::default()),
            name if name == AGGR_COLLECT.name => Box::new({
                if args.is_empty() {
                    AggrCollect::default()
                } else {
                    let arg = args[0].get_int().ok_or_else(|| {
                        miette!(
                            "the argument to 'collect' must be an integer, got {:?}",
                            args[0]
                        )
                    })?;
                    ensure!(
                        arg > 0,
                        "argument to 'collect' must be positive, got {}",
                        arg
                    );
                    AggrCollect::new(arg as usize)
                }
            }),
            _ => unreachable!(),
        });
        Ok(())
    }
}
