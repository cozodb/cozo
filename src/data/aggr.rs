use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use miette::{miette, bail, ensure, Result};

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

pub(crate) trait NormalAggrObj {
    fn set(&mut self, value: &DataValue) -> Result<()>;
    fn get(&self) -> Result<DataValue>;
}

pub(crate) trait MeetAggrObj {
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

struct AggrAnd {
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
        Ok(DataValue::Bool(self.accum))
    }
}

struct MeetAggrAnd;

impl MeetAggrObj for MeetAggrAnd {
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
struct AggrOr {
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
        Ok(DataValue::Bool(self.accum))
    }
}

struct MeetAggrOr;

impl MeetAggrObj for MeetAggrOr {
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
struct AggrUnique {
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
struct AggrGroupCount {
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
struct AggrCountUnique {
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
struct AggrUnion {
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

struct MeetAggrUnion;

impl MeetAggrObj for MeetAggrUnion {
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
struct AggrIntersection {
    accum: BTreeSet<DataValue>,
}

impl NormalAggrObj for AggrIntersection {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(v) => {
                for el in v.iter() {
                    self.accum.remove(el);
                }
            }
            v => bail!("cannot compute 'intersection' for value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(self.accum.iter().cloned().collect()))
    }
}

struct MeetAggrIntersection;

impl MeetAggrObj for MeetAggrIntersection {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        loop {
            if let DataValue::List(l) = left {
                let s = l.iter().cloned().collect();
                *left = DataValue::Set(s);
                continue;
            }
            return Ok(match (left, right) {
                (DataValue::Set(l), DataValue::Set(s)) => {
                    let mut removed = false;
                    for v in s.iter() {
                        removed |= l.remove(v);
                    }
                    removed
                }
                (DataValue::Set(l), DataValue::List(s)) => {
                    let mut removed = false;
                    for v in s.iter() {
                        removed |= l.remove(v);
                    }
                    removed
                }
                (_, v) => bail!("cannot compute 'union' for value {:?}", v),
            });
        }
    }
}

define_aggr!(AGGR_STR_JOIN, false);

#[derive(Default)]
struct AggrStrJoin {
    separator: Option<String>,
    accum: String,
}

impl AggrStrJoin {
    fn new(separator: String) -> Self {
        Self {
            separator: Some(separator),
            accum: "".to_string(),
        }
    }
}

impl NormalAggrObj for AggrStrJoin {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if let Some(sep) = &self.separator {
            if !self.accum.is_empty() {
                self.accum.push_str(sep)
            }
        }
        if let DataValue::Str(s) = value {
            self.accum.push_str(s);
            Ok(())
        } else {
            bail!("cannot apply 'str_join' to {:?}", value)
        }
    }

    fn get(&self) -> Result<DataValue> {
        todo!()
    }
}

define_aggr!(AGGR_COLLECT, false);

#[derive(Default)]
struct AggrCollect {
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

define_aggr!(AGGR_COUNT, false);

#[derive(Default)]
struct AggrCount {
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
struct AggrVariance {
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
struct AggrStdDev {
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
struct AggrMean {
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
struct AggrSum {
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

#[derive(Default)]
struct AggrProduct {
    product: f64,
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

struct AggrMin {
    found: DataValue,
}

impl Default for AggrMin {
    fn default() -> Self {
        Self {
            found: DataValue::Bot,
        }
    }
}

impl NormalAggrObj for AggrMin {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if *value < self.found {
            self.found = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

struct MeetAggrMin;

impl MeetAggrObj for MeetAggrMin {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(if *right < *left {
            *left = right.clone();
            true
        } else {
            false
        })
    }
}

define_aggr!(AGGR_MAX, true);

struct AggrMax {
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
        if *value > self.found {
            self.found = value.clone();
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

struct MeetAggrMax;

impl MeetAggrObj for MeetAggrMax {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(if *right > *left {
            *left = right.clone();
            true
        } else {
            false
        })
    }
}

define_aggr!(AGGR_CHOICE, true);

#[derive(Default)]
struct AggrChoice {
    found: Option<DataValue>,
}

impl NormalAggrObj for AggrChoice {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        if self.found.is_none() {
            self.found = Some(value.clone());
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        self.found.clone().ok_or_else(|| miette!("empty choice"))
    }
}

struct MeetAggrChoice;

impl MeetAggrObj for MeetAggrChoice {
    fn update(&self, _left: &mut DataValue, _right: &DataValue) -> Result<bool> {
        Ok(false)
    }
}

define_aggr!(AGGR_CHOICE_LAST, true);

struct AggrChoiceLast {
    found: DataValue,
}

impl Default for AggrChoiceLast {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrChoiceLast {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        self.found = value.clone();
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(self.found.clone())
    }
}

struct MeetAggrChoiceLast;

impl MeetAggrObj for MeetAggrChoiceLast {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(if *left == *right {
            false
        } else {
            *left = right.clone();
            true
        })
    }
}

define_aggr!(AGGR_MIN_COST, true);

struct AggrMinCost {
    found: DataValue,
    cost: DataValue,
}

impl Default for AggrMinCost {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
            cost: DataValue::Bot,
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
                if *c < self.cost {
                    self.cost = c.clone();
                    self.found = l[0].clone();
                }
                Ok(())
            }
            v => bail!("cannot compute 'min_cost' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(vec![self.found.clone(), self.cost.clone()]))
    }
}

struct MeetAggrMinCost;

impl MeetAggrObj for MeetAggrMinCost {
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
                let prev_cost = prev.get(1).unwrap();

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

define_aggr!(AGGR_MAX_COST, true);

struct AggrMaxCost {
    found: DataValue,
    cost: DataValue,
}

impl Default for AggrMaxCost {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
            cost: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrMaxCost {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::List(l) => {
                ensure!(
                    l.len() == 2,
                    "'max_cost' requires a list of exactly two items as argument"
                );
                let c = &l[1];
                if *c > self.cost {
                    self.cost = c.clone();
                    self.found = l[0].clone();
                }
                Ok(())
            }
            v => bail!("cannot compute 'max_cost' on {:?}", v),
        }
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::List(vec![self.found.clone(), self.cost.clone()]))
    }
}

struct MeetAggrMaxCost;

impl MeetAggrObj for MeetAggrMaxCost {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        Ok(match (left, right) {
            (DataValue::List(prev), DataValue::List(l)) => {
                ensure!(
                    l.len() == 2 && prev.len() == 2,
                    "'max_cost' requires a list of length 2 as argument, got {:?}, {:?}",
                    prev,
                    l
                );
                let cur_cost = l.get(1).unwrap();
                let prev_cost = prev.get(1).unwrap();

                if prev_cost >= cur_cost {
                    false
                } else {
                    *prev = l.clone();
                    true
                }
            }
            (u, v) => bail!("cannot compute 'max_cost' on {:?}, {:?}", u, v),
        })
    }
}

define_aggr!(AGGR_SHORTEST, true);

#[derive(Default)]
struct AggrShortest {
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

struct MeetAggrShortest;

impl MeetAggrObj for MeetAggrShortest {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
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

define_aggr!(AGGR_COALESCE, true);

struct AggrCoalesce {
    found: DataValue,
}

impl Default for AggrCoalesce {
    fn default() -> Self {
        Self {
            found: DataValue::Null,
        }
    }
}

impl NormalAggrObj for AggrCoalesce {
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

struct MeetAggrCoalesce;

impl MeetAggrObj for MeetAggrCoalesce {
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
struct AggrBitAnd {
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
        Ok(DataValue::Bytes(self.res.clone().into()))
    }
}

struct MeetAggrBitAnd;

impl MeetAggrObj for MeetAggrBitAnd {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bytes(left), DataValue::Bytes(right)) => {
                if left == right {
                    return Ok(false);
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
struct AggrBitOr {
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
        Ok(DataValue::Bytes(self.res.clone().into()))
    }
}

struct MeetAggrBitOr;

impl MeetAggrObj for MeetAggrBitOr {
    fn update(&self, left: &mut DataValue, right: &DataValue) -> Result<bool> {
        match (left, right) {
            (DataValue::Bytes(left), DataValue::Bytes(right)) => {
                if left == right {
                    return Ok(false);
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
struct AggrBitXor {
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
        Ok(DataValue::Bytes(self.res.clone().into()))
    }
}

pub(crate) fn parse_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "count" => &AGGR_COUNT,
        "group_count" => &AGGR_GROUP_COUNT,
        "count_unique" => &AGGR_COUNT_UNIQUE,
        "sum" => &AGGR_SUM,
        "min" => &AGGR_MIN,
        "max" => &AGGR_MAX,
        "mean" => &AGGR_MEAN,
        "choice" => &AGGR_CHOICE,
        "choice_last" => &AGGR_CHOICE_LAST,
        "collect" => &AGGR_COLLECT,
        "unique" => &AGGR_UNIQUE,
        "union" => &AGGR_UNION,
        "intersection" => &AGGR_INTERSECTION,
        "shortest" => &AGGR_SHORTEST,
        "min_cost" => &AGGR_MIN_COST,
        "coalesce" => &AGGR_COALESCE,
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
            name if name == AGGR_CHOICE_LAST.name => Box::new(MeetAggrChoiceLast),
            name if name == AGGR_BIT_AND.name => Box::new(MeetAggrBitAnd),
            name if name == AGGR_BIT_OR.name => Box::new(MeetAggrBitOr),
            name if name == AGGR_UNION.name => Box::new(MeetAggrUnion),
            name if name == AGGR_INTERSECTION.name => Box::new(MeetAggrIntersection),
            name if name == AGGR_SHORTEST.name => Box::new(MeetAggrShortest),
            name if name == AGGR_MIN_COST.name => Box::new(MeetAggrMinCost),
            name if name == AGGR_MAX_COST.name => Box::new(MeetAggrMaxCost),
            name if name == AGGR_COALESCE.name => Box::new(MeetAggrCoalesce),
            _ => unreachable!(),
        });
        Ok(())
    }
    pub(crate) fn normal_init(&mut self, args: &[DataValue]) -> Result<()> {
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
            name if name == AGGR_CHOICE_LAST.name => Box::new(AggrChoiceLast::default()),
            name if name == AGGR_BIT_AND.name => Box::new(AggrBitAnd::default()),
            name if name == AGGR_BIT_OR.name => Box::new(AggrBitOr::default()),
            name if name == AGGR_BIT_XOR.name => Box::new(AggrBitXor::default()),
            name if name == AGGR_UNIQUE.name => Box::new(AggrUnique::default()),
            name if name == AGGR_UNION.name => Box::new(AggrUnion::default()),
            name if name == AGGR_INTERSECTION.name => Box::new(AggrIntersection::default()),
            name if name == AGGR_SHORTEST.name => Box::new(AggrShortest::default()),
            name if name == AGGR_MIN_COST.name => Box::new(AggrMinCost::default()),
            name if name == AGGR_MAX_COST.name => Box::new(AggrMaxCost::default()),
            name if name == AGGR_COALESCE.name => Box::new(AggrCoalesce::default()),
            name if name == AGGR_STR_JOIN.name => Box::new({
                if args.is_empty() {
                    AggrStrJoin::default()
                } else {
                    let arg = args[0].get_string().ok_or_else(|| {
                        miette!(
                            "the argument to 'str_join' must be a string, got {:?}",
                            args[0]
                        )
                    })?;
                    AggrStrJoin::new(arg.to_string())
                }
            }),
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
