use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};

use anyhow::{anyhow, bail, ensure, Result};

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

define_aggr!(AGGR_MEAN, false);

#[derive(Default)]
struct AggrMean {
    count: i64,
    sum: f64,
}

impl NormalAggrObj for AggrMean {
    fn set(&mut self, value: &DataValue) -> Result<()> {
        match value {
            DataValue::Number(n) => {
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
            DataValue::Number(n) => {
                self.sum += n.get_float();
            }
            v => bail!("cannot compute 'mean': encountered value {:?}", v),
        }
        Ok(())
    }

    fn get(&self) -> Result<DataValue> {
        Ok(DataValue::from(self.sum))
    }
}

define_aggr!(AGGR_MIN, true);

struct AggrMin {
    found: DataValue,
}

impl Default for AggrMin {
    fn default() -> Self {
        Self {
            found: DataValue::Bottom,
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
        Ok(self.found.clone().ok_or_else(|| anyhow!("empty choice"))?)
    }
}

struct MeetAggrChoice;

impl MeetAggrObj for MeetAggrChoice {
    fn update(&self, _left: &mut DataValue, _right: &DataValue) -> Result<bool> {
        Ok(false)
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
            cost: DataValue::Bottom,
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

pub(crate) fn get_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "count" => &AGGR_COUNT,
        "group_count" => &AGGR_GROUP_COUNT,
        "count_unique" => &AGGR_COUNT_UNIQUE,
        "sum" => &AGGR_SUM,
        "min" => &AGGR_MIN,
        "max" => &AGGR_MAX,
        "mean" => &AGGR_MEAN,
        "choice" => &AGGR_CHOICE,
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
            name if name == AGGR_MIN.name => Box::new(MeetAggrMin),
            name if name == AGGR_MAX.name => Box::new(MeetAggrMax),
            name if name == AGGR_CHOICE.name => Box::new(MeetAggrChoice),
            name if name == AGGR_UNION.name => Box::new(MeetAggrUnion),
            name if name == AGGR_INTERSECTION.name => Box::new(MeetAggrIntersection),
            name if name == AGGR_SHORTEST.name => Box::new(MeetAggrShortest),
            name if name == AGGR_MIN_COST.name => Box::new(MeetAggrMinCost),
            name if name == AGGR_COALESCE.name => Box::new(MeetAggrCoalesce),
            _ => unreachable!(),
        });
        Ok(())
    }
    pub(crate) fn normal_init(&mut self, args: &[DataValue]) -> Result<()> {
        self.normal_op.replace(match self.name {
            name if name == AGGR_COUNT.name => Box::new(AggrCount::default()),
            name if name == AGGR_GROUP_COUNT.name => Box::new(AggrGroupCount::default()),
            name if name == AGGR_COUNT_UNIQUE.name => Box::new(AggrCountUnique::default()),
            name if name == AGGR_SUM.name => Box::new(AggrSum::default()),
            name if name == AGGR_MIN.name => Box::new(AggrMin::default()),
            name if name == AGGR_MAX.name => Box::new(AggrMax::default()),
            name if name == AGGR_MEAN.name => Box::new(AggrMean::default()),
            name if name == AGGR_CHOICE.name => Box::new(AggrChoice::default()),
            name if name == AGGR_UNIQUE.name => Box::new(AggrUnique::default()),
            name if name == AGGR_UNION.name => Box::new(AggrUnion::default()),
            name if name == AGGR_INTERSECTION.name => Box::new(AggrIntersection::default()),
            name if name == AGGR_SHORTEST.name => Box::new(AggrShortest::default()),
            name if name == AGGR_MIN_COST.name => Box::new(AggrMinCost::default()),
            name if name == AGGR_COALESCE.name => Box::new(AggrCoalesce::default()),
            name if name == AGGR_COLLECT.name => Box::new({
                if args.len() == 0 {
                    AggrCollect::default()
                } else {
                    let arg = args[0].get_int().ok_or_else(|| {
                        anyhow!(
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
