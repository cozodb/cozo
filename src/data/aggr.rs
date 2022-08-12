use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::ops::Sub;

use anyhow::{anyhow, bail, ensure, Result};
use itertools::Itertools;

use crate::data::value::{DataValue, Number};

#[derive(Clone)]
pub(crate) struct Aggregation {
    pub(crate) name: &'static str,
    pub(crate) combine: fn(&mut DataValue, &DataValue, &[DataValue]) -> Result<bool>,
    pub(crate) is_meet: bool,
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
            combine: ::casey::lower!($name),
            is_meet: $is_meet,
        };
    };
}

define_aggr!(AGGR_UNIQUE, false);
fn aggr_unique(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Set(Default::default());
            true
        }
        (accum @ DataValue::Guard, val) => {
            *accum = DataValue::Set(BTreeSet::from([val.clone()]));
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::Set(l), val) => l.insert(val.clone()),
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_GROUP_COUNT, false);
fn aggr_group_count(
    accum: &mut DataValue,
    current: &DataValue,
    _args: &[DataValue],
) -> Result<bool> {
    dbg!(&current);
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::List(vec![]);
            true
        }
        (accum @ DataValue::Guard, val) => {
            *accum = DataValue::Map(BTreeMap::from([(val.clone(), DataValue::from(1))]));
            true
        }
        (accum, DataValue::Guard) => {
            *accum = DataValue::List(
                accum
                    .get_map()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| DataValue::List(vec![k.clone(), v.clone()]))
                    .collect_vec(),
            );
            true
        }
        (DataValue::Map(l), val) => {
            let entry = l.entry(val.clone()).or_insert_with(|| DataValue::from(0));
            *entry = DataValue::from(entry.get_int().unwrap() + 1);
            true
        }
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_COUNT_UNIQUE, false);
fn aggr_count_unique(
    accum: &mut DataValue,
    current: &DataValue,
    _args: &[DataValue],
) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::from(0);
            true
        }
        (accum @ DataValue::Guard, val) => {
            *accum = DataValue::Set(BTreeSet::from([val.clone()]));
            true
        }
        (accum, DataValue::Guard) => {
            *accum = DataValue::from(accum.get_set().unwrap().len() as i64);
            true
        }
        (DataValue::Set(l), val) => l.insert(val.clone()),
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_UNION, true);
fn aggr_union(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Set(Default::default());
            true
        }
        (accum @ DataValue::Guard, DataValue::Set(s)) => {
            *accum = DataValue::Set(s.clone());
            true
        }
        (accum @ DataValue::Guard, DataValue::List(s)) => {
            *accum = DataValue::Set(s.iter().cloned().collect());
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::Set(l), DataValue::Set(s)) => {
            if s.is_subset(l) {
                false
            } else {
                l.extend(s.iter().cloned());
                true
            }
        }
        (DataValue::Set(l), DataValue::List(s)) => {
            let s: BTreeSet<_> = s.iter().cloned().collect();
            if s.is_subset(l) {
                false
            } else {
                l.extend(s);
                true
            }
        }
        (_, v) => bail!("cannot compute 'union' for value {:?}", v),
    })
}

define_aggr!(AGGR_INTERSECTION, true);
fn aggr_intersection(
    accum: &mut DataValue,
    current: &DataValue,
    _args: &[DataValue],
) -> Result<bool> {
    Ok(match (accum, current) {
        (DataValue::Guard, DataValue::Guard) => false,
        (accum @ DataValue::Guard, DataValue::Set(s)) => {
            *accum = DataValue::Set(s.clone());
            true
        }
        (accum @ DataValue::Guard, DataValue::List(s)) => {
            *accum = DataValue::Set(s.iter().cloned().collect());
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::Set(l), DataValue::Set(s)) => {
            if l.is_empty() || l.is_subset(s) {
                false
            } else {
                *l = l.sub(s);
                true
            }
        }
        (DataValue::Set(l), DataValue::List(s)) => {
            if l.is_empty() {
                false
            } else {
                let s: BTreeSet<_> = s.iter().cloned().collect();
                if l.is_subset(&s) {
                    false
                } else {
                    *l = l.sub(&s);
                    true
                }
            }
        }
        (_, v) => bail!("cannot compute 'intersection' for value {:?}", v),
    })
}

define_aggr!(AGGR_COLLECT, false);
fn aggr_collect(accum: &mut DataValue, current: &DataValue, args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            if let Some(limit) = args.get(0) {
                let limit = limit
                    .get_int()
                    .ok_or_else(|| anyhow!("collect limit must be an integer"))?;
                ensure!(limit > 0, "collect limit must be positive, got {}", limit);
            }
            *accum = DataValue::List(vec![]);
            true
        }
        (accum @ DataValue::Guard, val) => {
            if let Some(limit) = args.get(0) {
                let limit = limit
                    .get_int()
                    .ok_or_else(|| anyhow!("collect limit must be an integer"))?;
                ensure!(limit > 0, "collect limit must be positive, got {}", limit);
            }
            *accum = DataValue::List(vec![val.clone()]);
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::List(l), val) => {
            if let Some(limit) = args.get(0).and_then(|v| v.get_int()) {
                if l.len() >= (limit as usize) {
                    return Ok(false);
                }
            }
            l.push(val.clone());
            true
        }
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_COUNT, false);
fn aggr_count(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Number(Number::Int(0));
            true
        }
        (accum @ DataValue::Guard, _) => {
            *accum = DataValue::Number(Number::Int(1));
            true
        }
        (DataValue::Number(Number::Int(_)), DataValue::Guard) => false,
        (DataValue::Number(Number::Int(i)), _) => {
            *i += 1;
            true
        }
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_MEAN, false);
fn aggr_mean(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::from(0.);
            true
        }
        (accum @ DataValue::Guard, DataValue::Number(n)) => {
            *accum = DataValue::List(vec![DataValue::from(n.get_float()), DataValue::from(1)]);
            true
        }
        (accum @ DataValue::List(_), DataValue::Guard) => {
            let args = accum.get_list().unwrap();
            let total = args[0].get_float().unwrap();
            let count = args[1].get_float().unwrap();
            *accum = DataValue::from(total / count);
            true
        }
        (DataValue::List(l), DataValue::Number(j)) => {
            let new_total = l[0].get_float().unwrap() + j.get_float();
            l[0] = DataValue::from(new_total);
            let new_count = l[1].get_int().unwrap() + 1;
            l[1] = DataValue::from(new_count);
            true
        }
        (i, j) => bail!(
            "cannot compute mean: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    })
}

define_aggr!(AGGR_SUM, false);
fn aggr_sum(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Number(Number::Int(0));
            true
        }
        (accum @ DataValue::Guard, DataValue::Number(Number::Int(i))) => {
            *accum = DataValue::Number(Number::Int(*i));
            true
        }
        (accum @ DataValue::Guard, DataValue::Number(Number::Float(f))) => {
            *accum = DataValue::Number(Number::Float(*f));
            true
        }
        (DataValue::Number(_), DataValue::Guard) => false,
        (DataValue::Number(i), DataValue::Number(j)) => {
            match (*i, *j) {
                (Number::Int(a), Number::Int(b)) => {
                    *i = Number::Int(a + b);
                }
                (Number::Float(a), Number::Float(b)) => {
                    *i = Number::Float(a + b);
                }
                (Number::Int(a), Number::Float(b)) => {
                    *i = Number::Float((a as f64) + b);
                }
                (Number::Float(a), Number::Int(b)) => {
                    *i = Number::Float(a + (b as f64));
                }
            }
            true
        }
        (i, j) => bail!(
            "cannot compute min: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    })
}

define_aggr!(AGGR_MIN, true);
fn aggr_min(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Number(n)) => {
            *accum = DataValue::Number(*n);
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::Number(i), DataValue::Number(j)) => {
            if *i <= *j {
                false
            } else {
                *i = *j;
                true
            }
        }
        (i, j) => bail!(
            "cannot compute min: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    })
}

define_aggr!(AGGR_MAX, true);
fn aggr_max(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Number(n)) => {
            *accum = DataValue::Number(*n);
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::Number(i), DataValue::Number(j)) => {
            if *i >= *j {
                false
            } else {
                *i = *j;
                true
            }
        }
        (i, j) => bail!(
            "cannot compute max: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    })
}

define_aggr!(AGGR_CHOICE, true);
fn aggr_choice(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, v) => {
            *accum = v.clone();
            true
        }
        _ => false,
    })
}

define_aggr!(AGGR_MIN_COST, true);
fn aggr_min_cost(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Null;
            true
        }
        (accum @ DataValue::Guard, l @ DataValue::List(_)) => {
            if l.get_list().unwrap().len() != 2 {
                bail!("'min_cost' requires a list of length 2 as argument, got {:?}", l);
            }
            *accum = l.clone();
            true
        }
        (_, DataValue::Guard) => false,
        (accum, DataValue::List(l)) => {
            if l.len() != 2 {
                bail!("'min_cost' requires a list of length 2 as argument, got {:?}", l);
            }
            let cur_cost = l.get(1).unwrap();
            let prev = accum.get_list().unwrap();
            let prev_cost = prev.get(1).unwrap();

            if prev_cost <= cur_cost {
                false
            } else {
                *accum = DataValue::List(l.clone());
                true
            }
        }
        (_, v) => bail!("cannot compute 'min_cost' on {:?}", v),
    })
}

define_aggr!(AGGR_SHORTEST, true);
fn aggr_shortest(accum: &mut DataValue, current: &DataValue, _args: &[DataValue]) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::Null;
            true
        }
        (accum @ DataValue::Guard, l @ DataValue::List(_)) => {
            *accum = l.clone();
            true
        }
        (_, DataValue::Guard) => false,
        (accum, DataValue::List(l)) => {
            let current = accum.get_list().unwrap();
            if current.len() <= l.len() {
                false
            } else {
                *accum = DataValue::List(l.clone());
                true
            }
        }
        (_, v) => bail!("cannot compute 'shortest' on {:?}", v),
    })
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
        _ => return None,
    })
}
