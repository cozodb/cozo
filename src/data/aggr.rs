use std::fmt::{Debug, Formatter};

use anyhow::{bail, Result};

use crate::data::value::{DataValue, Number};

#[derive(Clone)]
pub(crate) struct Aggregation {
    pub(crate) name: &'static str,
    pub(crate) combine: fn(&mut DataValue, &DataValue) -> Result<bool>,
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

define_aggr!(AGGR_COLLECT, false);
fn aggr_collect(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::List(vec![]);
            true
        }
        (accum@DataValue::Guard, val) => {
            *accum = DataValue::List(vec![val.clone()]);
            true
        }
        (_, DataValue::Guard) => false,
        (DataValue::List(l), val) => {
            l.push(val.clone());
            true
        }
        _ => unreachable!(),
    })
}

define_aggr!(AGGR_COUNT, false);
fn aggr_count(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
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
fn aggr_mean(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Guard) => {
            *accum = DataValue::from(0.);
            true
        }
        (accum @ DataValue::Guard, DataValue::Number(Number::Int(i))) => {
            *accum = DataValue::List(vec![DataValue::from(*i as f64), DataValue::from(1)]);
            true
        }
        (accum @ DataValue::Guard, DataValue::Number(Number::Float(f))) => {
            *accum = DataValue::List(vec![DataValue::from(*f), DataValue::from(1)]);
            true
        }
        (accum@DataValue::List(_), DataValue::Guard) => {
            let args = accum.get_list().unwrap();
            let total = args[0].get_float().unwrap();
            let count = args[1].get_float().unwrap();
            *accum = DataValue::from(total / count);
            true
        },
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
fn aggr_sum(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
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
fn aggr_min(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Number(n)) => {
            *accum = DataValue::Number(*n);
            true
        }
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
fn aggr_max(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, DataValue::Number(n)) => {
            *accum = DataValue::Number(*n);
            true
        }
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
fn aggr_choice(accum: &mut DataValue, current: &DataValue) -> Result<bool> {
    Ok(match (accum, current) {
        (accum @ DataValue::Guard, v) => {
            *accum = v.clone();
            true
        }
        _ => false,
    })
}

pub(crate) fn get_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "count" => &AGGR_COUNT,
        "sum" => &AGGR_SUM,
        "min" => &AGGR_MIN,
        "max" => &AGGR_MAX,
        "mean" => &AGGR_MEAN,
        "choice" => &AGGR_CHOICE,
        "collect" => &AGGR_COLLECT,
        _ => return None,
    })
}
