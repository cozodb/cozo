use std::cmp::{max, min, Ordering};
use std::fmt::{Debug, Formatter};

use anyhow::{bail, Result};

use crate::data::value::{DataValue, Number};

#[derive(Clone)]
pub(crate) struct Aggregation {
    pub(crate) name: &'static str,
    pub(crate) combine: fn(&DataValue, &DataValue) -> Result<DataValue>,
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

define_aggr!(AGGR_COUNT, false);
fn aggr_count(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    match (accum, current) {
        (DataValue::Guard, DataValue::Guard) => Ok(DataValue::Number(Number::Int(0))),
        (DataValue::Guard, _) => Ok(DataValue::Number(Number::Int(1))),
        (DataValue::Number(Number::Int(i)), DataValue::Guard) => {
            Ok(DataValue::Number(Number::Int(*i)))
        }
        (DataValue::Number(Number::Int(i)), _) => Ok(DataValue::Number(Number::Int(*i + 1))),
        _ => unreachable!(),
    }
}

define_aggr!(AGGR_SUM, false);
fn aggr_sum(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    match (accum, current) {
        (DataValue::Guard, DataValue::Guard) => Ok(DataValue::Number(Number::Int(0))),
        (DataValue::Guard, DataValue::Number(Number::Int(i))) => {
            Ok(DataValue::Number(Number::Int(*i)))
        }
        (DataValue::Guard, DataValue::Number(Number::Float(f))) => {
            Ok(DataValue::Number(Number::Float(*f)))
        }
        (DataValue::Number(Number::Int(i)), DataValue::Guard) => {
            Ok(DataValue::Number(Number::Int(*i)))
        }
        (DataValue::Number(Number::Float(f)), DataValue::Guard) => {
            Ok(DataValue::Number(Number::Float(*f)))
        }
        (DataValue::Number(Number::Int(i)), DataValue::Number(Number::Int(j))) => {
            Ok(DataValue::Number(Number::Int(*i + *j)))
        }
        (DataValue::Number(Number::Int(j)), DataValue::Number(Number::Float(i)))
        | (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Int(j))) => {
            Ok(DataValue::Number(Number::Float(*i + (*j as f64))))
        }
        (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Float(j))) => {
            Ok(DataValue::Number(Number::Float(*i + *j)))
        }
        (i, j) => bail!(
            "cannot compute min: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    }
}

define_aggr!(AGGR_MIN, true);
fn aggr_min(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    match (accum, current) {
        (DataValue::Guard, DataValue::Number(Number::Int(i))) => {
            Ok(DataValue::Number(Number::Int(*i)))
        }
        (DataValue::Guard, DataValue::Number(Number::Float(f))) => {
            Ok(DataValue::Number(Number::Float(*f)))
        }
        (DataValue::Number(Number::Int(i)), DataValue::Number(Number::Int(j))) => {
            Ok(DataValue::Number(Number::Int(min(*i, *j))))
        }
        (DataValue::Number(Number::Int(j)), DataValue::Number(Number::Float(i)))
        | (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Int(j))) => {
            let m = match i.total_cmp(&(*j as f64)) {
                Ordering::Less => *i,
                Ordering::Equal => *i,
                Ordering::Greater => *j as f64,
            };
            Ok(DataValue::Number(Number::Float(m)))
        }
        (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Float(j))) => {
            let m = match i.total_cmp(j) {
                Ordering::Less => *i,
                Ordering::Equal => *i,
                Ordering::Greater => *j,
            };
            Ok(DataValue::Number(Number::Float(m)))
        }
        (i, j) => bail!(
            "cannot compute min: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    }
}

define_aggr!(AGGR_MAX, true);
fn aggr_max(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    match (accum, current) {
        (DataValue::Guard, DataValue::Number(Number::Int(i))) => {
            Ok(DataValue::Number(Number::Int(*i)))
        }
        (DataValue::Guard, DataValue::Number(Number::Float(f))) => {
            Ok(DataValue::Number(Number::Float(*f)))
        }
        (DataValue::Number(Number::Float(f)), DataValue::Guard) => {
            Ok(DataValue::Number(Number::Float(*f)))
        }
        (DataValue::Number(Number::Int(i)), DataValue::Number(Number::Int(j))) => {
            Ok(DataValue::Number(Number::Int(max(*i, *j))))
        }
        (DataValue::Number(Number::Int(j)), DataValue::Number(Number::Float(i)))
        | (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Int(j))) => {
            let m = match i.total_cmp(&(*j as f64)) {
                Ordering::Less => *j as f64,
                Ordering::Equal => *i,
                Ordering::Greater => *i,
            };
            Ok(DataValue::Number(Number::Float(m)))
        }
        (DataValue::Number(Number::Float(i)), DataValue::Number(Number::Float(j))) => {
            let m = match i.total_cmp(j) {
                Ordering::Less => *j,
                Ordering::Equal => *i,
                Ordering::Greater => *i,
            };
            Ok(DataValue::Number(Number::Float(m)))
        }
        (i, j) => bail!(
            "cannot compute min: encountered value {:?} for aggregate {:?}",
            j,
            i
        ),
    }
}

define_aggr!(AGGR_CHOICE, true);
fn aggr_choice(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    Ok(if *accum == DataValue::Guard {
        current.clone()
    } else {
        accum.clone()
    })
}

pub(crate) fn get_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "count" => &AGGR_COUNT,
        "sum" => &AGGR_SUM,
        "min" => &AGGR_MIN,
        "max" => &AGGR_MAX,
        "choice" => &AGGR_CHOICE,
        _ => return None,
    })
}
