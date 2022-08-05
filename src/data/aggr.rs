use std::cmp::{max, min};
use std::fmt::{Debug, Formatter};

use anyhow::{bail, Result};

use crate::data::value::DataValue;

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
        (DataValue::Bottom, DataValue::Bottom) => Ok(DataValue::Int(0)),
        (DataValue::Bottom, _) => Ok(DataValue::Int(1)),
        (DataValue::Int(i), DataValue::Bottom) => Ok(DataValue::Int(*i)),
        (DataValue::Int(i), _) => Ok(DataValue::Int(*i + 1)),
        _ => unreachable!(),
    }
}

define_aggr!(AGGR_SUM, false);
fn aggr_sum(accum: &DataValue, current: &DataValue) -> Result<DataValue> {
    match (accum, current) {
        (DataValue::Bottom, DataValue::Bottom) => Ok(DataValue::Int(0)),
        (DataValue::Bottom, DataValue::Int(i)) => Ok(DataValue::Int(*i)),
        (DataValue::Bottom, DataValue::Float(f)) => Ok(DataValue::Float(f.0.into())),
        (DataValue::Int(i), DataValue::Bottom) => Ok(DataValue::Int(*i)),
        (DataValue::Float(f), DataValue::Bottom) => Ok(DataValue::Float(f.0.into())),
        (DataValue::Int(i), DataValue::Int(j)) => Ok(DataValue::Int(*i + *j)),
        (DataValue::Int(j), DataValue::Float(i)) | (DataValue::Float(i), DataValue::Int(j)) => {
            Ok(DataValue::Float((i.0 + (*j as f64)).into()))
        }
        (DataValue::Float(i), DataValue::Float(j)) => Ok(DataValue::Float((i.0 + j.0).into())),
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
        (DataValue::Bottom, DataValue::Int(i)) => Ok(DataValue::Int(*i)),
        (DataValue::Bottom, DataValue::Float(f)) => Ok(DataValue::Float(f.0.into())),
        (DataValue::Int(i), DataValue::Int(j)) => Ok(DataValue::Int(min(*i, *j))),
        (DataValue::Int(j), DataValue::Float(i)) | (DataValue::Float(i), DataValue::Int(j)) => {
            Ok(DataValue::Float(min(i.clone(), (*j as f64).into())))
        }
        (DataValue::Float(i), DataValue::Float(j)) => {
            Ok(DataValue::Float(min(i.clone(), j.clone())))
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
        (DataValue::Bottom, DataValue::Int(i)) => Ok(DataValue::Int(*i)),
        (DataValue::Bottom, DataValue::Float(f)) => Ok(DataValue::Float(f.0.into())),
        (DataValue::Float(f), DataValue::Bottom) => Ok(DataValue::Float(f.0.into())),
        (DataValue::Int(i), DataValue::Int(j)) => Ok(DataValue::Int(max(*i, *j))),
        (DataValue::Int(j), DataValue::Float(i)) | (DataValue::Float(i), DataValue::Int(j)) => {
            Ok(DataValue::Float(max(i.clone(), (*j as f64).into())))
        }
        (DataValue::Float(i), DataValue::Float(j)) => {
            Ok(DataValue::Float(max(i.clone(), j.clone())))
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
    Ok(if *accum == DataValue::Bottom {
        current.clone()
    } else {
        accum.clone()
    })
}

pub(crate) fn get_aggr(name: &str) -> Option<&'static Aggregation> {
    Some(match name {
        "Count" => &AGGR_COUNT,
        "Sum" => &AGGR_SUM,
        "Min" => &AGGR_MIN,
        "Max" => &AGGR_MAX,
        "Choice" => &AGGR_CHOICE,
        _ => return None,
    })
}
