use std::fmt::{Debug, Formatter};

use anyhow::Result;

use crate::data::value::DataValue;

#[derive(Clone)]
pub(crate) struct Aggregation {
    pub(crate) name: &'static str,
    pub(crate) init_state: fn() -> DataValue,
    pub(crate) combine: fn(&DataValue, &DataValue) -> Result<DataValue>,
}

impl Debug for Aggregation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggr<{}>", self.name)
    }
}

macro_rules! define_aggr {
    ($name:ident, $init:ident) => {
        const $name: Aggregation = Aggregation {
            name: stringify!($name),
            init_state: $init,
            combine: ::casey::lower!($name),
        };
    };
}

fn init_zero() -> DataValue {
    DataValue::Int(0)
}

define_aggr!(AGGR_COUNT, init_zero);
fn aggr_count(existing: &DataValue, _: &DataValue) -> Result<DataValue> {
    match existing {
        DataValue::Int(i) => Ok(DataValue::Int(*i + 1)),
        _ => unreachable!(),
    }
}
