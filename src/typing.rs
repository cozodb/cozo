use std::collections::BTreeMap;
use crate::env::Env;

#[derive(Debug, Eq, PartialEq)]
pub enum BaseType {
    Bool,
    Int,
    UInt,
    Float,
    String,
    BitArr,
    U8Arr,
    I8Arr,
    I16Arr,
    U16Arr,
    I32Arr,
    U32Arr,
    I64Arr,
    U64Arr,
    F16Arr,
    F32Arr,
    F64Arr,
    C32Arr,
    C64Arr,
    C128Arr,
    Uuid,
    Timestamp,
    Datetime,
    Timezone,
    Date,
    Time,
    Duration,
    BigInt,
    BigDecimal,
    Inet,
    Crs,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Typing {
    Any,
    Base(BaseType),
    HList(Box<Typing>),
    Nullable(Box<Typing>),
    Tuple(Vec<Typing>),
    NamedTuple(BTreeMap<String, Typing>),
}


pub fn define_types<T: Env<Typing>>(env: &mut T) {
    env.define("Any", Typing::Any);
    env.define("Bool", Typing::Base(BaseType::Bool));
    env.define("Int", Typing::Base(BaseType::Int));
    env.define("UInt", Typing::Base(BaseType::UInt));
    env.define("Float", Typing::Base(BaseType::Float));
    env.define("String", Typing::Base(BaseType::String));
    env.define("Bytes", Typing::Base(BaseType::U8Arr));
    env.define("U8Arr", Typing::Base(BaseType::U8Arr));
    env.define("Uuid", Typing::Base(BaseType::Uuid));
    env.define("Timestamp", Typing::Base(BaseType::Timestamp));
    env.define("Datetime", Typing::Base(BaseType::Datetime));
    env.define("Timezone", Typing::Base(BaseType::Timezone));
    env.define("Date", Typing::Base(BaseType::Date));
    env.define("Time", Typing::Base(BaseType::Time));
    env.define("Duration", Typing::Base(BaseType::Duration));
    env.define("BigInt", Typing::Base(BaseType::BigInt));
    env.define("BigDecimal", Typing::Base(BaseType::BigDecimal));
    env.define("Int", Typing::Base(BaseType::Int));
    env.define("Crs", Typing::Base(BaseType::Crs));
}