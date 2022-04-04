use std::collections::BTreeMap;
use crate::env::Env;
use crate::value::Value;

#[derive(Debug, Eq, PartialEq)]
pub enum PrimitiveType {
    Bool,
    Int,
    UInt,
    Float,
    String,
    Bytes,
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
    Primitive(PrimitiveType),
    HList(Box<Typing>),
    Nullable(Box<Typing>),
    Tuple(Vec<Typing>),
    NamedTuple(BTreeMap<String, Typing>),
}


pub fn define_types<T: Env<Typing>>(env: &mut T) {
    env.define("Any", Typing::Any);
    env.define("Bool", Typing::Primitive(PrimitiveType::Bool));
    env.define("Int", Typing::Primitive(PrimitiveType::Int));
    env.define("UInt", Typing::Primitive(PrimitiveType::UInt));
    env.define("Float", Typing::Primitive(PrimitiveType::Float));
    env.define("String", Typing::Primitive(PrimitiveType::String));
    env.define("Bytes", Typing::Primitive(PrimitiveType::Bytes));
    env.define("Uuid", Typing::Primitive(PrimitiveType::Uuid));
    env.define("Timestamp", Typing::Primitive(PrimitiveType::Timestamp));
    env.define("Datetime", Typing::Primitive(PrimitiveType::Datetime));
    env.define("Timezone", Typing::Primitive(PrimitiveType::Timezone));
    env.define("Date", Typing::Primitive(PrimitiveType::Date));
    env.define("Time", Typing::Primitive(PrimitiveType::Time));
    env.define("Duration", Typing::Primitive(PrimitiveType::Duration));
    env.define("BigInt", Typing::Primitive(PrimitiveType::BigInt));
    env.define("BigDecimal", Typing::Primitive(PrimitiveType::BigDecimal));
    env.define("Int", Typing::Primitive(PrimitiveType::Int));
    env.define("Crs", Typing::Primitive(PrimitiveType::Crs));
}