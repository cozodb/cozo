use std::collections::BTreeMap;
use crate::relation::value::Value;


#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum Typing {
    Any,
    Bool,
    Int,
    Float,
    Text,
    Uuid,
    UInt,
    Nullable(Box<Typing>),
    Homogeneous(Box<Typing>),
    UnnamedTuple(Vec<Typing>),
    NamedTuple(BTreeMap<String, Typing>),
}

impl Typing {
    #[inline]
    pub fn to_storage(&self, _v: Value) -> Option<Value> {
        todo!()
    }
    #[inline]
    pub fn to_display(&self, _v: Value) -> Option<Value> {
        todo!()
    }
}