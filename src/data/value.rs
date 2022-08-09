use std::cmp::{Ordering, Reverse};
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};

use anyhow::{bail, Result};
use regex::Regex;
use rmp_serde::Serializer;
use serde::{Deserialize, Deserializer, Serialize};
use serde_derive::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

use crate::data::encode::EncodedVec;
use crate::data::id::{EntityId, TxId};
use crate::data::triple::StoreOp;

#[derive(Clone)]
pub(crate) struct RegexWrapper(pub(crate) Regex);

impl Serialize for RegexWrapper {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error> where S: serde::Serializer {
        panic!("serializing regex");
    }
}

impl<'de> Deserialize<'de> for RegexWrapper {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error> where D: Deserializer<'de> {
        panic!("deserializing regex");
    }
}

impl PartialEq for RegexWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for RegexWrapper {}

impl Ord for RegexWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_str().cmp(other.0.as_str())
    }
}

impl PartialOrd for RegexWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.as_str().partial_cmp(other.0.as_str())
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub(crate) enum DataValue {
    #[serde(rename = "n")]
    Null,
    #[serde(rename = "b")]
    Bool(bool),
    #[serde(rename = "i")]
    Number(Number),
    #[serde(rename = "s")]
    String(SmartString<LazyCompact>),
    #[serde(rename = "u")]
    Uuid(Uuid),
    #[serde(rename = "m")]
    Timestamp(i64),
    #[serde(rename = "v")]
    Bytes(Box<[u8]>),
    #[serde(rename = "x")]
    Regex(RegexWrapper),
    #[serde(rename = "z")]
    List(Vec<DataValue>),
    #[serde(rename = "y")]
    Set(BTreeSet<DataValue>),
    #[serde(rename = "g")]
    Guard,
    #[serde(rename = "o")]
    DescVal(Reverse<Box<DataValue>>),
    #[serde(rename = "r")]
    Bottom,
}

impl From<i64> for DataValue {
    fn from(v: i64) -> Self {
        DataValue::Number(Number::Int(v))
    }
}

impl From<f64> for DataValue {
    fn from(v: f64) -> Self {
        DataValue::Number(Number::Float(v))
    }
}

#[derive(Copy, Clone, Deserialize, Serialize)]
pub(crate) enum Number {
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "f")]
    Float(f64),
}

impl Number {
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            Number::Int(i) => Some(*i),
            _ => None,
        }
    }
    pub(crate) fn get_float(&self) -> f64 {
        match self {
            Number::Int(i) => *i as f64,
            Number::Float(f) => *f,
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Number {}

impl Display for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Number::Int(i) => write!(f, "{}", i),
            Number::Float(n) => write!(f, "{}", n),
        }
    }
}

impl Debug for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Number::Int(i) => write!(f, "{}", i),
            Number::Float(n) => write!(f, "{}", n),
        }
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Number::Int(i), Number::Float(r)) => {
                let l = *i as f64;
                match l.total_cmp(&r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Number::Float(l), Number::Int(i)) => {
                let r = *i as f64;
                match l.total_cmp(&r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Greater,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Number::Int(l), Number::Int(r)) => l.cmp(r),
            (Number::Float(l), Number::Float(r)) => l.total_cmp(r),
        }
    }
}

impl Debug for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => {
                write!(f, "null")
            }
            DataValue::Bool(b) => {
                write!(f, "{}", b)
            }
            DataValue::Number(i) => {
                write!(f, "{}", i)
            }
            DataValue::String(s) => {
                write!(f, "{:?}", s)
            }
            DataValue::Regex(r) => {
                write!(f, "{:?}", r.0.as_str())
            }
            DataValue::Uuid(u) => {
                write!(f, "{}", u)
            }
            DataValue::Timestamp(ts) => {
                write!(f, "ts@{}", ts)
            }
            DataValue::Bytes(b) => {
                write!(f, "bytes(len={})", b.len())
            }
            DataValue::List(t) => f.debug_list().entries(t.iter()).finish(),
            DataValue::Set(t) => f.debug_list().entries(t.iter()).finish(),
            DataValue::DescVal(v) => {
                write!(f, "desc<{:?}>", v)
            }
            DataValue::Bottom => {
                write!(f, "bottom")
            }
            DataValue::Guard => {
                write!(f, "guard")
            }
        }
    }
}

pub(crate) const INLINE_VAL_SIZE_LIMIT: usize = 60;

impl DataValue {
    pub(crate) fn encode_with_op_and_tx(
        &self,
        op: StoreOp,
        txid: TxId,
    ) -> EncodedVec<INLINE_VAL_SIZE_LIMIT> {
        let mut ret = SmallVec::<[u8; INLINE_VAL_SIZE_LIMIT]>::new();
        ret.extend(txid.bytes());
        ret[0] = op as u8;
        self.serialize(&mut Serializer::new(&mut ret)).unwrap();
        ret.into()
    }

    pub(crate) fn get_entity_id(&self) -> Result<EntityId> {
        match self {
            DataValue::Number(Number::Int(id)) => Ok(EntityId(*id as u64)),
            _ => bail!("type mismatch: expect type EntId, got value {:?}", self),
        }
    }
    pub(crate) fn get_list(&self) -> Option<&[DataValue]> {
        match self {
            DataValue::List(l) => Some(l),
            _ => None,
        }
    }
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            DataValue::Number(n) => n.get_int(),
            _ => None,
        }
    }
    pub(crate) fn get_float(&self) -> Option<f64> {
        match self {
            DataValue::Number(n) => Some(n.get_float()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;

    use crate::data::symb::Symbol;
    use crate::data::value::DataValue;

    #[test]
    fn show_size() {
        dbg!(size_of::<DataValue>());
        dbg!(size_of::<Symbol>());
        dbg!(size_of::<String>());
        dbg!(size_of::<HashMap<String, String>>());
        dbg!(size_of::<BTreeMap<String, String>>());
    }
}
