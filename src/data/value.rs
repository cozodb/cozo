use std::cmp::{Ordering, Reverse};
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use ordered_float::OrderedFloat;
use regex::Regex;
use rmp_serde::Serializer;
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

use crate::data::encode::EncodedVec;
use crate::data::id::{EntityId, TxId};
use crate::data::triple::StoreOp;

#[derive(Clone, Hash, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) struct UuidWrapper(pub(crate) Uuid);

impl UuidWrapper {
    pub(crate) fn to_100_nanos(&self) -> Option<u64> {
        self.0.get_timestamp().map(|t| t.to_unix_nanos())
    }
}

impl PartialOrd<Self> for UuidWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UuidWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.to_100_nanos(), other.to_100_nanos()) {
            (Some(a), Some(b)) => {
                a.cmp(&b).then_with(||
                    self.0.as_bytes().cmp(other.0.as_bytes()))
            }
            _ => self.0.as_bytes().cmp(other.0.as_bytes())
        }
    }
}

#[derive(Clone)]
pub(crate) struct RegexWrapper(pub(crate) Regex);

impl Hash for RegexWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_str().hash(state)
    }
}

impl Serialize for RegexWrapper {
    fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
    {
        panic!("serializing regex");
    }
}

impl<'de> Deserialize<'de> for RegexWrapper {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
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

#[derive(
Clone, PartialEq, Eq, PartialOrd, Ord, serde_derive::Deserialize, serde_derive::Serialize, Hash,
)]
pub(crate) enum DataValue {
    #[serde(rename = "0", alias = "Null")]
    Null,
    #[serde(rename = "B", alias = "Bool")]
    Bool(bool),
    #[serde(rename = "N", alias = "Num")]
    Num(Num),
    #[serde(rename = "S", alias = "Str")]
    Str(SmartString<LazyCompact>),
    #[serde(rename = "X", alias = "Bytes", with = "serde_bytes")]
    Bytes(Vec<u8>),
    #[serde(rename = "U", alias = "Uuid")]
    Uuid(UuidWrapper),
    #[serde(rename = "R", alias = "Regex")]
    Regex(RegexWrapper),
    #[serde(rename = "L", alias = "List")]
    List(Vec<DataValue>),
    #[serde(rename = "H", alias = "Set")]
    Set(BTreeSet<DataValue>),
    #[serde(rename = "R", alias = "Rev")]
    Rev(Reverse<Box<DataValue>>),
    #[serde(rename = "G", alias = "Guard")]
    Guard,
    #[serde(rename = "_", alias = "Bot")]
    Bot,
}

pub(crate) fn same_value_type(a: &DataValue, b: &DataValue) -> bool {
    use DataValue::*;
    match (a, b) {
        (Null, Null)
        | (Bool(_), Bool(_))
        | (Num(_), Num(_))
        | (Str(_), Str(_))
        | (Bytes(_), Bytes(_))
        | (Regex(_), Regex(_))
        | (List(_), List(_))
        | (Set(_), Set(_))
        | (Rev(_), Rev(_))
        | (Guard, Guard)
        | (Bot, Bot) => true,
        _ => false,
    }
}

impl From<i64> for DataValue {
    fn from(v: i64) -> Self {
        DataValue::Num(Num::I(v))
    }
}

impl From<f64> for DataValue {
    fn from(v: f64) -> Self {
        DataValue::Num(Num::F(v))
    }
}

#[derive(Copy, Clone, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) enum Num {
    #[serde(alias = "Int")]
    I(i64),
    #[serde(alias = "Float")]
    F(f64),
}

impl Hash for Num {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Num::I(i) => i.hash(state),
            Num::F(f) => OrderedFloat(*f).hash(state),
        }
    }
}

impl Num {
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            Num::I(i) => Some(*i),
            _ => None,
        }
    }
    pub(crate) fn get_float(&self) -> f64 {
        match self {
            Num::I(i) => *i as f64,
            Num::F(f) => *f,
        }
    }
}

impl PartialEq for Num {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Num {}

impl Display for Num {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Num::I(i) => write!(f, "{}", i),
            Num::F(n) => write!(f, "{}", n),
        }
    }
}

impl Debug for Num {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Num::I(i) => write!(f, "{}", i),
            Num::F(n) => write!(f, "{}", n),
        }
    }
}

impl PartialOrd for Num {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Num {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Num::I(i), Num::F(r)) => {
                let l = *i as f64;
                match l.total_cmp(r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Num::F(l), Num::I(i)) => {
                let r = *i as f64;
                match l.total_cmp(&r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Greater,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Num::I(l), Num::I(r)) => l.cmp(r),
            (Num::F(l), Num::F(r)) => l.total_cmp(r),
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
            DataValue::Num(i) => {
                write!(f, "{}", i)
            }
            DataValue::Str(s) => {
                write!(f, "{:?}", s)
            }
            DataValue::Regex(r) => {
                write!(f, "{:?}", r.0.as_str())
            }
            DataValue::Bytes(b) => {
                write!(f, "bytes(len={})", b.len())
            }
            DataValue::List(t) => f.debug_list().entries(t.iter()).finish(),
            DataValue::Set(t) => f.debug_list().entries(t.iter()).finish(),
            DataValue::Rev(v) => {
                write!(f, "desc<{:?}>", v)
            }
            DataValue::Bot => {
                write!(f, "bottom")
            }
            DataValue::Guard => {
                write!(f, "guard")
            }
            DataValue::Uuid(u) => {
                let encoded = base64::encode_config(u.0.as_bytes(), base64::URL_SAFE_NO_PAD);
                write!(f, "{}", encoded)
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

    pub(crate) fn get_entity_id(&self) -> Option<EntityId> {
        self.get_uuid().map(EntityId)
    }
    pub(crate) fn get_list(&self) -> Option<&[DataValue]> {
        match self {
            DataValue::List(l) => Some(l),
            _ => None,
        }
    }
    pub(crate) fn get_string(&self) -> Option<&str> {
        match self {
            DataValue::Str(s) => Some(s),
            _ => None,
        }
    }
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            DataValue::Num(n) => n.get_int(),
            _ => None,
        }
    }
    pub(crate) fn get_non_neg_int(&self) -> Option<u64> {
        match self {
            DataValue::Num(n) => n
                .get_int()
                .and_then(|i| if i < 0 { None } else { Some(i as u64) }),
            _ => None,
        }
    }
    pub(crate) fn get_float(&self) -> Option<f64> {
        match self {
            DataValue::Num(n) => Some(n.get_float()),
            _ => None,
        }
    }
    pub(crate) fn get_bool(&self) -> Option<bool> {
        match self {
            DataValue::Bool(b) => Some(*b),
            _ => None
        }
    }
    pub(crate) fn uuid(uuid: uuid::Uuid) -> Self {
        Self::Uuid(UuidWrapper(uuid))
    }
    pub(crate) fn get_uuid(&self) -> Option<Uuid> {
        match self {
            DataValue::Uuid(UuidWrapper(uuid)) => Some(*uuid),
            DataValue::Str(s) => {
                uuid::Uuid::try_parse(s).ok()
            }
            _ => None
        }
    }
}

pub(crate) const LARGEST_UTF_CHAR: char = '\u{10ffff}';

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

    #[test]
    fn utf8() {
        let c = char::from_u32(0x10FFFF).unwrap();
        let mut s = String::new();
        s.push(c);
        println!("{}", s);
        println!(
            "{:b} {:b} {:b} {:b}",
            s.as_bytes()[0],
            s.as_bytes()[1],
            s.as_bytes()[2],
            s.as_bytes()[3]
        );
        dbg!(s);
    }
}
