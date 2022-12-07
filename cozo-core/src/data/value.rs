/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use ordered_float::OrderedFloat;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use smartstring::{LazyCompact, SmartString};
use uuid::Uuid;

#[derive(Clone, Hash, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub struct UuidWrapper(pub(crate) Uuid);

impl PartialOrd<Self> for UuidWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UuidWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        let (s_l, s_m, s_h, s_rest) = self.0.as_fields();
        let (o_l, o_m, o_h, o_rest) = other.0.as_fields();
        s_h.cmp(&o_h)
            .then_with(|| s_m.cmp(&o_m))
            .then_with(|| s_l.cmp(&o_l))
            .then_with(|| s_rest.cmp(o_rest))
    }
}

#[derive(Clone)]
pub struct RegexWrapper(pub(crate) Regex);

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
pub enum DataValue {
    Null,
    Bool(bool),
    Num(Num),
    Str(SmartString<LazyCompact>),
    #[serde(with = "serde_bytes")]
    Bytes(Vec<u8>),
    Uuid(UuidWrapper),
    Regex(RegexWrapper),
    List(Vec<DataValue>),
    Set(BTreeSet<DataValue>),
    Bot,
}

impl From<i64> for DataValue {
    fn from(v: i64) -> Self {
        DataValue::Num(Num::Int(v))
    }
}

impl From<f64> for DataValue {
    fn from(v: f64) -> Self {
        DataValue::Num(Num::Float(v))
    }
}

#[derive(Copy, Clone, serde_derive::Deserialize, serde_derive::Serialize)]
pub enum Num {
    Int(i64),
    Float(f64),
}

impl Hash for Num {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Num::Int(i) => i.hash(state),
            Num::Float(f) => OrderedFloat(*f).hash(state),
        }
    }
}

impl Num {
    pub(crate) fn get_int(&self) -> Option<i64> {
        match self {
            Num::Int(i) => Some(*i),
            Num::Float(f) => {
                if f.round() == *f {
                    Some(*f as i64)
                } else {
                    None
                }
            }
        }
    }
    pub(crate) fn get_float(&self) -> f64 {
        match self {
            Num::Int(i) => *i as f64,
            Num::Float(f) => *f,
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
            Num::Int(i) => write!(f, "{}", i),
            Num::Float(n) => {
                if n.is_nan() {
                    write!(f, r#"to_float("NAN")"#)
                } else if n.is_infinite() {
                    if n.is_sign_negative() {
                        write!(f, r#"to_float("NEG_INF")"#)
                    } else {
                        write!(f, r#"to_float("INF")"#)
                    }
                } else {
                    write!(f, "{}", n)
                }
            }
        }
    }
}

impl Debug for Num {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Num::Int(i) => write!(f, "{}", i),
            Num::Float(n) => write!(f, "{}", n),
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
            (Num::Int(i), Num::Float(r)) => {
                let l = *i as f64;
                match l.total_cmp(r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Less,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Num::Float(l), Num::Int(i)) => {
                let r = *i as f64;
                match l.total_cmp(&r) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => Ordering::Greater,
                    Ordering::Greater => Ordering::Greater,
                }
            }
            (Num::Int(l), Num::Int(r)) => l.cmp(r),
            (Num::Float(l), Num::Float(r)) => l.total_cmp(r),
        }
    }
}

impl Debug for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => f.write_str("null"),
            DataValue::Bool(b) => write!(f, "{}", b),
            DataValue::Num(n) => write!(f, "{}", n),
            DataValue::Str(s) => write!(f, "{:?}", s),
            DataValue::Bytes(b) => {
                let bs = base64::encode(b);
                write!(f, "decode_base64({:?})", bs)
            }
            DataValue::Uuid(u) => {
                let us = u.0.to_string();
                write!(f, "to_uuid({:?})", us)
            }
            DataValue::Regex(rx) => {
                write!(f, "regex({:?})", rx.0.as_str())
            }
            DataValue::List(ls) => f.debug_list().entries(ls).finish(),
            DataValue::Set(s) => f.debug_list().entries(s).finish(),
            DataValue::Bot => write!(f, "null"),
        }
    }
}

impl DataValue {
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
            _ => None,
        }
    }
    pub(crate) fn uuid(uuid: uuid::Uuid) -> Self {
        Self::Uuid(UuidWrapper(uuid))
    }
    pub(crate) fn get_uuid(&self) -> Option<Uuid> {
        match self {
            DataValue::Uuid(UuidWrapper(uuid)) => Some(*uuid),
            DataValue::Str(s) => uuid::Uuid::try_parse(s).ok(),
            _ => None,
        }
    }
}

pub(crate) const LARGEST_UTF_CHAR: char = '\u{10ffff}';

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;

    use smartstring::SmartString;

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

    #[test]
    fn display_datavalues() {
        println!("{}", DataValue::Null);
        println!("{}", DataValue::Bool(true));
        println!("{}", DataValue::from(-1));
        println!("{}", DataValue::from(-1121212121.331212121));
        println!("{}", DataValue::from(f64::NAN));
        println!("{}", DataValue::from(f64::NEG_INFINITY));
        println!(
            "{}",
            DataValue::List(vec![
                DataValue::Bool(false),
                DataValue::Str(SmartString::from(r###"abc"你"好'啊👌"###)),
                DataValue::from(f64::NEG_INFINITY)
            ])
        );
    }
}
