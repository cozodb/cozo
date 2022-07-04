use crate::data::id::{AttrId, EntityId, TxId};
use crate::data::keyword::Keyword;
use ordered_float::OrderedFloat;
use serde_derive::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Reverse;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Deserialize, Serialize)]
pub enum Value<'a> {
    #[serde(rename = "n")]
    Null,
    #[serde(rename = "b")]
    Bool(bool),
    #[serde(rename = "e")]
    EnId(EntityId),
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "f")]
    Float(OrderedFloat<f64>),
    #[serde(rename = "k")]
    Keyword(Keyword),
    #[serde(borrow)]
    #[serde(rename = "s")]
    String(Cow<'a, str>),
    #[serde(rename = "u")]
    Uuid(Uuid),
    #[serde(rename = "m")]
    Timestamp(i64),
    #[serde(borrow)]
    #[serde(rename = "v")]
    Bytes(Cow<'a, [u8]>),

    #[serde(rename = "z")]
    Tuple(Box<[Value<'a>]>),
    #[serde(rename = "o")]
    DescVal(Reverse<Box<Value<'a>>>),
    #[serde(rename = "r")]
    Bottom,
}

#[cfg(test)]
mod tests {
    use crate::data::keyword::Keyword;
    use crate::data::value::Value;
    use std::collections::{BTreeMap, HashMap};
    use std::mem::size_of;

    #[test]
    fn show_size() {
        dbg!(size_of::<Value>());
        dbg!(size_of::<Keyword>());
        dbg!(size_of::<String>());
        dbg!(size_of::<HashMap<String, String>>());
        dbg!(size_of::<BTreeMap<String, String>>());
    }
}
