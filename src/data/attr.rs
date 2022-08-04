use std::fmt::{Display, Formatter};

use anyhow::{anyhow, bail, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use smallvec::SmallVec;

use crate::data::encode::EncodedVec;
use crate::data::id::{AttrId, TxId};
use crate::data::json::JsonValue;
use crate::data::symb::Symbol;
use crate::data::triple::StoreOp;
use crate::data::value::DataValue;
use crate::parse::triple::TempIdCtx;

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub(crate) enum AttributeCardinality {
    One = 1,
    Many = 2,
}

impl AttributeCardinality {
    pub(crate) fn is_one(&self) -> bool {
        *self == AttributeCardinality::One
    }
    pub(crate) fn is_many(&self) -> bool {
        *self == AttributeCardinality::Many
    }
}

impl Display for AttributeCardinality {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttributeCardinality::One => write!(f, "one"),
            AttributeCardinality::Many => write!(f, "many"),
        }
    }
}

impl TryFrom<&'_ str> for AttributeCardinality {
    type Error = anyhow::Error;
    fn try_from(value: &'_ str) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            "one" => AttributeCardinality::One,
            "many" => AttributeCardinality::Many,
            s => bail!("unknown cardinality {}", s),
        })
    }
}

#[repr(u8)]
#[derive(Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub(crate) enum AttributeTyping {
    Ref = 1,
    Component = 2,
    Bool = 3,
    Int = 4,
    Float = 5,
    String = 6,
    Uuid = 7,
    Timestamp = 8,
    Bytes = 9,
    List = 10,
}

impl AttributeTyping {
    pub(crate) fn is_ref_type(&self) -> bool {
        matches!(self, AttributeTyping::Ref | AttributeTyping::Component)
    }
}

impl Display for AttributeTyping {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttributeTyping::Ref => write!(f, "ref"),
            AttributeTyping::Component => write!(f, "component"),
            AttributeTyping::Bool => write!(f, "bool"),
            AttributeTyping::Int => write!(f, "int"),
            AttributeTyping::Float => write!(f, "float"),
            AttributeTyping::String => write!(f, "string"),
            AttributeTyping::Uuid => write!(f, "uuid"),
            AttributeTyping::Timestamp => write!(f, "timestamp"),
            AttributeTyping::Bytes => write!(f, "bytes"),
            AttributeTyping::List => write!(f, "list"),
        }
    }
}

impl TryFrom<&'_ str> for AttributeTyping {
    type Error = anyhow::Error;
    fn try_from(value: &'_ str) -> std::result::Result<Self, Self::Error> {
        use AttributeTyping::*;
        Ok(match value {
            "ref" => Ref,
            "component" => Component,
            "bool" => Bool,
            "int" => Int,
            "float" => Float,
            "string" => String,
            "uuid" => Uuid,
            "timestamp" => Timestamp,
            "bytes" => Bytes,
            "list" => List,
            s => bail!("unknown type {}", s),
        })
    }
}

impl AttributeTyping {
    fn type_err(&self, val: DataValue) -> anyhow::Error {
        anyhow!("cannot coerce {:?} to {:?}", val, self)
    }
    pub(crate) fn coerce_value(&self, val: DataValue) -> Result<DataValue> {
        match self {
            AttributeTyping::Ref | AttributeTyping::Component => match val {
                DataValue::Int(s) if s > 0 => Ok(DataValue::Int(s)),
                val => Err(self.type_err(val).into()),
            },
            AttributeTyping::Bool => {
                if matches!(val, DataValue::Bool(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
            AttributeTyping::Int => {
                if matches!(val, DataValue::Int(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
            AttributeTyping::Float => match val {
                v @ DataValue::Float(_) => Ok(v),
                DataValue::Int(i) => Ok(DataValue::Float((i as f64).into())),
                val => Err(self.type_err(val).into()),
            },
            AttributeTyping::String => {
                if matches!(val, DataValue::String(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
            AttributeTyping::Uuid => {
                if matches!(val, DataValue::Uuid(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
            AttributeTyping::Timestamp => match val {
                val @ DataValue::Timestamp(_) => Ok(val),
                DataValue::Int(i) => Ok(DataValue::Timestamp(i)),
                val => Err(self.type_err(val).into()),
            },
            AttributeTyping::Bytes => {
                if matches!(val, DataValue::Bytes(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
            AttributeTyping::List => {
                if matches!(val, DataValue::List(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val).into())
                }
            }
        }
    }
}

#[repr(u8)]
#[derive(Clone, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub(crate) enum AttributeIndex {
    None = 0,
    Indexed = 1,
    Unique = 2,
    Identity = 3,
}

impl AttributeIndex {
    pub(crate) fn is_unique_index(&self) -> bool {
        matches!(self, AttributeIndex::Identity | AttributeIndex::Unique)
    }
    pub(crate) fn should_index(&self) -> bool {
        *self != AttributeIndex::None
    }
}

impl Display for AttributeIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttributeIndex::None => write!(f, "none"),
            AttributeIndex::Indexed => write!(f, "index"),
            AttributeIndex::Unique => write!(f, "unique"),
            AttributeIndex::Identity => write!(f, "identity"),
        }
    }
}

impl TryFrom<&'_ str> for AttributeIndex {
    type Error = anyhow::Error;
    fn try_from(value: &'_ str) -> std::result::Result<Self, Self::Error> {
        use AttributeIndex::*;
        Ok(match value {
            "none" => None,
            "indexed" => Indexed,
            "unique" => Unique,
            "identity" => Identity,
            s => bail!("unknown attribute indexing type {}", s),
        })
    }
}

#[derive(Clone, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub(crate) struct Attribute {
    #[serde(rename = "i")]
    pub(crate) id: AttrId,
    #[serde(rename = "n")]
    pub(crate) name: Symbol,
    #[serde(rename = "c")]
    pub(crate) cardinality: AttributeCardinality,
    #[serde(rename = "t")]
    pub(crate) val_type: AttributeTyping,
    #[serde(rename = "u")]
    pub(crate) indexing: AttributeIndex,
    #[serde(rename = "h")]
    pub(crate) with_history: bool,
}

const ATTR_VEC_SIZE: usize = 80;

impl Attribute {
    pub(crate) fn encode_with_op_and_tx(
        &self,
        op: StoreOp,
        tx_id: TxId,
    ) -> EncodedVec<ATTR_VEC_SIZE> {
        let mut inner = SmallVec::<[u8; ATTR_VEC_SIZE]>::new();
        inner.extend(tx_id.bytes());
        inner[0] = op as u8;
        self.serialize(&mut Serializer::new(&mut inner)).unwrap();
        EncodedVec { inner }
    }
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data)?)
    }
    pub(crate) fn to_json(&self) -> JsonValue {
        json!({
            "id": self.id.0,
            "name": self.name.to_string(),
            "cardinality": self.cardinality.to_string(),
            "type": self.val_type.to_string(),
            "index": self.indexing.to_string(),
            "history": self.with_history
        })
    }
    pub(crate) fn coerce_value(&self, value: DataValue, ctx: &mut TempIdCtx) -> Result<DataValue> {
        if self.val_type.is_ref_type() {
            if let DataValue::String(s) = value {
                return Ok(ctx.str2tempid(&s, false).to_value());
            }
        }
        self.val_type.coerce_value(value)
    }
}
