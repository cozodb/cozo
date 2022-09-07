use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use log::error;
use miette::{bail, ensure, miette, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};

use crate::data::encode::EncodedVec;
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::symb::Symbol;
use crate::data::triple::StoreOp;
use crate::data::value::{DataValue, Num};
use crate::runtime::transact::SessionTx;

// use crate::parse::triple::TempIdCtx;

#[repr(u8)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Ord,
    PartialOrd,
    Eq,
    Debug,
    serde_derive::Deserialize,
    serde_derive::Serialize,
)]
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
    type Error = miette::Error;
    fn try_from(value: &'_ str) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            "one" => AttributeCardinality::One,
            "many" => AttributeCardinality::Many,
            s => bail!("unknown cardinality {}", s),
        })
    }
}

#[repr(u8)]
#[derive(
    Copy,
    Clone,
    PartialEq,
    Ord,
    PartialOrd,
    Eq,
    Debug,
    serde_derive::Deserialize,
    serde_derive::Serialize,
)]
pub(crate) enum AttributeTyping {
    Ref = 1,
    // Component = 2,
    Bool = 3,
    Int = 4,
    Float = 5,
    String = 6,
    Bytes = 9,
    List = 10,
}

impl AttributeTyping {
    pub(crate) fn is_ref_type(&self) -> bool {
        matches!(self, AttributeTyping::Ref)
    }
}

impl Display for AttributeTyping {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AttributeTyping::Ref => write!(f, "ref"),
            AttributeTyping::Bool => write!(f, "bool"),
            AttributeTyping::Int => write!(f, "int"),
            AttributeTyping::Float => write!(f, "float"),
            AttributeTyping::String => write!(f, "string"),
            AttributeTyping::Bytes => write!(f, "bytes"),
            AttributeTyping::List => write!(f, "list"),
        }
    }
}

impl AttributeTyping {
    fn type_err(&self, val: DataValue) -> miette::Error {
        miette!("cannot coerce {:?} to {:?}", val, self)
    }
    pub(crate) fn coerce_value(&self, val: DataValue) -> Result<DataValue> {
        match self {
            AttributeTyping::Ref => match val {
                DataValue::Num(Num::I(s)) if s > 0 => Ok(DataValue::Num(Num::I(s))),
                val => Err(self.type_err(val)),
            },
            AttributeTyping::Bool => {
                if matches!(val, DataValue::Bool(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val))
                }
            }
            AttributeTyping::Int => {
                if matches!(val, DataValue::Num(Num::I(_))) {
                    Ok(val)
                } else {
                    Err(self.type_err(val))
                }
            }
            AttributeTyping::Float => match val {
                v @ DataValue::Num(Num::F(_)) => Ok(v),
                DataValue::Num(Num::I(i)) => Ok(DataValue::Num(Num::F(i as f64))),
                val => Err(self.type_err(val)),
            },
            AttributeTyping::String => {
                if matches!(val, DataValue::Str(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val))
                }
            }
            AttributeTyping::Bytes => {
                if matches!(val, DataValue::Bytes(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val))
                }
            }
            AttributeTyping::List => {
                if matches!(val, DataValue::List(_)) {
                    Ok(val)
                } else {
                    Err(self.type_err(val))
                }
            }
        }
    }
}

#[repr(u8)]
#[derive(
    Clone, PartialEq, Ord, PartialOrd, Eq, Debug, serde_derive::Deserialize, serde_derive::Serialize,
)]
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
    type Error = miette::Error;
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

#[derive(
    Clone, PartialEq, Ord, PartialOrd, Eq, Debug, serde_derive::Deserialize, serde_derive::Serialize,
)]
pub(crate) struct Attribute {
    pub(crate) id: AttrId,
    pub(crate) name: Symbol,
    pub(crate) cardinality: AttributeCardinality,
    pub(crate) val_type: AttributeTyping,
    pub(crate) indexing: AttributeIndex,
    pub(crate) with_history: bool,
}

impl Default for Attribute {
    fn default() -> Self {
        Self {
            id: AttrId(0),
            name: Symbol::from(""),
            cardinality: AttributeCardinality::One,
            val_type: AttributeTyping::Ref,
            indexing: AttributeIndex::None,
            with_history: false,
        }
    }
}

const ATTR_VEC_SIZE: usize = 80;

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
#[error("Cannot deserialize attribute")]
#[diagnostic(code(deser::attr))]
#[diagnostic(help("This could indicate a bug. Consider file a bug report."))]
struct AttrDeserError;

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
        Ok(rmp_serde::from_slice(data).map_err(|_| {
            error!("Cannot deserialize attribute from bytes: {:x?}", data);
            AttrDeserError
        })?)
    }
    pub(crate) fn coerce_value(
        &self,
        value: DataValue,
        temp_ids: &BTreeMap<SmartString<LazyCompact>, EntityId>,
        tx: &SessionTx,
        vld: Validity,
    ) -> Result<DataValue> {
        if self.val_type.is_ref_type() {
            match &value {
                DataValue::Str(s) => {
                    return Ok(temp_ids
                        .get(s)
                        .ok_or_else(|| miette!("required tempid {} not found", s))?
                        .as_datavalue());
                }
                DataValue::List(ls) => {
                    ensure!(
                        ls.len() == 2,
                        "list specifier for ref types must have length 2"
                    );
                    let attr_name = ls[0]
                        .get_string()
                        .ok_or_else(|| miette!("list specifier requires first argument string"))?;
                    let attr = tx
                        .attr_by_name(&Symbol::from(attr_name))?
                        .ok_or_else(|| miette!("attribute not found: {}", attr_name))?;
                    ensure!(
                        attr.indexing.is_unique_index(),
                        "ref type list specifier requires unique index"
                    );
                    let val = attr.coerce_value(ls[1].clone(), temp_ids, tx, vld)?;
                    let eid = tx.eid_by_unique_av(&attr, &val, vld)?.ok_or_else(|| {
                        miette!("entity not found for attr val {} {:?}", attr_name, val)
                    })?;
                    return Ok(eid.as_datavalue());
                }
                _ => {}
            }
        }
        self.val_type.coerce_value(value)
    }
}
