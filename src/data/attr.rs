use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use log::error;
use miette::{ensure, Diagnostic, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smallvec::SmallVec;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::encode::EncodedVec;
use crate::data::id::{AttrId, EntityId, TxId, Validity};
use crate::data::triple::StoreOp;
use crate::data::value::{DataValue, Num};
use crate::runtime::transact::SessionTx;
use crate::transact::meta::AttrNotFoundError;
use crate::transact::triple::EntityNotFound;

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
    pub(crate) fn coerce_value(&self, val: DataValue) -> Result<DataValue, DataValue> {
        match self {
            AttributeTyping::Ref => match val {
                DataValue::Num(Num::I(s)) if s > 0 => Ok(DataValue::Num(Num::I(s))),
                val => Err(val),
            },
            AttributeTyping::Bool => {
                if matches!(val, DataValue::Bool(_)) {
                    Ok(val)
                } else {
                    Err(val)
                }
            }
            AttributeTyping::Int => {
                if matches!(val, DataValue::Num(Num::I(_))) {
                    Ok(val)
                } else {
                    Err(val)
                }
            }
            AttributeTyping::Float => match val {
                v @ DataValue::Num(Num::F(_)) => Ok(v),
                DataValue::Num(Num::I(i)) => Ok(DataValue::Num(Num::F(i as f64))),
                val => Err(val),
            },
            AttributeTyping::String => {
                if matches!(val, DataValue::Str(_)) {
                    Ok(val)
                } else {
                    Err(val)
                }
            }
            AttributeTyping::Bytes => {
                if matches!(val, DataValue::Bytes(_)) {
                    Ok(val)
                } else {
                    Err(val)
                }
            }
            AttributeTyping::List => {
                if matches!(val, DataValue::List(_)) {
                    Ok(val)
                } else {
                    Err(val)
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

#[derive(
    Clone, PartialEq, Ord, PartialOrd, Eq, Debug, serde_derive::Deserialize, serde_derive::Serialize,
)]
pub(crate) struct Attribute {
    pub(crate) id: AttrId,
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) cardinality: AttributeCardinality,
    pub(crate) val_type: AttributeTyping,
    pub(crate) indexing: AttributeIndex,
    pub(crate) with_history: bool,
}

impl Default for Attribute {
    fn default() -> Self {
        Self {
            id: AttrId(0),
            name: SmartString::from(""),
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
pub(crate) struct AttrDeserError;

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
                    #[derive(Debug, Error, Diagnostic)]
                    #[error("Cannot find triple with temp ID '{temp_id}'")]
                    #[diagnostic(code(eval::temp_id_not_found))]
                    #[diagnostic(help(
                        "As the attribute {attr_name} is of type 'ref', \
                    the given value is interpreted as a temp id, \
                    but it cannot be found in the input triples."
                    ))]
                    struct TempIdNotFoundError {
                        attr_name: String,
                        temp_id: String,
                    }

                    return Ok(temp_ids
                        .get(s)
                        .ok_or_else(|| TempIdNotFoundError {
                            attr_name: self.name.to_string(),
                            temp_id: s.to_string(),
                        })?
                        .as_datavalue());
                }
                DataValue::List(ls) => {
                    #[derive(Debug, Diagnostic, Error)]
                    #[error("Cannot interpret the list {data:?} as a keyed entity")]
                    #[diagnostic(code(eval::bad_keyed_entity))]
                    #[diagnostic(help("As the attribute {attr_name} is of type 'ref', \
                    the list value is interpreted as a keyed entity, with the first element a string \
                    representing the attribute name, and the second element the keyed value."))]
                    struct BadUniqueKeySpecifierError {
                        data: Vec<DataValue>,
                        attr_name: String,
                    }

                    #[derive(Debug, Diagnostic, Error)]
                    #[error("The attribute {attr_name} is not uniquely indexed")]
                    #[diagnostic(code(eval::non_unique_keyed_entity))]
                    #[diagnostic(help("As the attribute {attr_name} is of type 'ref', and the list \
                    {data:?} is specified as the value, the attribute is required to have a unique index."))]
                    struct NonUniqueKeySpecifierError {
                        data: Vec<DataValue>,
                        attr_name: String,
                    }

                    ensure!(
                        ls.len() == 2,
                        BadUniqueKeySpecifierError {
                            data: ls.clone(),
                            attr_name: self.name.to_string()
                        }
                    );
                    let attr_name = ls.get(0).unwrap().get_string().ok_or_else(|| {
                        BadUniqueKeySpecifierError {
                            data: ls.clone(),
                            attr_name: self.name.to_string(),
                        }
                    })?;
                    let attr = tx
                        .attr_by_name(attr_name)?
                        .ok_or_else(|| AttrNotFoundError(attr_name.to_string()))?;
                    ensure!(
                        attr.indexing.is_unique_index(),
                        NonUniqueKeySpecifierError {
                            data: ls.clone(),
                            attr_name: self.name.to_string()
                        }
                    );
                    let val = attr.coerce_value(ls[1].clone(), temp_ids, tx, vld)?;
                    let eid = tx
                        .eid_by_unique_av(&attr, &val, vld)?
                        .ok_or_else(|| EntityNotFound(format!("{}: {:?}", attr_name, val)))?;
                    return Ok(eid.as_datavalue());
                }
                _ => {}
            }
        }
        self.val_type.coerce_value(value).map_err(|value| {
            ValueCoercionError {
                value,
                typing: self.val_type,
                attr_name: self.name.to_string(),
            }
            .into()
        })
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot coerce value {value:?} to type {typing:?}")]
#[diagnostic(code(eval::type_coercion))]
#[diagnostic(help("This is required by the attribute {attr_name}"))]
struct ValueCoercionError {
    value: DataValue,
    typing: AttributeTyping,
    attr_name: String,
}
