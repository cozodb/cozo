use itertools::Itertools;
use miette::{bail, Diagnostic, ensure, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::value::{DataValue, UuidWrapper};

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) struct NullableColType {
    pub(crate) coltype: ColType,
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) enum ColType {
    Any,
    Int,
    Float,
    String,
    Bytes,
    Uuid,
    List {
        eltype: Box<NullableColType>,
        len: Option<usize>,
    },
    Tuple(Vec<NullableColType>),
}

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) struct ColumnDef {
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) typing: NullableColType,
    pub(crate) default_gen: Option<Expr>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) struct StoredRelationMetadata {
    pub(crate) keys: Vec<ColumnDef>,
    pub(crate) dependents: Vec<ColumnDef>,
}

impl StoredRelationMetadata {
    pub(crate) fn satisfied_by_required_col(&self, col: &ColumnDef, is_key: bool) -> Result<()> {
        let targets = if is_key { &self.keys } else { &self.dependents };
        for target in targets {
            if target.name == col.name {
                return Ok(())
            }
        }
        if col.default_gen.is_none() {

            #[derive(Debug, Error, Diagnostic)]
            #[error("Required column {0} not provided by input")]
            #[diagnostic(code(eval::required_col_not_provided))]
            struct ColumnNotProvided(String);

            bail!(ColumnNotProvided(col.name.to_string()))
        }
        Ok(())
    }
    pub(crate) fn compatible_with_col(&self, col: &ColumnDef, is_key: bool) -> Result<()> {
        let targets = if is_key { &self.keys } else { &self.dependents };
        for target in targets {
            if target.name == col.name {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Requested column {0} has typing {1:?}, but the requested typing is {2:?}")]
                #[diagnostic(code(eval::col_type_mismatch))]
                struct IncompatibleTyping(String, NullableColType, NullableColType);
                if (!col.typing.nullable || col.typing.coltype != ColType::Any) && target.typing != col.typing {
                    bail!(IncompatibleTyping(col.name.to_string(), target.typing.clone(), col.typing.clone()))
                }

                return Ok(())
            }
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("Required column {0} not found")]
        #[diagnostic(code(eval::required_col_not_found))]
        struct ColumnNotFound(String);

        bail!(ColumnNotFound(col.name.to_string()))
    }
}

impl NullableColType {
    pub(crate) fn coerce(&self, data: DataValue) -> Result<DataValue> {
        if matches!(data, DataValue::Null) {
            return if self.nullable {
                Ok(data)
            } else {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Encountered null value for non-null type {0:?}")]
                #[diagnostic(code(eval::coercion_null))]
                struct InvalidNullValue(ColType);

                Err(InvalidNullValue(self.coltype.clone()).into())
            };
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("Data coercion failed: expected type {0:?}, got value {1:?}")]
        #[diagnostic(code(eval::coercion_failed))]
        struct DataCoercionFailed(ColType, DataValue);

        #[derive(Debug, Error, Diagnostic)]
        #[error("Bad list length: expected datatype {0:?}, got length {1}")]
        #[diagnostic(code(eval::coercion_bad_list_len))]
        struct BadListLength(ColType, usize);

        let make_err = || DataCoercionFailed(self.coltype.clone(), data.clone());

        return Ok(match &self.coltype {
            ColType::Any => data,
            ColType::Int => {
                DataValue::from(data.get_int().ok_or_else(make_err)?)
            }
            ColType::Float => {
                DataValue::from(data.get_float().ok_or_else(make_err)?)
            }
            ColType::String => {
                if matches!(data, DataValue::Str(_)) {
                    data
                } else {
                    bail!(make_err())
                }
            }
            ColType::Bytes => {
                match data {
                    d @ DataValue::Bytes(_) => d,
                    DataValue::Str(s) => {
                        #[derive(Debug, Error, Diagnostic)]
                        #[error("Cannot decode string as base64-encoded bytes: {0}")]
                        #[diagnostic(code(eval::coercion_bad_base_64))]
                        struct BadBase64EncodedString(String);
                        let b = base64::decode(s).map_err(|e| BadBase64EncodedString(e.to_string()))?;
                        DataValue::Bytes(b)
                    }
                    _ => bail!(make_err())
                }
            }
            ColType::Uuid => {
                DataValue::Uuid(UuidWrapper(data.get_uuid().ok_or_else(make_err)?))
            }
            ColType::List { eltype, len } => {
                if let DataValue::List(l) = data {
                    if let Some(expected) = len {
                        ensure!(*expected == l.len(), BadListLength(self.coltype.clone(), l.len()))
                    }
                    DataValue::List(l.into_iter().map(|el| eltype.coerce(el)).try_collect()?)
                } else {
                    bail!(make_err())
                }
            }
            ColType::Tuple(typ) => {
                if let DataValue::List(l) = data {
                    ensure!(typ.len() == l.len(), BadListLength(self.coltype.clone(), l.len()));
                    DataValue::List(l.into_iter().zip(typ.iter()).map(|(el, t)| t.coerce(el)).try_collect()?)
                } else {
                    bail!(make_err())
                }
            }
        });
    }
}