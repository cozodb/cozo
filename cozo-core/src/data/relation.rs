/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::DateTime;
use std::cmp::Reverse;
use std::fmt::{Display, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::Itertools;
use miette::{bail, ensure, Diagnostic, Result};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::value::{DataValue, UuidWrapper, Validity, ValidityTs};

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) struct NullableColType {
    pub(crate) coltype: ColType,
    pub(crate) nullable: bool,
}

impl Display for NullableColType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.coltype {
            ColType::Any => f.write_str("Any")?,
            ColType::Bool => f.write_str("Bool")?,
            ColType::Int => f.write_str("Int")?,
            ColType::Float => f.write_str("Float")?,
            ColType::String => f.write_str("String")?,
            ColType::Bytes => f.write_str("Bytes")?,
            ColType::Uuid => f.write_str("Uuid")?,
            ColType::Validity => f.write_str("Validity")?,
            ColType::List { eltype, len } => {
                f.write_str("[")?;
                write!(f, "{}", eltype)?;
                if let Some(l) = len {
                    write!(f, ";{}", l)?;
                }
                f.write_str("]")?;
            }
            ColType::Tuple(t) => {
                f.write_str("(")?;
                let l = t.len();
                for (i, el) in t.iter().enumerate() {
                    write!(f, "{}", el)?;
                    if i != l - 1 {
                        f.write_str(",")?
                    }
                }
                f.write_str(")")?;
            }
        }
        if self.nullable {
            f.write_str("?")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
pub(crate) enum ColType {
    Any,
    Bool,
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
    Validity,
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
    pub(crate) non_keys: Vec<ColumnDef>,
}

impl StoredRelationMetadata {
    pub(crate) fn satisfied_by_required_col(&self, col: &ColumnDef, is_key: bool) -> Result<()> {
        let targets = if is_key { &self.keys } else { &self.non_keys };
        for target in targets {
            if target.name == col.name {
                return Ok(());
            }
        }
        if col.default_gen.is_none() {
            #[derive(Debug, Error, Diagnostic)]
            #[error("required column {0} not provided by input")]
            #[diagnostic(code(eval::required_col_not_provided))]
            struct ColumnNotProvided(String);

            bail!(ColumnNotProvided(col.name.to_string()))
        }
        Ok(())
    }
    pub(crate) fn compatible_with_col(&self, col: &ColumnDef, is_key: bool) -> Result<()> {
        let targets = if is_key { &self.keys } else { &self.non_keys };
        for target in targets {
            if target.name == col.name {
                #[derive(Debug, Error, Diagnostic)]
                #[error("requested column {0} has typing {1}, but the requested typing is {2}")]
                #[diagnostic(code(eval::col_type_mismatch))]
                struct IncompatibleTyping(String, NullableColType, NullableColType);
                if (!col.typing.nullable || col.typing.coltype != ColType::Any)
                    && target.typing != col.typing
                {
                    bail!(IncompatibleTyping(
                        col.name.to_string(),
                        target.typing.clone(),
                        col.typing.clone()
                    ))
                }

                return Ok(());
            }
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("required column {0} not found")]
        #[diagnostic(code(eval::required_col_not_found))]
        struct ColumnNotFound(String);

        bail!(ColumnNotFound(col.name.to_string()))
    }
}

impl NullableColType {
    pub(crate) fn coerce(&self, data: DataValue, cur_vld: ValidityTs) -> Result<DataValue> {
        if matches!(data, DataValue::Null) {
            return if self.nullable {
                Ok(data)
            } else {
                #[derive(Debug, Error, Diagnostic)]
                #[error("encountered null value for non-null type {0}")]
                #[diagnostic(code(eval::coercion_null))]
                struct InvalidNullValue(NullableColType);

                Err(InvalidNullValue(self.clone()).into())
            };
        }

        #[derive(Debug, Error, Diagnostic)]
        #[error("data coercion failed: expected type {0}, got value {1:?}")]
        #[diagnostic(code(eval::coercion_failed))]
        struct DataCoercionFailed(NullableColType, DataValue);

        #[derive(Debug, Error, Diagnostic)]
        #[error("bad list length: expected datatype {0}, got length {1}")]
        #[diagnostic(code(eval::coercion_bad_list_len))]
        struct BadListLength(NullableColType, usize);

        let make_err = || DataCoercionFailed(self.clone(), data.clone());

        Ok(match &self.coltype {
            ColType::Any => data,
            ColType::Bool => DataValue::Bool(data.get_bool().ok_or_else(make_err)?),
            ColType::Int => DataValue::from(data.get_int().ok_or_else(make_err)?),
            ColType::Float => DataValue::from(data.get_float().ok_or_else(make_err)?),
            ColType::String => {
                if matches!(data, DataValue::Str(_)) {
                    data
                } else {
                    bail!(make_err())
                }
            }
            ColType::Bytes => match data {
                d @ DataValue::Bytes(_) => d,
                DataValue::Str(s) => {
                    #[derive(Debug, Error, Diagnostic)]
                    #[error("cannot decode string as base64-encoded bytes: {0}")]
                    #[diagnostic(code(eval::coercion_bad_base_64))]
                    struct BadBase64EncodedString(String);
                    let b = base64::decode(s).map_err(|e| BadBase64EncodedString(e.to_string()))?;
                    DataValue::Bytes(b)
                }
                _ => bail!(make_err()),
            },
            ColType::Uuid => DataValue::Uuid(UuidWrapper(data.get_uuid().ok_or_else(make_err)?)),
            ColType::List { eltype, len } => {
                if let DataValue::List(l) = data {
                    if let Some(expected) = len {
                        ensure!(*expected == l.len(), BadListLength(self.clone(), l.len()))
                    }
                    DataValue::List(
                        l.into_iter()
                            .map(|el| eltype.coerce(el, cur_vld))
                            .try_collect()?,
                    )
                } else {
                    bail!(make_err())
                }
            }
            ColType::Tuple(typ) => {
                if let DataValue::List(l) = data {
                    ensure!(typ.len() == l.len(), BadListLength(self.clone(), l.len()));
                    DataValue::List(
                        l.into_iter()
                            .zip(typ.iter())
                            .map(|(el, t)| t.coerce(el, cur_vld))
                            .try_collect()?,
                    )
                } else {
                    bail!(make_err())
                }
            }
            ColType::Validity => {
                #[derive(Debug, Error, Diagnostic)]
                #[error("{0} cannot be coerced into validity")]
                #[diagnostic(code(eval::invalid_validity))]
                struct InvalidValidity(DataValue);

                match data {
                    vld @ DataValue::Validity(_) => vld,
                    DataValue::Str(s) => match &s as &str {
                        "ASSERT" => DataValue::Validity(Validity {
                            timestamp: cur_vld,
                            is_assert: true,
                        }),
                        "RETRACT" => DataValue::Validity(Validity {
                            timestamp: cur_vld,
                            is_assert: false,
                        }),
                        s => {
                            let (is_assert, ts_str) = match s.strip_prefix('~') {
                                None => (true, s),
                                Some(remaining) => (false, remaining),
                            };
                            let dt = DateTime::parse_from_rfc3339(ts_str)
                                .map_err(|_| InvalidValidity(DataValue::Str(s.into())))?;
                            let st: SystemTime = dt.into();
                            let microseconds =
                                st.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

                            DataValue::Validity(Validity {
                                timestamp: ValidityTs(Reverse(microseconds)),
                                is_assert,
                            })
                        }
                    },
                    DataValue::List(l) => {
                        if l.len() == 2 {
                            let o_ts = l[0].get_int();
                            let o_is_assert = l[1].get_bool();
                            if let (Some(ts), Some(is_assert)) = (o_ts, o_is_assert) {
                                return Ok(DataValue::Validity(Validity {
                                    timestamp: ValidityTs(Reverse(ts)),
                                    is_assert,
                                }));
                            }
                        }
                        bail!(InvalidValidity(DataValue::List(l)))
                    }
                    v => bail!(InvalidValidity(v)),
                }
            }
        })
    }
}
