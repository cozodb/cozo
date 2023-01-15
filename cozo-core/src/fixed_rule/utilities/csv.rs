/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use csv::StringRecord;
use miette::{bail, ensure, IntoDiagnostic, Result};
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::functions::{op_to_float, op_to_uuid, TERMINAL_VALIDITY};
use crate::data::program::{FixedRuleOptionNotFoundError, WrongFixedRuleOptionError};
use crate::data::relation::{ColType, NullableColType};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
#[cfg(feature = "requests")]
use crate::fixed_rule::utilities::jlines::get_file_content_from_url;
use crate::fixed_rule::{CannotDetermineArity, FixedRule, FixedRulePayload};
use crate::parse::{parse_type, SourceSpan};
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct CsvReader;

impl FixedRule for CsvReader {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        _poison: Poison,
    ) -> Result<()> {
        let delimiter = payload.string_option("delimiter", Some(","))?;
        let delimiter = delimiter.as_bytes();
        ensure!(
            delimiter.len() == 1,
            WrongFixedRuleOptionError {
                name: "delimiter".to_string(),
                span: payload.span(),
                rule_name: "CsvReader".to_string(),
                help: "'delimiter' must be a single-byte string".to_string()
            }
        );
        let delimiter = delimiter[0];
        let prepend_index = payload.bool_option("prepend_index", Some(false))?;
        let has_headers = payload.bool_option("has_headers", Some(true))?;
        let types_opts = payload.expr_option("types", None)?.eval_to_const()?;
        let typing = NullableColType {
            coltype: ColType::List {
                eltype: Box::new(NullableColType {
                    coltype: ColType::String,
                    nullable: false,
                }),
                len: None,
            },
            nullable: false,
        };
        let types_opts = typing.coerce(types_opts, TERMINAL_VALIDITY.timestamp)?;
        let mut types = vec![];
        for type_str in types_opts.get_slice().unwrap() {
            let type_str = type_str.get_str().unwrap();
            let typ = parse_type(type_str).map_err(|e| WrongFixedRuleOptionError {
                name: "types".to_string(),
                span: payload.span(),
                rule_name: "CsvReader".to_string(),
                help: e.to_string(),
            })?;
            types.push(typ);
        }

        let mut rdr_builder = csv::ReaderBuilder::new();
        rdr_builder
            .delimiter(delimiter)
            .has_headers(has_headers)
            .flexible(true);

        let mut counter = -1i64;
        let out_tuple_size = if prepend_index {
            types.len() + 1
        } else {
            types.len()
        };
        let mut process_row = |row: StringRecord| -> Result<()> {
            let mut out_tuple = Vec::with_capacity(out_tuple_size);
            if prepend_index {
                counter += 1;
                out_tuple.push(DataValue::from(counter));
            }
            for (i, typ) in types.iter().enumerate() {
                match row.get(i) {
                    None => {
                        if typ.nullable {
                            out_tuple.push(DataValue::Null)
                        } else {
                            bail!(
                                "encountered null value when processing CSV when non-null required"
                            )
                        }
                    }
                    Some(s) => {
                        let dv = DataValue::from(s);
                        match &typ.coltype {
                            ColType::Any | ColType::String => out_tuple.push(dv),
                            ColType::Uuid => out_tuple.push(match op_to_uuid(&[dv]) {
                                Ok(uuid) => uuid,
                                Err(err) => {
                                    if typ.nullable {
                                        DataValue::Null
                                    } else {
                                        bail!(err)
                                    }
                                }
                            }),
                            ColType::Float => out_tuple.push(match op_to_float(&[dv]) {
                                Ok(data) => data,
                                Err(err) => {
                                    if typ.nullable {
                                        DataValue::Null
                                    } else {
                                        bail!(err)
                                    }
                                }
                            }),
                            ColType::Int => {
                                let f = op_to_float(&[dv]).unwrap_or(DataValue::Null);
                                match f.get_int() {
                                    None => {
                                        if typ.nullable {
                                            out_tuple.push(DataValue::Null)
                                        } else {
                                            bail!("cannot convert {} to type {}", s, typ)
                                        }
                                    }
                                    Some(i) => out_tuple.push(DataValue::from(i)),
                                };
                            }
                            _ => bail!("cannot convert {} to type {}", s, typ),
                        }
                    }
                }
            }
            out.put(out_tuple);
            Ok(())
        };

        let url = payload.string_option("url", None)?;
        match url.strip_prefix("file://") {
            Some(file_path) => {
                let mut rdr = rdr_builder.from_path(file_path).into_diagnostic()?;
                for record in rdr.records() {
                    let record = record.into_diagnostic()?;
                    process_row(record)?;
                }
            }
            None => {
                #[cfg(feature = "requests")]
                {
                    let content = get_file_content_from_url(&url)?;
                    let mut rdr = rdr_builder.from_reader(content.as_bytes());
                    for record in rdr.records() {
                        let record = record.into_diagnostic()?;
                        process_row(record)?;
                    }
                }
                #[cfg(not(feature = "requests"))]
                bail!("the feature `requests` is not enabled for the build")
            }
        }
        Ok(())
    }

    fn arity(
        &self,
        options: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize> {
        let with_row_num = match options.get("prepend_index") {
            None => 0,
            Some(Expr::Const {
                val: DataValue::Bool(true),
                ..
            }) => 1,
            Some(Expr::Const {
                val: DataValue::Bool(false),
                ..
            }) => 0,
            _ => bail!(CannotDetermineArity(
                "CsvReader".to_string(),
                "invalid option 'prepend_index' given, expect a boolean".to_string(),
                span
            )),
        };
        let columns = options
            .get("types")
            .ok_or_else(|| FixedRuleOptionNotFoundError {
                name: "types".to_string(),
                span,
                rule_name: "CsvReader".to_string(),
            })?;
        let columns = columns.clone().eval_to_const()?;
        if let Some(l) = columns.get_slice() {
            return Ok(l.len() + with_row_num);
        }
        bail!(CannotDetermineArity(
            "CsvReader".to_string(),
            "invalid option 'types' given, expect positive number or list".to_string(),
            span
        ))
    }
}
