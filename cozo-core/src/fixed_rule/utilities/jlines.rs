/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::{fs, io};

use itertools::Itertools;
use log::error;
#[allow(unused_imports)]
use miette::{bail, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
#[cfg(feature = "requests")]
use minreq::Response;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::json::JsonValue;
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{CannotDetermineArity, FixedRule, FixedRulePayload};
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::temp_store::RegularTempStore;

pub(crate) struct JsonReader;

impl FixedRule for JsonReader {
    fn run(
        &self,
        payload: FixedRulePayload<'_, '_>,
        out: &mut RegularTempStore,
        _poison: Poison,
    ) -> Result<()> {
        let url = payload.string_option("url", None)?;
        let json_lines = payload.bool_option("json_lines", Some(true))?;
        let null_if_absent = payload.bool_option("null_if_absent", Some(false))?;
        let prepend_index = payload.bool_option("prepend_index", Some(false))?;

        #[derive(Error, Diagnostic, Debug)]
        #[error("fields specification must be a list of strings")]
        #[diagnostic(code(eval::algo_bad_fields))]
        struct BadFields(#[label] SourceSpan);

        let fields_expr = payload.expr_option("fields", None)?;
        let fields_span = fields_expr.span();
        let fields: Vec<_> = match fields_expr.eval_to_const()? {
            DataValue::List(l) => l
                .into_iter()
                .map(|d| match d {
                    DataValue::Str(s) => Ok(s),
                    _ => Err(BadFields(fields_span)),
                })
                .try_collect()?,
            _ => bail!(BadFields(fields_span)),
        };
        let mut counter = -1i64;
        let mut process_row = |row: &BTreeMap<String, JsonValue>| -> Result<()> {
            let mut ret = if prepend_index {
                counter += 1;
                vec![DataValue::from(counter)]
            } else {
                vec![]
            };
            for field in &fields {
                let val = match row.get(field as &str) {
                    None => {
                        if null_if_absent {
                            DataValue::Null
                        } else {
                            bail!("field {} is absent from JSON line", field);
                        }
                    }
                    Some(v) => DataValue::from(v),
                };
                ret.push(val);
            }
            out.put(ret);
            Ok(())
        };
        match url.strip_prefix("file://") {
            Some(file_path) => {
                if json_lines {
                    let file = File::open(file_path).into_diagnostic()?;
                    for line in io::BufReader::new(file).lines() {
                        let line = line.into_diagnostic()?;
                        let line = line.trim();
                        if !line.is_empty() {
                            let row: BTreeMap<String, JsonValue> =
                                serde_json::from_str(line).into_diagnostic()?;
                            process_row(&row)?;
                        }
                    }
                } else {
                    let content = fs::read_to_string(file_path).into_diagnostic()?;
                    let rows: Vec<BTreeMap<String, JsonValue>> =
                        serde_json::from_str(&content).into_diagnostic()?;
                    for row in &rows {
                        process_row(row)?;
                    }
                }
            }
            None => {
                #[cfg(feature = "requests")]
                {
                    let content = get_file_content_from_url(&url)?;
                    let content = content.as_str().into_diagnostic()?;
                    if json_lines {
                        for line in content.lines() {
                            let line = line.trim();
                            if !line.is_empty() {
                                let row = serde_json::from_str(line).into_diagnostic()?;
                                process_row(&row)?;
                            }
                        }
                    } else {
                        let rows: Vec<BTreeMap<String, JsonValue>> =
                            serde_json::from_str(content).into_diagnostic()?;
                        for row in &rows {
                            process_row(row)?;
                        }
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
        opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
        _rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize> {
        let with_row_num = match opts.get("prepend_index") {
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
                "JsonReader".to_string(),
                "invalid option 'prepend_index' given, expect a boolean".to_string(),
                span
            )),
        };
        let fields = opts.get("fields").ok_or_else(|| {
            CannotDetermineArity(
                "JsonReader".to_string(),
                "option 'fields' not provided".to_string(),
                span,
            )
        })?;
        Ok(match fields.clone().eval_to_const()? {
            DataValue::List(l) => l.len() + with_row_num,
            _ => bail!(CannotDetermineArity(
                "JsonReader".to_string(),
                "invalid option 'fields' given, expect a list".to_string(),
                span
            )),
        })
    }
}

#[cfg(feature = "requests")]
pub(crate) fn get_file_content_from_url(url: &str) -> Result<Response> {
    minreq::get(url as &str)
        .send()
        .map_err(|e| {
            error!("{:?}", e);
            miette!(e)
        })
        .wrap_err_with(|| format!("when requesting URL {url}"))
}
