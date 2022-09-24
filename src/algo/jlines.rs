use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufRead;
use std::{fs, io};

use itertools::Itertools;
use miette::{bail, miette, Diagnostic, IntoDiagnostic, Result};
use thiserror::Error;

use crate::algo::AlgoImpl;
use crate::data::json::JsonValue;
use crate::data::program::{MagicAlgoApply, MagicSymbol};
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::db::Poison;
use crate::runtime::derived::DerivedRelStore;
use crate::runtime::transact::SessionTx;

pub(crate) struct JsonReader;

impl AlgoImpl for JsonReader {
    fn run(
        &mut self,
        _tx: &SessionTx,
        algo: &MagicAlgoApply,
        _stores: &BTreeMap<MagicSymbol, DerivedRelStore>,
        out: &DerivedRelStore,
        _poison: Poison,
    ) -> Result<()> {
        let url = algo.string_option("url", None)?;
        let json_lines = algo.bool_option("json_lines", Some(true))?;
        let null_if_absent = algo.bool_option("null_if_absent", Some(false))?;
        let prepend_index = algo.bool_option("prepend_index", Some(false))?;

        #[derive(Error, Diagnostic, Debug)]
        #[error("fields specification must be a list of strings")]
        #[diagnostic(code(eval::algo_bad_fields))]
        struct BadFields(#[label] SourceSpan);

        let fields_expr = algo.expr_option("fields", None)?;
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
        let mut process_row = |row: &JsonValue| -> Result<()> {
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
            out.put(Tuple(ret), 0);
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
                            let row = serde_json::from_str(&line).into_diagnostic()?;
                            process_row(&row)?;
                        }
                    }
                } else {
                    let content = fs::read_to_string(file_path).into_diagnostic()?;
                    let data: JsonValue = serde_json::from_str(&content).into_diagnostic()?;
                    let rows = data
                        .as_array()
                        .ok_or_else(|| miette!("JSON file is not an array"))?;
                    for row in rows {
                        process_row(row)?;
                    }
                }
            }
            None => {
                let content = reqwest::blocking::get(&url as &str)
                    .into_diagnostic()?
                    .text()
                    .into_diagnostic()?;
                if json_lines {
                    for line in content.lines() {
                        let line = line.trim();
                        if !line.is_empty() {
                            let row = serde_json::from_str(&line).into_diagnostic()?;
                            process_row(&row)?;
                        }
                    }
                } else {
                    let data: JsonValue = serde_json::from_str(&content).into_diagnostic()?;
                    let rows = data
                        .as_array()
                        .ok_or_else(|| miette!("JSON file is not an array"))?;
                    for row in rows {
                        process_row(row)?;
                    }
                }
            }
        }
        Ok(())
    }
}
