use std::collections::BTreeMap;

use csv::StringRecord;
use miette::{bail, ensure, IntoDiagnostic, Result};
use smartstring::{LazyCompact, SmartString};

use crate::algo::{AlgoImpl, CannotDetermineArity};
use crate::data::expr::Expr;
use crate::data::functions::{op_to_float, op_to_uuid};
use crate::data::program::{
    AlgoOptionNotFoundError, MagicAlgoApply, MagicSymbol, WrongAlgoOptionError,
};
use crate::data::relation::{ColType, NullableColType};
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::{parse_type, SourceSpan};
use crate::runtime::db::Poison;
use crate::runtime::in_mem::InMemRelation;
use crate::runtime::transact::SessionTx;

pub(crate) struct CsvReader;

impl AlgoImpl for CsvReader {
    fn run(
        &mut self,
        _tx: &SessionTx,
        algo: &MagicAlgoApply,
        _stores: &BTreeMap<MagicSymbol, InMemRelation>,
        out: &InMemRelation,
        _poison: Poison,
    ) -> Result<()> {
        let delimiter = algo.string_option("delimiter", Some(","))?;
        let delimiter = delimiter.as_bytes();
        ensure!(
            delimiter.len() == 1,
            WrongAlgoOptionError {
                name: "delimiter".to_string(),
                span: algo.span,
                algo_name: "CsvReader".to_string(),
                help: "'delimiter' must be a single-byte string".to_string()
            }
        );
        let delimiter = delimiter[0];
        let prepend_index = algo.bool_option("prepend_index", Some(false))?;
        let has_headers = algo.bool_option("has_headers", Some(true))?;
        let types_opts = algo.expr_option("types", None)?.eval_to_const()?;
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
        let types_opts = typing.coerce(types_opts)?;
        let mut types = vec![];
        for type_str in types_opts.get_list().unwrap() {
            let type_str = type_str.get_string().unwrap();
            let typ = parse_type(type_str).map_err(|e| WrongAlgoOptionError {
                name: "types".to_string(),
                span: algo.span,
                algo_name: "CsvReader".to_string(),
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
            let mut out_tuple = Tuple(Vec::with_capacity(out_tuple_size));
            if prepend_index {
                counter += 1;
                out_tuple.0.push(DataValue::from(counter));
            }
            for (i, typ) in types.iter().enumerate() {
                match row.get(i) {
                    None => {
                        if typ.nullable {
                            out_tuple.0.push(DataValue::Null)
                        } else {
                            bail!(
                                "encountered null value when processing CSV when non-null required"
                            )
                        }
                    }
                    Some(s) => {
                        let dv = DataValue::Str(SmartString::from(s));
                        match &typ.coltype {
                            ColType::Any | ColType::String => out_tuple.0.push(dv),
                            ColType::Uuid => out_tuple.0.push(match op_to_uuid(&[dv]) {
                                Ok(uuid) => uuid,
                                Err(err) => {
                                    if typ.nullable {
                                        DataValue::Null
                                    } else {
                                        bail!(err)
                                    }
                                }
                            }),
                            ColType::Float => out_tuple.0.push(match op_to_float(&[dv]) {
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
                                            out_tuple.0.push(DataValue::Null)
                                        } else {
                                            bail!("cannot convert {} to type {}", s, typ)
                                        }
                                    }
                                    Some(i) => out_tuple.0.push(DataValue::from(i)),
                                };
                            }
                            _ => bail!("cannot convert {} to type {}", s, typ),
                        }
                    }
                }
            }
            out.put(out_tuple, 0);
            Ok(())
        };

        let url = algo.string_option("url", None)?;
        match url.strip_prefix("file://") {
            Some(file_path) => {
                let mut rdr = rdr_builder.from_path(file_path).into_diagnostic()?;
                for record in rdr.records() {
                    let record = record.into_diagnostic()?;
                    process_row(record)?;
                }
            }
            None => {
                let content = minreq::get(&url as &str).send().into_diagnostic()?;
                let mut rdr = rdr_builder.from_reader(content.as_bytes());
                for record in rdr.records() {
                    let record = record.into_diagnostic()?;
                    process_row(record)?;
                }
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
            .ok_or_else(|| AlgoOptionNotFoundError {
                name: "types".to_string(),
                span,
                algo_name: "CsvReader".to_string(),
            })?;
        let columns = columns.clone().eval_to_const()?;
        if let Some(l) = columns.get_list() {
            return Ok(l.len() + with_row_num);
        }
        bail!(CannotDetermineArity(
            "CsvReader".to_string(),
            "invalid option 'types' given, expect positive number or list".to_string(),
            span
        ))
    }
}
