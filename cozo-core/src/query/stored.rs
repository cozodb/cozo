/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use itertools::Itertools;
use miette::{bail, Diagnostic, Result, WrapErr};
use smartstring::SmartString;
use thiserror::Error;

use crate::algo::constant::Constant;
use crate::algo::AlgoHandle;
use crate::data::expr::Expr;
use crate::data::program::{AlgoApply, InputInlineRulesOrAlgo, InputProgram, RelationOp};
use crate::data::relation::{ColumnDef, NullableColType};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, ENCODED_KEY_MIN_LEN};
use crate::data::value::DataValue;
use crate::parse::parse_script;
use crate::runtime::relation::{AccessLevel, InputRelationHandle, InsufficientAccessLevel};
use crate::runtime::transact::SessionTx;
use crate::storage::Storage;
use crate::Db;

#[derive(Debug, Error, Diagnostic)]
#[error("attempting to write into relation {0} of arity {1} with data of arity {2}")]
#[diagnostic(code(eval::relation_arity_mismatch))]
struct RelationArityMismatch(String, usize, usize);

impl<'a> SessionTx<'a> {
    pub(crate) fn execute_relation<'s, S: Storage<'s>>(
        &mut self,
        db: &Db<S>,
        res_iter: impl Iterator<Item = Tuple>,
        op: RelationOp,
        meta: &InputRelationHandle,
        headers: &[Symbol],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut to_clear = vec![];
        let mut replaced_old_triggers = None;
        if op == RelationOp::Replace {
            if let Ok(old_handle) = self.get_relation(&meta.name, true) {
                if old_handle.access_level < AccessLevel::Normal {
                    bail!(InsufficientAccessLevel(
                        old_handle.name.to_string(),
                        "relation replacement".to_string(),
                        old_handle.access_level
                    ));
                }
                if old_handle.has_triggers() {
                    replaced_old_triggers = Some((old_handle.put_triggers, old_handle.rm_triggers))
                }
                for trigger in &old_handle.replace_triggers {
                    let program =
                        parse_script(trigger, &Default::default())?.get_single_program()?;

                    let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                        if err.source_code().is_some() {
                            err
                        } else {
                            err.with_source_code(trigger.to_string())
                        }
                    })?;
                    to_clear.extend(cleanups);
                }

                to_clear.push(self.destroy_relation(&meta.name)?);
            }
        }
        let mut relation_store = if op == RelationOp::Replace || op == RelationOp::Create {
            self.create_relation(meta.clone())?
        } else {
            self.get_relation(&meta.name, false)?
        };
        if let Some((old_put, old_retract)) = replaced_old_triggers {
            relation_store.put_triggers = old_put;
            relation_store.rm_triggers = old_retract;
        }
        let InputRelationHandle {
            metadata,
            key_bindings,
            dep_bindings,
            span,
            ..
        } = meta;

        match op {
            RelationOp::Rm => {
                if relation_store.access_level < AccessLevel::Protected {
                    bail!(InsufficientAccessLevel(
                        relation_store.name.to_string(),
                        "row removal".to_string(),
                        relation_store.access_level
                    ));
                }
                let key_extractors = make_extractors(
                    &relation_store.metadata.keys,
                    &metadata.keys,
                    key_bindings,
                    headers,
                )?;

                let has_triggers = !relation_store.rm_triggers.is_empty();
                let mut new_tuples: Vec<DataValue> = vec![];
                let mut old_tuples: Vec<DataValue> = vec![];

                for tuple in res_iter {
                    let extracted = key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?;
                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    if has_triggers {
                        if let Some(existing) = self.store_tx.get(&key, false)? {
                            let mut tup = extracted.clone();
                            if !existing.is_empty() {
                                let mut remaining = &existing[ENCODED_KEY_MIN_LEN..];
                                while !remaining.is_empty() {
                                    let (val, nxt) = DataValue::decode_from_key(remaining);
                                    tup.push(val);
                                    remaining = nxt;
                                }
                            }
                            old_tuples.push(DataValue::List(tup));
                        }
                        new_tuples.push(DataValue::List(extracted.clone()));
                    }
                    self.store_tx.del(&key)?;
                }

                if has_triggers && !new_tuples.is_empty() {
                    for trigger in &relation_store.rm_triggers {
                        let mut program =
                            parse_script(trigger, &Default::default())?.get_single_program()?;

                        let mut bindings = relation_store
                            .metadata
                            .keys
                            .iter()
                            .map(|k| Symbol::new(k.name.clone(), Default::default()))
                            .collect_vec();

                        make_const_rule(&mut program, "_new", bindings.clone(), new_tuples.clone());

                        let v_bindings = relation_store
                            .metadata
                            .non_keys
                            .iter()
                            .map(|k| Symbol::new(k.name.clone(), Default::default()));
                        bindings.extend(v_bindings);

                        make_const_rule(&mut program, "_old", bindings, old_tuples.clone());

                        let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                            if err.source_code().is_some() {
                                err
                            } else {
                                err.with_source_code(trigger.to_string())
                            }
                        })?;
                        to_clear.extend(cleanups);
                    }
                }
            }
            RelationOp::Ensure => {
                if relation_store.access_level < AccessLevel::ReadOnly {
                    bail!(InsufficientAccessLevel(
                        relation_store.name.to_string(),
                        "row check".to_string(),
                        relation_store.access_level
                    ));
                }

                let mut key_extractors = make_extractors(
                    &relation_store.metadata.keys,
                    &metadata.keys,
                    key_bindings,
                    headers,
                )?;

                let val_extractors = make_extractors(
                    &relation_store.metadata.non_keys,
                    &metadata.non_keys,
                    dep_bindings,
                    headers,
                )?;
                key_extractors.extend(val_extractors);

                for tuple in res_iter {
                    let extracted = key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?;

                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    let val = relation_store.encode_val_for_store(&extracted, *span)?;

                    let existing = self.store_tx.get(&key, true)?;
                    match existing {
                        None => {
                            bail!(TransactAssertionFailure {
                                relation: relation_store.name.to_string(),
                                key: extracted,
                                notice: "key does not exist in database".to_string()
                            })
                        }
                        Some(v) => {
                            if &v as &[u8] != &val as &[u8] {
                                bail!(TransactAssertionFailure {
                                    relation: relation_store.name.to_string(),
                                    key: extracted,
                                    notice: "key exists in database, but value does not match"
                                        .to_string()
                                })
                            }
                        }
                    }
                }
            }
            RelationOp::EnsureNot => {
                if relation_store.access_level < AccessLevel::ReadOnly {
                    bail!(InsufficientAccessLevel(
                        relation_store.name.to_string(),
                        "row check".to_string(),
                        relation_store.access_level
                    ));
                }

                let key_extractors = make_extractors(
                    &relation_store.metadata.keys,
                    &metadata.keys,
                    key_bindings,
                    headers,
                )?;

                for tuple in res_iter {
                    let extracted = key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?;
                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    if self.store_tx.exists(&key, true)? {
                        bail!(TransactAssertionFailure {
                            relation: relation_store.name.to_string(),
                            key: extracted,
                            notice: "key exists in database".to_string()
                        })
                    }
                }
            }
            RelationOp::Create | RelationOp::Replace | RelationOp::Put => {
                if relation_store.access_level < AccessLevel::Protected {
                    bail!(InsufficientAccessLevel(
                        relation_store.name.to_string(),
                        "row insertion".to_string(),
                        relation_store.access_level
                    ));
                }

                let mut key_extractors = make_extractors(
                    &relation_store.metadata.keys,
                    &metadata.keys,
                    key_bindings,
                    headers,
                )?;

                let has_triggers = !relation_store.put_triggers.is_empty();
                let mut new_tuples: Vec<DataValue> = vec![];
                let mut old_tuples: Vec<DataValue> = vec![];

                let val_extractors = make_extractors(
                    &relation_store.metadata.non_keys,
                    &metadata.non_keys,
                    dep_bindings,
                    headers,
                )?;
                key_extractors.extend(val_extractors);

                for tuple in res_iter {
                    let extracted = key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple))
                        .try_collect()?;

                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    let val = relation_store.encode_val_for_store(&extracted, *span)?;

                    if has_triggers {
                        if let Some(existing) = self.store_tx.get(&key, false)? {
                            let mut tup = extracted.clone();
                            let mut remaining = &existing[ENCODED_KEY_MIN_LEN..];
                            while !remaining.is_empty() {
                                let (val, nxt) = DataValue::decode_from_key(remaining);
                                tup.push(val);
                                remaining = nxt;
                            }
                            old_tuples.push(DataValue::List(tup));
                        }

                        new_tuples.push(DataValue::List(extracted));
                    }

                    self.store_tx.put(&key, &val)?;
                }

                if has_triggers && !new_tuples.is_empty() {
                    for trigger in &relation_store.put_triggers {
                        let mut program =
                            parse_script(trigger, &Default::default())?.get_single_program()?;

                        let mut bindings = relation_store
                            .metadata
                            .keys
                            .iter()
                            .map(|k| Symbol::new(k.name.clone(), Default::default()))
                            .collect_vec();
                        let v_bindings = relation_store
                            .metadata
                            .non_keys
                            .iter()
                            .map(|k| Symbol::new(k.name.clone(), Default::default()));
                        bindings.extend(v_bindings);

                        make_const_rule(&mut program, "_new", bindings.clone(), new_tuples.clone());
                        make_const_rule(&mut program, "_old", bindings, old_tuples.clone());

                        let (_, cleanups) = db.run_query(self, program).map_err(|err| {
                            if err.source_code().is_some() {
                                err
                            } else {
                                err.with_source_code(trigger.to_string())
                            }
                        })?;
                        to_clear.extend(cleanups);
                    }
                }
            }
        };

        Ok(to_clear)
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Assertion failure for {key:?} of {relation}: {notice}")]
struct TransactAssertionFailure {
    relation: String,
    key: Vec<DataValue>,
    notice: String,
}

enum DataExtractor {
    DefaultExtractor(Expr, NullableColType),
    IndexExtractor(usize, NullableColType),
}

impl DataExtractor {
    fn extract_data(&self, tuple: &Tuple) -> Result<DataValue> {
        Ok(match self {
            DataExtractor::DefaultExtractor(expr, typ) => typ
                .coerce(expr.clone().eval_to_const()?)
                .wrap_err_with(|| format!("when processing tuple {:?}", tuple))?,
            DataExtractor::IndexExtractor(i, typ) => typ
                .coerce(tuple[*i].clone())
                .wrap_err_with(|| format!("when processing tuple {:?}", tuple))?,
        })
    }
}

fn make_extractors(
    stored: &[ColumnDef],
    input: &[ColumnDef],
    bindings: &[Symbol],
    tuple_headers: &[Symbol],
) -> Result<Vec<DataExtractor>> {
    stored
        .iter()
        .map(|s| make_extractor(s, input, bindings, tuple_headers))
        .try_collect()
}

fn make_extractor(
    stored: &ColumnDef,
    input: &[ColumnDef],
    bindings: &[Symbol],
    tuple_headers: &[Symbol],
) -> Result<DataExtractor> {
    for (inp_col, inp_binding) in input.iter().zip(bindings.iter()) {
        if inp_col.name == stored.name {
            for (idx, tuple_head) in tuple_headers.iter().enumerate() {
                if tuple_head == inp_binding {
                    return Ok(DataExtractor::IndexExtractor(idx, stored.typing.clone()));
                }
            }
        }
    }
    if let Some(expr) = &stored.default_gen {
        Ok(DataExtractor::DefaultExtractor(
            expr.clone(),
            stored.typing.clone(),
        ))
    } else {
        #[derive(Debug, Error, Diagnostic)]
        #[error("cannot make extractor for column {0}")]
        #[diagnostic(code(eval::unable_to_make_extractor))]
        struct UnableToMakeExtractor(String);
        Err(UnableToMakeExtractor(stored.name.to_string()).into())
    }
}

fn make_const_rule(
    program: &mut InputProgram,
    rule_name: &str,
    bindings: Vec<Symbol>,
    data: Vec<DataValue>,
) {
    let rule_symbol = Symbol::new(SmartString::from(rule_name), Default::default());
    let mut options = BTreeMap::new();
    options.insert(
        SmartString::from("data"),
        Expr::Const {
            val: DataValue::List(data),
            span: Default::default(),
        },
    );
    let bindings_arity = bindings.len();
    program.prog.insert(
        rule_symbol,
        InputInlineRulesOrAlgo::Algo {
            algo: AlgoApply {
                algo: AlgoHandle {
                    name: Symbol::new("Constant", Default::default()),
                },
                rule_args: vec![],
                options,
                head: bindings,
                arity: bindings_arity,
                span: Default::default(),
                algo_impl: Box::new(Constant),
            },
        },
    );
}
