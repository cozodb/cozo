/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use itertools::Itertools;
use miette::{bail, Diagnostic, Result, WrapErr};
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::program::{FixedRuleApply, InputInlineRulesOrFixed, InputProgram, RelationOp};
use crate::data::relation::{ColumnDef, NullableColType};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, ENCODED_KEY_MIN_LEN};
use crate::data::value::{DataValue, ValidityTs};
use crate::fixed_rule::utilities::constant::Constant;
use crate::fixed_rule::FixedRuleHandle;
use crate::parse::parse_script;
use crate::runtime::db::{CallbackCollector, CallbackOp};
use crate::runtime::relation::{AccessLevel, extend_tuple_from_v, InputRelationHandle, InsufficientAccessLevel};
use crate::runtime::transact::SessionTx;
use crate::storage::Storage;
use crate::{Db, decode_tuple_from_kv, NamedRows, StoreTx};

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
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        propagate_triggers: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut to_clear = vec![];
        let mut replaced_old_triggers = None;
        if op == RelationOp::Replace {
            if !propagate_triggers {
                #[derive(Debug, Error, Diagnostic)]
                #[error("replace op in trigger is not allowed: {0}")]
                #[diagnostic(code(eval::replace_in_trigger))]
                struct ReplaceInTrigger(String);
                bail!(ReplaceInTrigger(meta.name.to_string()))

            }
            if let Ok(old_handle) = self.get_relation(&meta.name, true) {
                if !old_handle.indices.is_empty() {
                    #[derive(Debug, Error, Diagnostic)]
                    #[error("cannot replace relation {0} since it has indices")]
                    #[diagnostic(code(eval::replace_rel_with_indices))]
                    struct ReplaceRelationWithIndices(String);
                    bail!(ReplaceRelationWithIndices(old_handle.name.to_string()))
                }
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
                        parse_script(trigger, &Default::default(), &db.algorithms, cur_vld)?
                            .get_single_program()?;

                    let (_, cleanups) = db
                        .run_query(self, program, cur_vld, callback_targets, callback_collector, false)
                        .map_err(|err| {
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

        let is_callback_target = callback_targets.contains(&relation_store.name);

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

                let need_to_collect = !relation_store.is_temp
                    && (is_callback_target
                        || (propagate_triggers && !relation_store.rm_triggers.is_empty()));
                let has_indices = !relation_store.indices.is_empty();
                let mut new_tuples: Vec<DataValue> = vec![];
                let mut old_tuples: Vec<DataValue> = vec![];

                for tuple in res_iter {
                    let extracted = key_extractors
                        .iter()
                        .map(|ex| ex.extract_data(&tuple, cur_vld))
                        .try_collect()?;
                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    if need_to_collect || has_indices {
                        if let Some(existing) = self.store_tx.get(&key, false)? {
                            let mut tup = extracted.clone();
                            if !existing.is_empty() {
                                extend_tuple_from_v(&mut tup, &existing);
                            }
                            if has_indices {
                                for (idx_rel, extractor) in relation_store.indices.values() {
                                    let idx_tup =
                                        extractor.iter().map(|i| tup[*i].clone()).collect_vec();
                                    let encoded = idx_rel
                                        .encode_key_for_store(&idx_tup, Default::default())?;
                                    self.store_tx.del(&encoded)?;
                                }
                            }
                            if need_to_collect {
                                old_tuples.push(DataValue::List(tup));
                            }
                        }
                        if need_to_collect {
                            new_tuples.push(DataValue::List(extracted.clone()));
                        }
                    }
                    if relation_store.is_temp {
                        self.temp_store_tx.del(&key)?;
                    } else {
                        self.store_tx.del(&key)?;
                    }
                }

                // triggers and callbacks
                if need_to_collect && !new_tuples.is_empty() {
                    let k_bindings = relation_store
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
                    let mut kv_bindings = k_bindings.clone();
                    kv_bindings.extend(v_bindings);
                    let kv_bindings = kv_bindings;

                    if propagate_triggers {
                        for trigger in &relation_store.rm_triggers {
                            let mut program =
                                parse_script(trigger, &Default::default(), &db.algorithms, cur_vld)?
                                    .get_single_program()?;

                            make_const_rule(
                                &mut program,
                                "_new",
                                k_bindings.clone(),
                                new_tuples.clone(),
                            );

                            make_const_rule(
                                &mut program,
                                "_old",
                                kv_bindings.clone(),
                                old_tuples.clone(),
                            );

                            let (_, cleanups) = db
                                .run_query(self, program, cur_vld, callback_targets, callback_collector, false)
                                .map_err(|err| {
                                    if err.source_code().is_some() {
                                        err
                                    } else {
                                        err.with_source_code(trigger.to_string())
                                    }
                                })?;
                            to_clear.extend(cleanups);
                        }
                    }

                    if is_callback_target {
                        let target_collector = callback_collector
                            .entry(relation_store.name.clone())
                            .or_default();
                        target_collector.push((
                            CallbackOp::Rm,
                            NamedRows {
                                headers: k_bindings
                                    .into_iter()
                                    .map(|k| k.name.to_string())
                                    .collect_vec(),
                                rows: new_tuples
                                    .into_iter()
                                    .map(|v| match v {
                                        DataValue::List(l) => l,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec(),
                            },
                            NamedRows {
                                headers: kv_bindings
                                    .into_iter()
                                    .map(|k| k.name.to_string())
                                    .collect_vec(),
                                rows: old_tuples
                                    .into_iter()
                                    .map(|v| match v {
                                        DataValue::List(l) => l,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec(),
                            },
                        ))
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
                        .map(|ex| ex.extract_data(&tuple, cur_vld))
                        .try_collect()?;

                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    let val = relation_store.encode_val_for_store(&extracted, *span)?;

                    let existing = if relation_store.is_temp {
                        self.temp_store_tx.get(&key, true)?
                    } else {
                        self.store_tx.get(&key, true)?
                    };
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
                        .map(|ex| ex.extract_data(&tuple, cur_vld))
                        .try_collect()?;
                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    let already_exists = if relation_store.is_temp {
                        self.temp_store_tx.exists(&key, true)?
                    } else {
                        self.store_tx.exists(&key, true)?
                    };
                    if already_exists {
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

                let need_to_collect = !relation_store.is_temp
                    && (is_callback_target
                        || (propagate_triggers && !relation_store.put_triggers.is_empty()));
                let has_indices = !relation_store.indices.is_empty();
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
                        .map(|ex| ex.extract_data(&tuple, cur_vld))
                        .try_collect()?;

                    let key = relation_store.encode_key_for_store(&extracted, *span)?;
                    let val = relation_store.encode_val_for_store(&extracted, *span)?;

                    if need_to_collect || has_indices {
                        if let Some(existing) = self.store_tx.get(&key, false)? {
                            let mut tup = extracted.clone();
                            if !existing.is_empty() {
                                extend_tuple_from_v(&mut tup, &existing);
                            }
                            if has_indices {
                                if extracted != tup {
                                    for (idx_rel, extractor) in relation_store.indices.values() {
                                        let idx_tup_old =
                                            extractor.iter().map(|i| tup[*i].clone()).collect_vec();
                                        let encoded_old = idx_rel.encode_key_for_store(
                                            &idx_tup_old,
                                            Default::default(),
                                        )?;
                                        self.store_tx.del(&encoded_old)?;

                                        let idx_tup_new = extractor
                                            .iter()
                                            .map(|i| extracted[*i].clone())
                                            .collect_vec();
                                        let encoded_new = idx_rel.encode_key_for_store(
                                            &idx_tup_new,
                                            Default::default(),
                                        )?;
                                        self.store_tx.put(&encoded_new, &[])?;
                                    }
                                }
                            }
                            if need_to_collect {
                                old_tuples.push(DataValue::List(tup));
                            }
                        } else if has_indices {
                            for (idx_rel, extractor) in relation_store.indices.values() {
                                let idx_tup_new = extractor
                                    .iter()
                                    .map(|i| extracted[*i].clone())
                                    .collect_vec();
                                let encoded_new = idx_rel
                                    .encode_key_for_store(&idx_tup_new, Default::default())?;
                                self.store_tx.put(&encoded_new, &[])?;
                            }
                        }

                        if need_to_collect {
                            new_tuples.push(DataValue::List(extracted));
                        }
                    }

                    if relation_store.is_temp {
                        self.temp_store_tx.put(&key, &val)?;
                    } else {
                        self.store_tx.put(&key, &val)?;
                    }
                }

                if need_to_collect && !new_tuples.is_empty() {
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

                    let kv_bindings = bindings;
                    if propagate_triggers {
                        for trigger in &relation_store.put_triggers {
                            let mut program =
                                parse_script(trigger, &Default::default(), &db.algorithms, cur_vld)?
                                    .get_single_program()?;

                            make_const_rule(
                                &mut program,
                                "_new",
                                kv_bindings.clone(),
                                new_tuples.clone(),
                            );
                            make_const_rule(
                                &mut program,
                                "_old",
                                kv_bindings.clone(),
                                old_tuples.clone(),
                            );

                            let (_, cleanups) = db
                                .run_query(self, program, cur_vld, callback_targets, callback_collector, false)
                                .map_err(|err| {
                                    if err.source_code().is_some() {
                                        err
                                    } else {
                                        err.with_source_code(trigger.to_string())
                                    }
                                })?;
                            to_clear.extend(cleanups);
                        }
                    }

                    if is_callback_target {
                        let target_collector = callback_collector
                            .entry(relation_store.name.clone())
                            .or_default();
                        let headers = kv_bindings
                            .into_iter()
                            .map(|k| k.name.to_string())
                            .collect_vec();
                        target_collector.push((
                            CallbackOp::Put,
                            NamedRows {
                                headers: headers.clone(),
                                rows: new_tuples
                                    .into_iter()
                                    .map(|v| match v {
                                        DataValue::List(l) => l,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec(),
                            },
                            NamedRows {
                                headers,
                                rows: old_tuples
                                    .into_iter()
                                    .map(|v| match v {
                                        DataValue::List(l) => l,
                                        _ => unreachable!(),
                                    })
                                    .collect_vec(),
                            },
                        ))
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
    fn extract_data(&self, tuple: &Tuple, cur_vld: ValidityTs) -> Result<DataValue> {
        Ok(match self {
            DataExtractor::DefaultExtractor(expr, typ) => typ
                .coerce(expr.clone().eval_to_const()?, cur_vld)
                .wrap_err_with(|| format!("when processing tuple {tuple:?}"))?,
            DataExtractor::IndexExtractor(i, typ) => typ
                .coerce(tuple[*i].clone(), cur_vld)
                .wrap_err_with(|| format!("when processing tuple {tuple:?}"))?,
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
        InputInlineRulesOrFixed::Fixed {
            fixed: FixedRuleApply {
                fixed_handle: FixedRuleHandle {
                    name: Symbol::new("Constant", Default::default()),
                },
                rule_args: vec![],
                options: Arc::new(options),
                head: bindings,
                arity: bindings_arity,
                span: Default::default(),
                fixed_impl: Arc::new(Box::new(Constant)),
            },
        },
    );
}
