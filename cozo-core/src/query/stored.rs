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
use miette::{bail, Diagnostic, IntoDiagnostic, Result, WrapErr};
use pest::Parser;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::expr::{Bytecode, Expr};
use crate::data::program::{FixedRuleApply, InputInlineRulesOrFixed, InputProgram, RelationOp};
use crate::data::relation::{ColumnDef, NullableColType, StoredRelationMetadata};
use crate::data::symb::Symbol;
use crate::data::tuple::{Tuple, ENCODED_KEY_MIN_LEN};
use crate::data::value::{DataValue, ValidityTs};
use crate::fixed_rule::utilities::constant::Constant;
use crate::fixed_rule::FixedRuleHandle;
use crate::fts::tokenizer::TextAnalyzer;
use crate::parse::expr::build_expr;
use crate::parse::{parse_script, CozoScriptParser, Rule};
use crate::runtime::callback::{CallbackCollector, CallbackOp};
use crate::runtime::minhash_lsh::HashPermutations;
use crate::runtime::relation::{
    extend_tuple_from_v, AccessLevel, InputRelationHandle, InsufficientAccessLevel, RelationHandle,
};
use crate::runtime::transact::SessionTx;
use crate::storage::Storage;
use crate::{Db, NamedRows, SourceSpan, StoreTx};

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
        force_collect: &str,
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
                    let program = parse_script(
                        trigger,
                        &Default::default(),
                        &db.fixed_rules.read().unwrap(),
                        cur_vld,
                    )?
                    .get_single_program()?;

                    let (_, cleanups) = db
                        .run_query(
                            self,
                            program,
                            cur_vld,
                            callback_targets,
                            callback_collector,
                            false,
                        )
                        .map_err(|err| {
                            if err.source_code().is_some() {
                                err
                            } else {
                                err.with_source_code(format!("{trigger}"))
                            }
                        })?;
                    to_clear.extend(cleanups);
                }
                let destroy_res = self.destroy_relation(&meta.name)?;
                if !meta.name.is_temp_store_name() {
                    to_clear.extend(destroy_res);
                }
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
            RelationOp::Rm | RelationOp::Delete => self.remove_from_relation(
                db,
                res_iter,
                headers,
                cur_vld,
                callback_targets,
                callback_collector,
                propagate_triggers,
                &mut to_clear,
                &relation_store,
                metadata,
                key_bindings,
                op == RelationOp::Delete,
                force_collect,
                *span,
            )?,
            RelationOp::Ensure => self.ensure_in_relation(
                res_iter,
                headers,
                cur_vld,
                &relation_store,
                metadata,
                key_bindings,
                *span,
            )?,
            RelationOp::EnsureNot => self.ensure_not_in_relation(
                res_iter,
                headers,
                cur_vld,
                &relation_store,
                metadata,
                key_bindings,
                *span,
            )?,
            RelationOp::Update => self.update_in_relation(
                db,
                res_iter,
                headers,
                cur_vld,
                callback_targets,
                callback_collector,
                propagate_triggers,
                &mut to_clear,
                &relation_store,
                metadata,
                key_bindings,
                force_collect,
                *span,
            )?,
            RelationOp::Create | RelationOp::Replace | RelationOp::Put | RelationOp::Insert => self
                .put_into_relation(
                    db,
                    res_iter,
                    headers,
                    cur_vld,
                    callback_targets,
                    callback_collector,
                    propagate_triggers,
                    &mut to_clear,
                    &relation_store,
                    metadata,
                    key_bindings,
                    dep_bindings,
                    op == RelationOp::Insert,
                    force_collect,
                    *span,
                )?,
        };

        Ok(to_clear)
    }

    fn put_into_relation<'s, S: Storage<'s>>(
        &mut self,
        db: &Db<S>,
        res_iter: impl Iterator<Item = Tuple>,
        headers: &[Symbol],
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        propagate_triggers: bool,
        to_clear: &mut Vec<(Vec<u8>, Vec<u8>)>,
        relation_store: &RelationHandle,
        metadata: &StoredRelationMetadata,
        key_bindings: &[Symbol],
        dep_bindings: &[Symbol],
        is_insert: bool,
        force_collect: &str,
        span: SourceSpan,
    ) -> Result<()> {
        let is_callback_target = callback_targets.contains(&relation_store.name)
            || force_collect == relation_store.name;

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

        let need_to_collect = !force_collect.is_empty()
            || (!relation_store.is_temp
                && (is_callback_target
                    || (propagate_triggers && !relation_store.put_triggers.is_empty())));
        let has_indices = !relation_store.indices.is_empty();
        let has_hnsw_indices = !relation_store.hnsw_indices.is_empty();
        let has_fts_indices = !relation_store.fts_indices.is_empty();
        let has_lsh_indices = !relation_store.lsh_indices.is_empty();
        let mut new_tuples: Vec<DataValue> = vec![];
        let mut old_tuples: Vec<DataValue> = vec![];

        let val_extractors = if metadata.non_keys.is_empty() {
            make_extractors(
                &relation_store.metadata.non_keys,
                &metadata.keys,
                key_bindings,
                headers,
            )?
        } else {
            make_extractors(
                &relation_store.metadata.non_keys,
                &metadata.non_keys,
                dep_bindings,
                headers,
            )?
        };
        key_extractors.extend(val_extractors);
        let mut stack = vec![];
        let hnsw_filters = Self::make_hnsw_filters(relation_store)?;
        let fts_lsh_processors = self.make_fts_lsh_processors(relation_store)?;
        let lsh_perms = self.make_lsh_hash_perms(relation_store);

        for tuple in res_iter {
            let extracted: Vec<DataValue> = key_extractors
                .iter()
                .map(|ex| ex.extract_data(&tuple, cur_vld))
                .try_collect()?;

            let key = relation_store.encode_key_for_store(&extracted, span)?;

            if is_insert {
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
                    });
                }
            }

            let val = relation_store.encode_val_for_store(&extracted, span)?;

            if need_to_collect
                || has_indices
                || has_hnsw_indices
                || has_fts_indices
                || has_lsh_indices
            {
                if let Some(existing) = self.store_tx.get(&key, false)? {
                    let mut tup = extracted[0..relation_store.metadata.keys.len()].to_vec();
                    extend_tuple_from_v(&mut tup, &existing);
                    if has_indices && extracted != tup {
                        self.update_in_index(relation_store, &extracted, &tup)?;
                        self.del_in_fts(relation_store, &mut stack, &fts_lsh_processors, &tup)?;
                        self.del_in_lsh(relation_store, &tup)?;
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
                        let encoded_new =
                            idx_rel.encode_key_for_store(&idx_tup_new, Default::default())?;
                        self.store_tx.put(&encoded_new, &[])?;
                    }
                }

                self.update_in_hnsw(relation_store, &mut stack, &hnsw_filters, &extracted)?;
                self.put_in_fts(relation_store, &mut stack, &fts_lsh_processors, &extracted)?;
                self.put_in_lsh(
                    relation_store,
                    &mut stack,
                    &fts_lsh_processors,
                    &extracted,
                    &lsh_perms,
                )?;

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
            self.collect_mutations(
                db,
                cur_vld,
                callback_targets,
                callback_collector,
                propagate_triggers,
                to_clear,
                relation_store,
                is_callback_target,
                new_tuples,
                old_tuples,
            )?;
        }
        Ok(())
    }

    fn put_in_fts(
        &mut self,
        rel_handle: &RelationHandle,
        stack: &mut Vec<DataValue>,
        processors: &BTreeMap<SmartString<LazyCompact>, (Arc<TextAnalyzer>, Vec<Bytecode>)>,
        new_kv: &[DataValue],
    ) -> Result<()> {
        for (k, (idx_handle, _)) in rel_handle.fts_indices.iter() {
            let (tokenizer, extractor) = processors.get(k).unwrap();
            self.put_fts_index_item(new_kv, extractor, stack, tokenizer, rel_handle, idx_handle)?;
        }
        Ok(())
    }

    fn del_in_fts(
        &mut self,
        rel_handle: &RelationHandle,
        stack: &mut Vec<DataValue>,
        processors: &BTreeMap<SmartString<LazyCompact>, (Arc<TextAnalyzer>, Vec<Bytecode>)>,
        old_kv: &[DataValue],
    ) -> Result<()> {
        for (k, (idx_handle, _)) in rel_handle.fts_indices.iter() {
            let (tokenizer, extractor) = processors.get(k).unwrap();
            self.del_fts_index_item(old_kv, extractor, stack, tokenizer, rel_handle, idx_handle)?;
        }
        Ok(())
    }

    fn put_in_lsh(
        &mut self,
        rel_handle: &RelationHandle,
        stack: &mut Vec<DataValue>,
        processors: &BTreeMap<SmartString<LazyCompact>, (Arc<TextAnalyzer>, Vec<Bytecode>)>,
        new_kv: &[DataValue],
        hash_perms_map: &BTreeMap<SmartString<LazyCompact>, HashPermutations>,
    ) -> Result<()> {
        for (k, (idx_handle, inv_idx_handle, manifest)) in rel_handle.lsh_indices.iter() {
            let (tokenizer, extractor) = processors.get(k).unwrap();
            self.put_lsh_index_item(
                new_kv,
                extractor,
                stack,
                tokenizer,
                rel_handle,
                idx_handle,
                inv_idx_handle,
                manifest,
                hash_perms_map.get(k).unwrap(),
            )?;
        }
        Ok(())
    }

    fn del_in_lsh(&mut self, rel_handle: &RelationHandle, old_kv: &[DataValue]) -> Result<()> {
        for (idx_handle, inv_idx_handle, _) in rel_handle.lsh_indices.values() {
            self.del_lsh_index_item(old_kv, None, idx_handle, inv_idx_handle)?;
        }
        Ok(())
    }

    fn update_in_hnsw(
        &mut self,
        relation_store: &RelationHandle,
        stack: &mut Vec<DataValue>,
        hnsw_filters: &BTreeMap<SmartString<LazyCompact>, Vec<Bytecode>>,
        new_kv: &[DataValue],
    ) -> Result<()> {
        for (name, (idx_handle, idx_manifest)) in relation_store.hnsw_indices.iter() {
            let filter = hnsw_filters.get(name);
            self.hnsw_put(
                idx_manifest,
                relation_store,
                idx_handle,
                filter,
                stack,
                new_kv,
            )?;
        }
        Ok(())
    }

    fn make_lsh_hash_perms(
        &self,
        relation_store: &RelationHandle,
    ) -> BTreeMap<SmartString<LazyCompact>, HashPermutations> {
        let mut perms = BTreeMap::new();
        for (name, (_, _, manifest)) in relation_store.lsh_indices.iter() {
            perms.insert(name.clone(), manifest.get_hash_perms());
        }
        perms
    }

    fn make_fts_lsh_processors(
        &self,
        relation_store: &RelationHandle,
    ) -> Result<BTreeMap<SmartString<LazyCompact>, (Arc<TextAnalyzer>, Vec<Bytecode>)>> {
        let mut processors = BTreeMap::new();
        for (name, (_, manifest)) in relation_store.fts_indices.iter() {
            let tokenizer = self
                .tokenizers
                .get(name, &manifest.tokenizer, &manifest.filters)?;

            let parsed = CozoScriptParser::parse(Rule::expr, &manifest.extractor)
                .into_diagnostic()?
                .next()
                .unwrap();
            let mut code_expr = build_expr(parsed, &Default::default())?;
            let binding_map = relation_store.raw_binding_map();
            code_expr.fill_binding_indices(&binding_map)?;
            let extractor = code_expr.compile()?;
            processors.insert(name.clone(), (tokenizer, extractor));
        }
        for (name, (_, _, manifest)) in relation_store.lsh_indices.iter() {
            let tokenizer = self
                .tokenizers
                .get(name, &manifest.tokenizer, &manifest.filters)?;

            let parsed = CozoScriptParser::parse(Rule::expr, &manifest.extractor)
                .into_diagnostic()?
                .next()
                .unwrap();
            let mut code_expr = build_expr(parsed, &Default::default())?;
            let binding_map = relation_store.raw_binding_map();
            code_expr.fill_binding_indices(&binding_map)?;
            let extractor = code_expr.compile()?;
            processors.insert(name.clone(), (tokenizer, extractor));
        }
        Ok(processors)
    }

    fn make_hnsw_filters(
        relation_store: &RelationHandle,
    ) -> Result<BTreeMap<SmartString<LazyCompact>, Vec<Bytecode>>> {
        let mut hnsw_filters = BTreeMap::new();
        for (name, (_, manifest)) in relation_store.hnsw_indices.iter() {
            if let Some(f_code) = &manifest.index_filter {
                let parsed = CozoScriptParser::parse(Rule::expr, f_code)
                    .into_diagnostic()?
                    .next()
                    .unwrap();
                let mut code_expr = build_expr(parsed, &Default::default())?;
                let binding_map = relation_store.raw_binding_map();
                code_expr.fill_binding_indices(&binding_map)?;
                hnsw_filters.insert(name.clone(), code_expr.compile()?);
            }
        }
        Ok(hnsw_filters)
    }

    fn update_in_relation<'s, S: Storage<'s>>(
        &mut self,
        db: &Db<S>,
        res_iter: impl Iterator<Item = Tuple>,
        headers: &[Symbol],
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        propagate_triggers: bool,
        to_clear: &mut Vec<(Vec<u8>, Vec<u8>)>,
        relation_store: &RelationHandle,
        metadata: &StoredRelationMetadata,
        key_bindings: &[Symbol],
        force_collect: &str,
        span: SourceSpan,
    ) -> Result<()> {
        let is_callback_target = callback_targets.contains(&relation_store.name)
            || force_collect == relation_store.name;

        if relation_store.access_level < AccessLevel::Protected {
            bail!(InsufficientAccessLevel(
                relation_store.name.to_string(),
                "row update".to_string(),
                relation_store.access_level
            ));
        }

        let key_extractors = make_extractors(
            &relation_store.metadata.keys,
            &metadata.keys,
            key_bindings,
            headers,
        )?;

        let need_to_collect = !force_collect.is_empty()
            || (!relation_store.is_temp
                && (is_callback_target
                    || (propagate_triggers && !relation_store.put_triggers.is_empty())));
        let has_indices = !relation_store.indices.is_empty();
        let has_hnsw_indices = !relation_store.hnsw_indices.is_empty();
        let has_fts_indices = !relation_store.fts_indices.is_empty();
        let has_lsh_indices = !relation_store.lsh_indices.is_empty();
        let mut new_tuples: Vec<DataValue> = vec![];
        let mut old_tuples: Vec<DataValue> = vec![];

        let val_extractors = make_update_extractors(
            &relation_store.metadata.non_keys,
            &metadata.keys,
            key_bindings,
            headers,
        )?;

        let mut stack = vec![];
        let hnsw_filters = Self::make_hnsw_filters(relation_store)?;
        let fts_lsh_processors = self.make_fts_lsh_processors(relation_store)?;
        let lsh_perms = self.make_lsh_hash_perms(relation_store);

        for tuple in res_iter {
            let mut new_kv: Vec<DataValue> = key_extractors
                .iter()
                .map(|ex| ex.extract_data(&tuple, cur_vld))
                .try_collect()?;

            let key = relation_store.encode_key_for_store(&new_kv, span)?;
            let original_val_bytes = if relation_store.is_temp {
                self.temp_store_tx.get(&key, true)?
            } else {
                self.store_tx.get(&key, true)?
            };
            let original_val: Tuple = match original_val_bytes {
                None => {
                    bail!(TransactAssertionFailure {
                        relation: relation_store.name.to_string(),
                        key: new_kv,
                        notice: "key to update does not exist".to_string()
                    })
                }
                Some(v) => rmp_serde::from_slice(&v[ENCODED_KEY_MIN_LEN..]).unwrap(),
            };
            let mut old_kv = Vec::with_capacity(relation_store.arity());
            old_kv.extend_from_slice(&new_kv);
            old_kv.extend_from_slice(&original_val);
            new_kv.reserve_exact(relation_store.arity());
            for (i, extractor) in val_extractors.iter().enumerate() {
                match extractor {
                    None => {
                        new_kv.push(original_val[i].clone());
                    }
                    Some(ex) => {
                        let val = ex.extract_data(&tuple, cur_vld)?;
                        new_kv.push(val);
                    }
                }
            }
            let new_val = relation_store.encode_val_for_store(&new_kv, span)?;

            if need_to_collect
                || has_indices
                || has_hnsw_indices
                || has_fts_indices
                || has_lsh_indices
            {
                self.del_in_fts(relation_store, &mut stack, &fts_lsh_processors, &old_kv)?;
                self.del_in_lsh(relation_store, &old_kv)?;
                self.update_in_index(relation_store, &new_kv, &old_kv)?;

                if need_to_collect {
                    old_tuples.push(DataValue::List(old_kv));
                }

                self.update_in_hnsw(relation_store, &mut stack, &hnsw_filters, &new_kv)?;
                self.put_in_fts(relation_store, &mut stack, &fts_lsh_processors, &new_kv)?;
                self.put_in_lsh(
                    relation_store,
                    &mut stack,
                    &fts_lsh_processors,
                    &new_kv,
                    &lsh_perms,
                )?;

                if need_to_collect {
                    new_tuples.push(DataValue::List(new_kv));
                }
            }

            if relation_store.is_temp {
                self.temp_store_tx.put(&key, &new_val)?;
            } else {
                self.store_tx.put(&key, &new_val)?;
            }
        }

        if need_to_collect && !new_tuples.is_empty() {
            self.collect_mutations(
                db,
                cur_vld,
                callback_targets,
                callback_collector,
                propagate_triggers,
                to_clear,
                relation_store,
                is_callback_target,
                new_tuples,
                old_tuples,
            )?;
        }
        Ok(())
    }

    fn collect_mutations<'s, S: Storage<'s>>(
        &mut self,
        db: &Db<S>,
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        propagate_triggers: bool,
        to_clear: &mut Vec<(Vec<u8>, Vec<u8>)>,
        relation_store: &RelationHandle,
        is_callback_target: bool,
        new_tuples: Vec<DataValue>,
        old_tuples: Vec<DataValue>,
    ) -> Result<()> {
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
                let mut program = parse_script(
                    trigger,
                    &Default::default(),
                    &db.fixed_rules.read().unwrap(),
                    cur_vld,
                )?
                .get_single_program()?;

                make_const_rule(
                    &mut program,
                    "_new",
                    kv_bindings.clone(),
                    new_tuples.to_vec(),
                );
                make_const_rule(
                    &mut program,
                    "_old",
                    kv_bindings.clone(),
                    old_tuples.to_vec(),
                );

                let (_, cleanups) = db
                    .run_query(
                        self,
                        program,
                        cur_vld,
                        callback_targets,
                        callback_collector,
                        false,
                    )
                    .map_err(|err| {
                        if err.source_code().is_some() {
                            err
                        } else {
                            err.with_source_code(format!("{trigger} "))
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
                NamedRows::new(
                    headers.clone(),
                    new_tuples
                        .into_iter()
                        .map(|v| match v {
                            DataValue::List(l) => l,
                            _ => unreachable!(),
                        })
                        .collect_vec(),
                ),
                NamedRows::new(
                    headers,
                    old_tuples
                        .into_iter()
                        .map(|v| match v {
                            DataValue::List(l) => l,
                            _ => unreachable!(),
                        })
                        .collect_vec(),
                ),
            ))
        }
        Ok(())
    }

    fn update_in_index(
        &mut self,
        relation_store: &RelationHandle,
        new_kv: &[DataValue],
        old_kv: &[DataValue],
    ) -> Result<()> {
        for (idx_rel, idx_extractor) in relation_store.indices.values() {
            let idx_tup_old = idx_extractor
                .iter()
                .map(|i| old_kv[*i].clone())
                .collect_vec();
            let encoded_old = idx_rel.encode_key_for_store(&idx_tup_old, Default::default())?;
            self.store_tx.del(&encoded_old)?;

            let idx_tup_new = idx_extractor
                .iter()
                .map(|i| new_kv[*i].clone())
                .collect_vec();
            let encoded_new = idx_rel.encode_key_for_store(&idx_tup_new, Default::default())?;
            self.store_tx.put(&encoded_new, &[])?;
        }
        Ok(())
    }

    fn ensure_not_in_relation(
        &mut self,
        res_iter: impl Iterator<Item = Tuple>,
        headers: &[Symbol],
        cur_vld: ValidityTs,
        relation_store: &RelationHandle,
        metadata: &StoredRelationMetadata,
        key_bindings: &[Symbol],
        span: SourceSpan,
    ) -> Result<()> {
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
            let extracted: Vec<DataValue> = key_extractors
                .iter()
                .map(|ex| ex.extract_data(&tuple, cur_vld))
                .try_collect()?;
            let key = relation_store.encode_key_for_store(&extracted, span)?;
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
        Ok(())
    }

    fn ensure_in_relation(
        &mut self,
        res_iter: impl Iterator<Item = Tuple>,
        headers: &[Symbol],
        cur_vld: ValidityTs,
        relation_store: &RelationHandle,
        metadata: &StoredRelationMetadata,
        key_bindings: &[Symbol],
        span: SourceSpan,
    ) -> Result<()> {
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
            &metadata.keys,
            key_bindings,
            headers,
        )?;
        key_extractors.extend(val_extractors);

        for tuple in res_iter {
            let extracted: Vec<DataValue> = key_extractors
                .iter()
                .map(|ex| ex.extract_data(&tuple, cur_vld))
                .try_collect()?;

            let key = relation_store.encode_key_for_store(&extracted, span)?;
            let val = relation_store.encode_val_for_store(&extracted, span)?;

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
                            notice: "key exists in database, but value does not match".to_string()
                        })
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_from_relation<'s, S: Storage<'s>>(
        &mut self,
        db: &Db<S>,
        res_iter: impl Iterator<Item = Tuple>,
        headers: &[Symbol],
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        propagate_triggers: bool,
        to_clear: &mut Vec<(Vec<u8>, Vec<u8>)>,
        relation_store: &RelationHandle,
        metadata: &StoredRelationMetadata,
        key_bindings: &[Symbol],
        check_exists: bool,
        force_collect: &str,
        span: SourceSpan,
    ) -> Result<()> {
        let is_callback_target =
            callback_targets.contains(&relation_store.name) || force_collect == relation_store.name;

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

        let need_to_collect = !force_collect.is_empty()
            || (!relation_store.is_temp
                && (is_callback_target
                    || (propagate_triggers && !relation_store.rm_triggers.is_empty())));
        let has_indices = !relation_store.indices.is_empty();
        let has_hnsw_indices = !relation_store.hnsw_indices.is_empty();
        let has_fts_indices = !relation_store.fts_indices.is_empty();
        let has_lsh_indices = !relation_store.lsh_indices.is_empty();
        let fts_processors = self.make_fts_lsh_processors(relation_store)?;
        let mut new_tuples: Vec<DataValue> = vec![];
        let mut old_tuples: Vec<DataValue> = vec![];
        let mut stack = vec![];

        for tuple in res_iter {
            let extracted: Vec<DataValue> = key_extractors
                .iter()
                .map(|ex| ex.extract_data(&tuple, cur_vld))
                .try_collect()?;
            let key = relation_store.encode_key_for_store(&extracted, span)?;
            if check_exists {
                let exists = if relation_store.is_temp {
                    self.temp_store_tx.exists(&key, false)?
                } else {
                    self.store_tx.exists(&key, false)?
                };
                if !exists {
                    bail!(TransactAssertionFailure {
                        relation: relation_store.name.to_string(),
                        key: extracted,
                        notice: "key does not exists in database".to_string()
                    });
                }
            }
            if need_to_collect || has_indices || has_hnsw_indices || has_fts_indices || has_lsh_indices {
                if let Some(existing) = self.store_tx.get(&key, false)? {
                    let mut tup = extracted.clone();
                    extend_tuple_from_v(&mut tup, &existing);
                    self.del_in_fts(relation_store, &mut stack, &fts_processors, &tup)?;
                    self.del_in_lsh(relation_store, &tup)?;
                    if has_indices {
                        for (idx_rel, extractor) in relation_store.indices.values() {
                            let idx_tup = extractor.iter().map(|i| tup[*i].clone()).collect_vec();
                            let encoded =
                                idx_rel.encode_key_for_store(&idx_tup, Default::default())?;
                            self.store_tx.del(&encoded)?;
                        }
                    }
                    if has_hnsw_indices {
                        for (idx_handle, _) in relation_store.hnsw_indices.values() {
                            self.hnsw_remove(relation_store, idx_handle, &extracted)?;
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
                    let mut program = parse_script(
                        trigger,
                        &Default::default(),
                        &db.fixed_rules.read().unwrap(),
                        cur_vld,
                    )?
                    .get_single_program()?;

                    make_const_rule(&mut program, "_new", k_bindings.clone(), new_tuples.clone());

                    make_const_rule(
                        &mut program,
                        "_old",
                        kv_bindings.clone(),
                        old_tuples.clone(),
                    );

                    let (_, cleanups) = db
                        .run_query(
                            self,
                            program,
                            cur_vld,
                            callback_targets,
                            callback_collector,
                            false,
                        )
                        .map_err(|err| {
                            if err.source_code().is_some() {
                                err
                            } else {
                                err.with_source_code(format!("{trigger} "))
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
                    NamedRows::new(
                        k_bindings
                            .into_iter()
                            .map(|k| k.name.to_string())
                            .collect_vec(),
                        new_tuples
                            .into_iter()
                            .map(|v| match v {
                                DataValue::List(l) => l,
                                _ => unreachable!(),
                            })
                            .collect_vec(),
                    ),
                    NamedRows::new(
                        kv_bindings
                            .into_iter()
                            .map(|k| k.name.to_string())
                            .collect_vec(),
                        old_tuples
                            .into_iter()
                            .map(|v| match v {
                                DataValue::List(l) => l,
                                _ => unreachable!(),
                            })
                            .collect_vec(),
                    ),
                ))
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Assertion failure for {key:?} of {relation}: {notice}")]
#[diagnostic(code(transact::assertion_failure))]
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

fn make_update_extractors(
    stored: &[ColumnDef],
    input: &[ColumnDef],
    bindings: &[Symbol],
    tuple_headers: &[Symbol],
) -> Result<Vec<Option<DataExtractor>>> {
    let input_keys: BTreeSet<_> = input.iter().map(|b| &b.name).collect();
    let mut extractors = Vec::with_capacity(stored.len());
    for col in stored.iter() {
        if input_keys.contains(&col.name) {
            extractors.push(Some(make_extractor(col, input, bindings, tuple_headers)?));
        } else {
            extractors.push(None);
        }
    }
    Ok(extractors)
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
