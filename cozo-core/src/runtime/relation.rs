/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::Ordering;

use itertools::Itertools;
use log::error;
use miette::{bail, ensure, Diagnostic, IntoDiagnostic, Result};
use pest::Parser;
use rmp_serde::Serializer;
use serde::Serialize;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::relation::{ColType, ColumnDef, NullableColType, StoredRelationMetadata};
use crate::data::symb::Symbol;
use crate::data::tuple::{decode_tuple_from_key, Tuple, TupleT, ENCODED_KEY_MIN_LEN};
use crate::data::value::{DataValue, ValidityTs};
use crate::fts::FtsIndexManifest;
use crate::parse::expr::build_expr;
use crate::parse::sys::{FtsIndexConfig, HnswIndexConfig, MinHashLshConfig};
use crate::parse::{CozoScriptParser, Rule, SourceSpan};
use crate::query::compile::IndexPositionUse;
use crate::runtime::hnsw::HnswIndexManifest;
use crate::runtime::minhash_lsh::{HashPermutations, LshParams, MinHashLshIndexManifest, Weights};
use crate::runtime::transact::SessionTx;
use crate::utils::TempCollector;
use crate::{NamedRows, StoreTx};

#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Debug,
    serde_derive::Serialize,
    serde_derive::Deserialize,
    PartialOrd,
    Ord,
)]
pub(crate) struct RelationId(pub(crate) u64);

impl RelationId {
    pub(crate) fn new(u: u64) -> Self {
        if u > 2u64.pow(6 * 8) {
            panic!("StoredRelId overflow: {u}")
        } else {
            Self(u)
        }
    }
    pub(crate) fn next(&self) -> Self {
        Self::new(self.0 + 1)
    }
    pub(crate) const SYSTEM: Self = Self(0);
    pub(crate) fn raw_encode(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub(crate) fn raw_decode(src: &[u8]) -> Self {
        let u = u64::from_be_bytes([
            src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
        ]);
        Self::new(u)
    }
}

#[derive(Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct RelationHandle {
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) id: RelationId,
    pub(crate) metadata: StoredRelationMetadata,
    pub(crate) put_triggers: Vec<String>,
    pub(crate) rm_triggers: Vec<String>,
    pub(crate) replace_triggers: Vec<String>,
    pub(crate) access_level: AccessLevel,
    pub(crate) is_temp: bool,
    pub(crate) indices: BTreeMap<SmartString<LazyCompact>, (RelationHandle, Vec<usize>)>,
    pub(crate) hnsw_indices:
        BTreeMap<SmartString<LazyCompact>, (RelationHandle, HnswIndexManifest)>,
    pub(crate) fts_indices: BTreeMap<SmartString<LazyCompact>, (RelationHandle, FtsIndexManifest)>,
    pub(crate) lsh_indices: BTreeMap<
        SmartString<LazyCompact>,
        (RelationHandle, RelationHandle, MinHashLshIndexManifest),
    >,
    pub(crate) description: SmartString<LazyCompact>,
}

impl RelationHandle {
    pub(crate) fn has_index(&self, index_name: &str) -> bool {
        self.indices.contains_key(index_name)
            || self.hnsw_indices.contains_key(index_name)
            || self.fts_indices.contains_key(index_name)
            || self.lsh_indices.contains_key(index_name)
    }
    pub(crate) fn has_no_index(&self) -> bool {
        self.indices.is_empty()
            && self.hnsw_indices.is_empty()
            && self.fts_indices.is_empty()
            && self.lsh_indices.is_empty()
    }
}

#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde_derive::Serialize,
    serde_derive::Deserialize,
    Default,
    Ord,
    PartialOrd,
)]
pub(crate) enum AccessLevel {
    Hidden,
    ReadOnly,
    Protected,
    #[default]
    Normal,
}

impl Display for AccessLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AccessLevel::Normal => f.write_str("normal"),
            AccessLevel::Protected => f.write_str("protected"),
            AccessLevel::ReadOnly => f.write_str("read_only"),
            AccessLevel::Hidden => f.write_str("hidden"),
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Arity mismatch for stored relation {name}: expect {expect_arity}, got {actual_arity}")]
#[diagnostic(code(eval::stored_rel_arity_mismatch))]
struct StoredRelArityMismatch {
    name: String,
    expect_arity: usize,
    actual_arity: usize,
    #[label]
    span: SourceSpan,
}

impl RelationHandle {
    pub(crate) fn raw_binding_map(&self) -> BTreeMap<Symbol, usize> {
        let mut ret = BTreeMap::new();
        for (i, col) in self.metadata.keys.iter().enumerate() {
            ret.insert(Symbol::new(col.name.clone(), Default::default()), i);
        }
        for (i, col) in self.metadata.non_keys.iter().enumerate() {
            ret.insert(
                Symbol::new(col.name.clone(), Default::default()),
                i + self.metadata.keys.len(),
            );
        }
        ret
    }
    pub(crate) fn has_triggers(&self) -> bool {
        !self.put_triggers.is_empty() || !self.rm_triggers.is_empty()
    }
    fn encode_key_prefix(&self, len: usize) -> Vec<u8> {
        let mut ret = Vec::with_capacity(4 + 4 * len + 10 * len);
        let prefix_bytes = self.id.0.to_be_bytes();
        ret.extend(prefix_bytes);
        ret
    }
    pub(crate) fn as_named_rows(&self, tx: &SessionTx<'_>) -> Result<NamedRows> {
        let rows: Vec<_> = self.scan_all(tx).try_collect()?;
        let mut headers = self
            .metadata
            .keys
            .iter()
            .map(|col| col.name.to_string())
            .collect_vec();
        headers.extend(
            self.metadata
                .non_keys
                .iter()
                .map(|col| col.name.to_string()),
        );
        Ok(NamedRows::new(headers, rows))
    }
    #[allow(dead_code)]
    pub(crate) fn amend_key_prefix(&self, data: &mut [u8]) {
        let prefix_bytes = self.id.0.to_be_bytes();
        data[0..8].copy_from_slice(&prefix_bytes);
    }
    pub(crate) fn choose_index(
        &self,
        arg_uses: &[IndexPositionUse],
        validity_query: bool,
    ) -> Option<(RelationHandle, Vec<usize>, bool)> {
        if self.indices.is_empty() {
            return None;
        }
        if *arg_uses.first().unwrap() == IndexPositionUse::Join {
            return None;
        }
        let mut max_prefix_len = 0;
        let required_positions = arg_uses
            .iter()
            .enumerate()
            .filter_map(|(i, pos_use)| {
                if *pos_use != IndexPositionUse::Ignored {
                    Some(i)
                } else {
                    None
                }
            })
            .collect_vec();
        let mut chosen = None;
        for (manifest, mapper) in self.indices.values() {
            if validity_query && *mapper.last().unwrap() != self.metadata.keys.len() - 1 {
                continue;
            }

            let mut cur_prefix_len = 0;
            for i in mapper {
                if arg_uses[*i] == IndexPositionUse::Join {
                    cur_prefix_len += 1;
                } else {
                    break;
                }
            }
            if cur_prefix_len > max_prefix_len {
                max_prefix_len = cur_prefix_len;
                let mut need_join = false;
                for need_pos in required_positions.iter() {
                    if !mapper.contains(need_pos) {
                        need_join = true;
                        break;
                    }
                }
                chosen = Some((manifest.clone(), mapper.clone(), need_join))
            }
        }
        chosen
    }
    pub(crate) fn encode_key_for_store(
        &self,
        tuple: &[DataValue],
        span: SourceSpan,
    ) -> Result<Vec<u8>> {
        let len = self.metadata.keys.len();
        ensure!(
            tuple.len() >= len,
            StoredRelArityMismatch {
                name: self.name.to_string(),
                expect_arity: self.arity(),
                actual_arity: tuple.len(),
                span
            }
        );
        let mut ret = self.encode_key_prefix(len);
        for val in &tuple[0..len] {
            ret.encode_datavalue(val);
        }
        Ok(ret)
    }
    pub(crate) fn encode_partial_key_for_store(&self, tuple: &[DataValue]) -> Vec<u8> {
        let mut ret = self.encode_key_prefix(tuple.len());
        for val in tuple {
            ret.encode_datavalue(val);
        }
        ret
    }
    pub(crate) fn encode_val_for_store(
        &self,
        tuple: &[DataValue],
        _span: SourceSpan,
    ) -> Result<Vec<u8>> {
        let start = self.metadata.keys.len();
        let len = self.metadata.non_keys.len();
        let mut ret = self.encode_key_prefix(len);
        tuple[start..]
            .serialize(&mut Serializer::new(&mut ret))
            .unwrap();
        Ok(ret)
    }
    pub(crate) fn encode_val_only_for_store(
        &self,
        tuple: &[DataValue],
        _span: SourceSpan,
    ) -> Result<Vec<u8>> {
        let mut ret = self.encode_key_prefix(tuple.len());
        tuple.serialize(&mut Serializer::new(&mut ret)).unwrap();
        Ok(ret)
    }
    pub(crate) fn ensure_compatible(
        &self,
        inp: &InputRelationHandle,
        is_remove_or_update: bool,
    ) -> Result<()> {
        let InputRelationHandle { metadata, .. } = inp;
        // check that every given key is found and compatible
        for col in metadata.keys.iter().chain(self.metadata.non_keys.iter()) {
            self.metadata.compatible_with_col(col)?
        }
        // check that every key is provided or has default
        for col in &self.metadata.keys {
            metadata.satisfied_by_required_col(col)?;
        }
        if !is_remove_or_update {
            for col in &self.metadata.non_keys {
                metadata.satisfied_by_required_col(col)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct InputRelationHandle {
    pub(crate) name: Symbol,
    pub(crate) metadata: StoredRelationMetadata,
    pub(crate) key_bindings: Vec<Symbol>,
    pub(crate) dep_bindings: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}

impl Debug for RelationHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Relation<{}>", self.name)
    }
}

#[derive(thiserror::Error, miette::Diagnostic, Debug)]
#[error("Cannot deserialize relation")]
#[diagnostic(code(deser::relation))]
#[diagnostic(help(
    "This could indicate a bug, or you are using an incompatible DB version. \
Consider file a bug report."
))]
pub(crate) struct RelationDeserError;

impl RelationHandle {
    pub(crate) fn arity(&self) -> usize {
        self.metadata.non_keys.len() + self.metadata.keys.len()
    }
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        Ok(rmp_serde::from_slice(data).map_err(|e| {
            error!(
                "Cannot deserialize relation metadata from bytes: {:x?}, {:?}",
                data, e
            );
            RelationDeserError
        })?)
    }
    pub(crate) fn scan_all<'a>(
        &self,
        tx: &'a SessionTx<'_>,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let lower = Tuple::default().encode_as_key(self.id);
        let upper = Tuple::default().encode_as_key(self.id.next());
        if self.is_temp {
            tx.temp_store_tx.range_scan_tuple(&lower, &upper)
        } else {
            tx.store_tx.range_scan_tuple(&lower, &upper)
        }
    }

    pub(crate) fn skip_scan_all<'a>(
        &self,
        tx: &'a SessionTx<'_>,
        valid_at: ValidityTs,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let lower = Tuple::default().encode_as_key(self.id);
        let upper = Tuple::default().encode_as_key(self.id.next());
        if self.is_temp {
            tx.temp_store_tx
                .range_skip_scan_tuple(&lower, &upper, valid_at)
        } else {
            tx.store_tx.range_skip_scan_tuple(&lower, &upper, valid_at)
        }
    }

    pub(crate) fn get(&self, tx: &SessionTx<'_>, key: &[DataValue]) -> Result<Option<Tuple>> {
        let key_data = key.encode_as_key(self.id);
        if self.is_temp {
            Ok(tx
                .temp_store_tx
                .get(&key_data, false)?
                .map(|val_data| decode_tuple_from_kv(&key_data, &val_data, Some(self.arity()))))
        } else {
            Ok(tx
                .store_tx
                .get(&key_data, false)?
                .map(|val_data| decode_tuple_from_kv(&key_data, &val_data, Some(self.arity()))))
        }
    }

    pub(crate) fn get_val_only(
        &self,
        tx: &SessionTx<'_>,
        key: &[DataValue],
    ) -> Result<Option<Tuple>> {
        let key_data = key.encode_as_key(self.id);
        if self.is_temp {
            Ok(tx
                .temp_store_tx
                .get(&key_data, false)?
                .map(|val_data| rmp_serde::from_slice(&val_data[ENCODED_KEY_MIN_LEN..]).unwrap()))
        } else {
            Ok(tx
                .store_tx
                .get(&key_data, false)?
                .map(|val_data| rmp_serde::from_slice(&val_data[ENCODED_KEY_MIN_LEN..]).unwrap()))
        }
    }

    pub(crate) fn exists(&self, tx: &SessionTx<'_>, key: &[DataValue]) -> Result<bool> {
        let key_data = key.encode_as_key(self.id);
        if self.is_temp {
            tx.temp_store_tx.exists(&key_data, false)
        } else {
            tx.store_tx.exists(&key_data, false)
        }
    }

    pub(crate) fn scan_prefix<'a>(
        &self,
        tx: &'a SessionTx<'_>,
        prefix: &Tuple,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mut lower = prefix.clone();
        lower.truncate(self.metadata.keys.len());
        let mut upper = lower.clone();
        upper.push(DataValue::Bot);
        let prefix_encoded = lower.encode_as_key(self.id);
        let upper_encoded = upper.encode_as_key(self.id);
        if self.is_temp {
            tx.temp_store_tx
                .range_scan_tuple(&prefix_encoded, &upper_encoded)
        } else {
            tx.store_tx
                .range_scan_tuple(&prefix_encoded, &upper_encoded)
        }
    }

    pub(crate) fn skip_scan_prefix<'a>(
        &self,
        tx: &'a SessionTx<'_>,
        prefix: &Tuple,
        valid_at: ValidityTs,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mut lower = prefix.clone();
        lower.truncate(self.metadata.keys.len());
        let mut upper = lower.clone();
        upper.push(DataValue::Bot);
        let prefix_encoded = lower.encode_as_key(self.id);
        let upper_encoded = upper.encode_as_key(self.id);
        if self.is_temp {
            tx.temp_store_tx
                .range_skip_scan_tuple(&prefix_encoded, &upper_encoded, valid_at)
        } else {
            tx.store_tx
                .range_skip_scan_tuple(&prefix_encoded, &upper_encoded, valid_at)
        }
    }

    pub(crate) fn scan_bounded_prefix<'a>(
        &self,
        tx: &'a SessionTx<'_>,
        prefix: &[DataValue],
        lower: &[DataValue],
        upper: &[DataValue],
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mut lower_t = prefix.to_vec();
        lower_t.extend_from_slice(lower);
        let mut upper_t = prefix.to_vec();
        upper_t.extend_from_slice(upper);
        upper_t.push(DataValue::Bot);
        let lower_encoded = lower_t.encode_as_key(self.id);
        let upper_encoded = upper_t.encode_as_key(self.id);
        if self.is_temp {
            tx.temp_store_tx
                .range_scan_tuple(&lower_encoded, &upper_encoded)
        } else {
            tx.store_tx.range_scan_tuple(&lower_encoded, &upper_encoded)
        }
    }
    pub(crate) fn skip_scan_bounded_prefix<'a>(
        &self,
        tx: &'a SessionTx<'_>,
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
        valid_at: ValidityTs,
    ) -> impl Iterator<Item = Result<Tuple>> + 'a {
        let mut lower_t = prefix.clone();
        lower_t.extend_from_slice(lower);
        let mut upper_t = prefix.clone();
        upper_t.extend_from_slice(upper);
        upper_t.push(DataValue::Bot);
        let lower_encoded = lower_t.encode_as_key(self.id);
        let upper_encoded = upper_t.encode_as_key(self.id);
        if self.is_temp {
            tx.temp_store_tx
                .range_skip_scan_tuple(&lower_encoded, &upper_encoded, valid_at)
        } else {
            tx.store_tx
                .range_skip_scan_tuple(&lower_encoded, &upper_encoded, valid_at)
        }
    }
}

const DEFAULT_SIZE_HINT: usize = 16;

/// Decode tuple from key-value pairs. Used for customizing storage
/// in trait [`StoreTx`](crate::StoreTx).
#[inline]
pub fn decode_tuple_from_kv(key: &[u8], val: &[u8], size_hint: Option<usize>) -> Tuple {
    let mut tup = decode_tuple_from_key(key, size_hint.unwrap_or(DEFAULT_SIZE_HINT));
    extend_tuple_from_v(&mut tup, val);
    tup
}

pub fn extend_tuple_from_v(key: &mut Tuple, val: &[u8]) {
    if !val.is_empty() {
        let vals: Vec<DataValue> = rmp_serde::from_slice(&val[ENCODED_KEY_MIN_LEN..]).unwrap();
        key.extend(vals);
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("index {0} for relation {1} already exists")]
#[diagnostic(code(tx::index_already_exists))]
pub(crate) struct IndexAlreadyExists(String, String);

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot create relation {0} as one with the same name already exists")]
#[diagnostic(code(eval::rel_name_conflict))]
struct RelNameConflictError(String);

impl<'a> SessionTx<'a> {
    pub(crate) fn relation_exists(&self, name: &str) -> Result<bool> {
        let key = DataValue::from(name);
        let encoded = vec![key].encode_as_key(RelationId::SYSTEM);
        if name.starts_with('_') {
            self.temp_store_tx.exists(&encoded, false)
        } else {
            self.store_tx.exists(&encoded, false)
        }
    }
    pub(crate) fn set_relation_triggers(
        &mut self,
        name: &Symbol,
        puts: &[String],
        rms: &[String],
        replaces: &[String],
    ) -> Result<()> {
        if name.name.starts_with('_') {
            bail!("Cannot set triggers for temp store")
        }
        let mut original = self.get_relation(name, true)?;
        if original.access_level < AccessLevel::Protected {
            bail!(InsufficientAccessLevel(
                original.name.to_string(),
                "set triggers".to_string(),
                original.access_level
            ))
        }
        original.put_triggers = puts.to_vec();
        original.rm_triggers = rms.to_vec();
        original.replace_triggers = replaces.to_vec();

        let name_key =
            vec![DataValue::Str(original.name.clone())].encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        original
            .serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.store_tx.put(&name_key, &meta_val)?;

        Ok(())
    }
    pub(crate) fn create_relation(
        &mut self,
        input_meta: InputRelationHandle,
    ) -> Result<RelationHandle> {
        let key = DataValue::Str(input_meta.name.name.clone());
        let encoded = vec![key].encode_as_key(RelationId::SYSTEM);

        let is_temp = input_meta.name.is_temp_store_name();

        if is_temp {
            if self.store_tx.exists(&encoded, true)? {
                bail!(RelNameConflictError(input_meta.name.to_string()))
            };
        } else if self.temp_store_tx.exists(&encoded, true)? {
            bail!(RelNameConflictError(input_meta.name.to_string()))
        }

        let metadata = input_meta.metadata.clone();
        let last_id = if is_temp {
            self.temp_store_id.fetch_add(1, Ordering::Relaxed) as u64
        } else {
            self.relation_store_id.fetch_add(1, Ordering::SeqCst)
        };
        let meta = RelationHandle {
            name: input_meta.name.name,
            id: RelationId::new(last_id + 1),
            metadata,
            put_triggers: vec![],
            rm_triggers: vec![],
            replace_triggers: vec![],
            access_level: AccessLevel::Normal,
            is_temp,
            indices: Default::default(),
            hnsw_indices: Default::default(),
            fts_indices: Default::default(),
            lsh_indices: Default::default(),
            description: Default::default(),
        };

        let name_key = vec![DataValue::Str(meta.name.clone())].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        let tuple = vec![DataValue::Null];
        let t_encoded = tuple.encode_as_key(RelationId::SYSTEM);

        if is_temp {
            self.temp_store_tx.put(&encoded, &meta.id.raw_encode())?;
            self.temp_store_tx.put(&name_key, &meta_val)?;
            self.temp_store_tx.put(&t_encoded, &meta.id.raw_encode())?;
        } else {
            self.store_tx.put(&encoded, &meta.id.raw_encode())?;
            self.store_tx.put(&name_key, &meta_val)?;
            self.store_tx.put(&t_encoded, &meta.id.raw_encode())?;
        }

        Ok(meta)
    }
    pub(crate) fn get_relation(&self, name: &str, lock: bool) -> Result<RelationHandle> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Cannot find requested stored relation '{0}'")]
        #[diagnostic(code(query::relation_not_found))]
        struct StoredRelationNotFoundError(String);

        let key = DataValue::from(name);
        let encoded = vec![key].encode_as_key(RelationId::SYSTEM);

        let found = if name.starts_with('_') {
            self.temp_store_tx
                .get(&encoded, lock)?
                .ok_or_else(|| StoredRelationNotFoundError(name.to_string()))?
        } else {
            self.store_tx
                .get(&encoded, lock)?
                .ok_or_else(|| StoredRelationNotFoundError(name.to_string()))?
        };
        let metadata = RelationHandle::decode(&found)?;
        Ok(metadata)
    }
    pub(crate) fn describe_relation(&mut self, name: &str, description: &str) -> Result<()> {
        let mut meta = self.get_relation(name, true)?;

        meta.description = SmartString::from(description);
        let name_key = vec![DataValue::Str(meta.name.clone())].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        if meta.is_temp {
            self.temp_store_tx.put(&name_key, &meta_val)?;
        } else {
            self.store_tx.put(&name_key, &meta_val)?;
        }

        Ok(())
    }
    pub(crate) fn destroy_relation(&mut self, name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let is_temp = name.starts_with('_');
        let mut to_clean = vec![];

        // if name.starts_with('_') {
        //     bail!("Cannot destroy temp relation");
        // }
        let store = self.get_relation(name, true)?;
        if !store.has_no_index() {
            bail!(
                "Cannot remove stored relation `{}` with indices attached.",
                name
            );
        }
        if store.access_level < AccessLevel::Normal {
            bail!(InsufficientAccessLevel(
                store.name.to_string(),
                "relation removal".to_string(),
                store.access_level
            ))
        }

        for k in store.indices.keys() {
            let more_to_clean = self.destroy_relation(&format!("{name}:{k}"))?;
            to_clean.extend(more_to_clean);
        }

        for k in store.hnsw_indices.keys() {
            let more_to_clean = self.destroy_relation(&format!("{name}:{k}"))?;
            to_clean.extend(more_to_clean);
        }

        let key = DataValue::from(name);
        let encoded = vec![key].encode_as_key(RelationId::SYSTEM);
        if is_temp {
            self.temp_store_tx.del(&encoded)?;
        } else {
            self.store_tx.del(&encoded)?;
        }
        let lower_bound = Tuple::default().encode_as_key(store.id);
        let upper_bound = Tuple::default().encode_as_key(store.id.next());
        to_clean.push((lower_bound, upper_bound));
        Ok(to_clean)
    }
    pub(crate) fn set_access_level(&mut self, rel: &Symbol, level: AccessLevel) -> Result<()> {
        let mut meta = self.get_relation(rel, true)?;
        meta.access_level = level;

        let name_key = vec![DataValue::Str(meta.name.clone())].encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.store_tx.put(&name_key, &meta_val)?;

        Ok(())
    }

    pub(crate) fn create_minhash_lsh_index(&mut self, config: &MinHashLshConfig) -> Result<()> {
        // Get relation handle
        let mut rel_handle = self.get_relation(&config.base_relation, true)?;

        // Check if index already exists
        if rel_handle.has_index(&config.index_name) {
            bail!(IndexAlreadyExists(
                config.index_name.to_string(),
                config.index_name.to_string()
            ));
        }

        let inv_idx_keys = rel_handle.metadata.keys.clone();
        let inv_idx_vals = vec![ColumnDef {
            name: SmartString::from("minhash"),
            typing: NullableColType {
                coltype: ColType::Bytes,
                nullable: false,
            },
            default_gen: None,
        }];

        let mut idx_keys = vec![ColumnDef {
            name: SmartString::from("hash"),
            typing: NullableColType {
                coltype: ColType::Bytes,
                nullable: false,
            },
            default_gen: None,
        }];
        for k in rel_handle.metadata.keys.iter() {
            idx_keys.push(ColumnDef {
                name: format!("src_{}", k.name).into(),
                typing: k.typing.clone(),
                default_gen: None,
            });
        }
        let idx_vals = vec![];

        let idx_handle = self.write_idx_relation(
            &config.base_relation,
            &config.index_name,
            idx_keys,
            idx_vals,
        )?;

        let inv_idx_handle = self.write_idx_relation(
            &config.base_relation,
            &format!("{}:inv", config.index_name),
            inv_idx_keys,
            inv_idx_vals,
        )?;

        // add index to relation
        let params = LshParams::find_optimal_params(
            config.target_threshold.0,
            config.n_perm,
            &Weights(
                config.false_positive_weight.0,
                config.false_negative_weight.0,
            ),
        );
        let num_perm = params.b * params.r;
        let perms = HashPermutations::new(num_perm);
        let manifest = MinHashLshIndexManifest {
            base_relation: config.base_relation.clone(),
            index_name: config.index_name.clone(),
            extractor: config.extractor.clone(),
            n_gram: config.n_gram,
            tokenizer: config.tokenizer.clone(),
            filters: config.filters.clone(),
            num_perm,
            n_bands: params.b,
            n_rows_in_band: params.r,
            threshold: config.target_threshold.0,
            perms: perms.as_bytes().to_vec(),
        };

        // populate index
        let tokenizer =
            self.tokenizers
                .get(&idx_handle.name, &manifest.tokenizer, &manifest.filters)?;
        let parsed = CozoScriptParser::parse(Rule::expr, &manifest.extractor)
            .into_diagnostic()?
            .next()
            .unwrap();
        let mut code_expr = build_expr(parsed, &Default::default())?;
        let binding_map = rel_handle.raw_binding_map();
        code_expr.fill_binding_indices(&binding_map)?;
        let extractor = code_expr.compile()?;

        let mut stack = vec![];

        let hash_perms = manifest.get_hash_perms();
        let mut existing = TempCollector::default();
        for tuple in rel_handle.scan_all(self) {
            existing.push(tuple?);
        }

        for tuple in existing.into_iter() {
            self.put_lsh_index_item(
                &tuple,
                &extractor,
                &mut stack,
                &tokenizer,
                &rel_handle,
                &idx_handle,
                &inv_idx_handle,
                &manifest,
                &hash_perms,
            )?;
        }

        rel_handle.lsh_indices.insert(
            manifest.index_name.clone(),
            (idx_handle, inv_idx_handle, manifest),
        );

        // update relation metadata
        let new_encoded =
            vec![DataValue::from(&rel_handle.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel_handle
            .serialize(&mut Serializer::new(&mut meta_val))
            .unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    pub(crate) fn create_fts_index(&mut self, config: &FtsIndexConfig) -> Result<()> {
        // Get relation handle
        let mut rel_handle = self.get_relation(&config.base_relation, true)?;

        // Check if index already exists
        if rel_handle.has_index(&config.index_name) {
            bail!(IndexAlreadyExists(
                config.index_name.to_string(),
                config.index_name.to_string()
            ));
        }

        // Build key columns definitions
        let mut idx_keys: Vec<ColumnDef> = vec![ColumnDef {
            name: SmartString::from("word"),
            typing: NullableColType {
                coltype: ColType::String,
                nullable: false,
            },
            default_gen: None,
        }];

        for k in rel_handle.metadata.keys.iter() {
            idx_keys.push(ColumnDef {
                name: format!("src_{}", k.name).into(),
                typing: k.typing.clone(),
                default_gen: None,
            });
        }

        let col_type = NullableColType {
            coltype: ColType::List {
                eltype: Box::new(NullableColType {
                    coltype: ColType::Int,
                    nullable: false,
                }),
                len: None,
            },
            nullable: false,
        };

        let non_idx_keys: Vec<ColumnDef> = vec![
            ColumnDef {
                name: SmartString::from("offset_from"),
                typing: col_type.clone(),
                default_gen: None,
            },
            ColumnDef {
                name: SmartString::from("offset_to"),
                typing: col_type.clone(),
                default_gen: None,
            },
            ColumnDef {
                name: SmartString::from("position"),
                typing: col_type,
                default_gen: None,
            },
            ColumnDef {
                name: SmartString::from("total_length"),
                typing: NullableColType {
                    coltype: ColType::Int,
                    nullable: false,
                },
                default_gen: None,
            },
        ];

        let idx_handle = self.write_idx_relation(
            &config.base_relation,
            &config.index_name,
            idx_keys,
            non_idx_keys,
        )?;

        // add index to relation
        let manifest = FtsIndexManifest {
            base_relation: config.base_relation.clone(),
            index_name: config.index_name.clone(),
            extractor: config.extractor.clone(),
            tokenizer: config.tokenizer.clone(),
            filters: config.filters.clone(),
        };

        // populate index
        let tokenizer =
            self.tokenizers
                .get(&idx_handle.name, &manifest.tokenizer, &manifest.filters)?;

        let parsed = CozoScriptParser::parse(Rule::expr, &manifest.extractor)
            .into_diagnostic()?
            .next()
            .unwrap();
        let mut code_expr = build_expr(parsed, &Default::default())?;
        let binding_map = rel_handle.raw_binding_map();
        code_expr.fill_binding_indices(&binding_map)?;
        let extractor = code_expr.compile()?;

        let mut stack = vec![];

        let mut existing = TempCollector::default();
        for tuple in rel_handle.scan_all(self) {
            existing.push(tuple?);
        }
        for tuple in existing.into_iter() {
            let key_part = &tuple[..rel_handle.metadata.keys.len()];
            if rel_handle.exists(self, key_part)? {
                self.del_fts_index_item(
                    &tuple,
                    &extractor,
                    &mut stack,
                    &tokenizer,
                    &rel_handle,
                    &idx_handle,
                )?;
            }
            self.put_fts_index_item(
                &tuple,
                &extractor,
                &mut stack,
                &tokenizer,
                &rel_handle,
                &idx_handle,
            )?;
        }

        rel_handle
            .fts_indices
            .insert(manifest.index_name.clone(), (idx_handle, manifest));

        // update relation metadata
        let new_encoded =
            vec![DataValue::from(&rel_handle.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel_handle
            .serialize(&mut Serializer::new(&mut meta_val))
            .unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    pub(crate) fn create_hnsw_index(&mut self, config: &HnswIndexConfig) -> Result<()> {
        // Get relation handle
        let mut rel_handle = self.get_relation(&config.base_relation, true)?;

        // Check if index already exists
        if rel_handle.has_index(&config.index_name) {
            bail!(IndexAlreadyExists(
                config.index_name.to_string(),
                config.index_name.to_string()
            ));
        }

        // Check that what we are indexing are really vectors
        if config.vec_fields.is_empty() {
            bail!("Cannot create HNSW index without vector fields");
        }
        let mut vec_field_indices = vec![];
        for field in config.vec_fields.iter() {
            let mut found = false;
            for (i, col) in rel_handle
                .metadata
                .keys
                .iter()
                .chain(rel_handle.metadata.non_keys.iter())
                .enumerate()
            {
                if col.name == *field {
                    let mut col_type = col.typing.coltype.clone();
                    if let ColType::List { eltype, .. } = &col_type {
                        col_type = eltype.coltype.clone();
                    }

                    if let ColType::Vec { eltype, len } = col_type {
                        if eltype != config.dtype {
                            bail!("Cannot create HNSW index with field {} of type {:?} (expected {:?})", field, eltype, config.dtype);
                        }
                        if len != config.vec_dim {
                            bail!("Cannot create HNSW index with field {} of dimension {} (expected {})", field, len, config.vec_dim);
                        }
                    } else {
                        bail!("Cannot create HNSW index with non-vector field {}", field)
                    }

                    found = true;
                    vec_field_indices.push(i);
                    break;
                }
            }
            if !found {
                bail!("Cannot create HNSW index with non-existent field {}", field);
            }
        }

        // Build key columns definitions
        let mut idx_keys: Vec<ColumnDef> = vec![ColumnDef {
            // layer -1 stores the self-loops
            name: SmartString::from("layer"),
            typing: NullableColType {
                coltype: ColType::Int,
                nullable: false,
            },
            default_gen: None,
        }];
        // for self-loops, fr and to are identical
        for prefix in ["fr", "to"] {
            for col in rel_handle.metadata.keys.iter() {
                let mut col = col.clone();
                col.name = SmartString::from(format!("{}_{}", prefix, col.name));
                idx_keys.push(col);
            }
            idx_keys.push(ColumnDef {
                name: SmartString::from(format!("{}__field", prefix)),
                typing: NullableColType {
                    coltype: ColType::Int,
                    nullable: false,
                },
                default_gen: None,
            });
            idx_keys.push(ColumnDef {
                name: SmartString::from(format!("{}__sub_idx", prefix)),
                typing: NullableColType {
                    coltype: ColType::Int,
                    nullable: false,
                },
                default_gen: None,
            });
        }

        // Build non-key columns definitions
        let non_idx_keys = vec![
            // For self-loops, stores the number of neighbours
            ColumnDef {
                name: SmartString::from("dist"),
                typing: NullableColType {
                    coltype: ColType::Float,
                    nullable: false,
                },
                default_gen: None,
            },
            // For self-loops, stores a hash of the neighbours, for conflict detection
            ColumnDef {
                name: SmartString::from("hash"),
                typing: NullableColType {
                    coltype: ColType::Bytes,
                    nullable: true,
                },
                default_gen: None,
            },
            ColumnDef {
                name: SmartString::from("ignore_link"),
                typing: NullableColType {
                    coltype: ColType::Bool,
                    nullable: false,
                },
                default_gen: None,
            },
        ];
        // create index relation
        let idx_handle = self.write_idx_relation(
            &config.base_relation,
            &config.index_name,
            idx_keys,
            non_idx_keys,
        )?;

        // add index to relation
        let manifest = HnswIndexManifest {
            base_relation: config.base_relation.clone(),
            index_name: config.index_name.clone(),
            vec_dim: config.vec_dim,
            dtype: config.dtype,
            vec_fields: vec_field_indices,
            distance: config.distance,
            ef_construction: config.ef_construction,
            m_neighbours: config.m_neighbours,
            m_max: config.m_neighbours,
            m_max0: config.m_neighbours * 2,
            level_multiplier: 1. / (config.m_neighbours as f64).ln(),
            index_filter: config.index_filter.clone(),
            extend_candidates: config.extend_candidates,
            keep_pruned_connections: config.keep_pruned_connections,
        };

        // populate index
        let mut all_tuples = TempCollector::default();
        for tuple in rel_handle.scan_all(self) {
            all_tuples.push(tuple?);
        }
        let filter = if let Some(f_code) = &manifest.index_filter {
            let parsed = CozoScriptParser::parse(Rule::expr, f_code)
                .into_diagnostic()?
                .next()
                .unwrap();
            let mut code_expr = build_expr(parsed, &Default::default())?;
            let binding_map = rel_handle.raw_binding_map();
            code_expr.fill_binding_indices(&binding_map)?;
            code_expr.compile()?
        } else {
            vec![]
        };
        let filter = if filter.is_empty() {
            None
        } else {
            Some(&filter)
        };
        let mut stack = vec![];
        for tuple in all_tuples.into_iter() {
            self.hnsw_put(
                &manifest,
                &rel_handle,
                &idx_handle,
                filter,
                &mut stack,
                &tuple,
            )?;
        }

        rel_handle
            .hnsw_indices
            .insert(config.index_name.clone(), (idx_handle, manifest));

        // update relation metadata
        let new_encoded =
            vec![DataValue::from(&config.base_relation as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel_handle
            .serialize(&mut Serializer::new(&mut meta_val))
            .unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    fn write_idx_relation(
        &mut self,
        base_name: &str,
        idx_name: &str,
        idx_keys: Vec<ColumnDef>,
        non_idx_keys: Vec<ColumnDef>,
    ) -> Result<RelationHandle> {
        let key_bindings = idx_keys
            .iter()
            .map(|col| Symbol::new(col.name.clone(), Default::default()))
            .collect();
        let dep_bindings = non_idx_keys
            .iter()
            .map(|col| Symbol::new(col.name.clone(), Default::default()))
            .collect();
        let idx_handle = InputRelationHandle {
            name: Symbol::new(format!("{}:{}", base_name, idx_name), Default::default()),
            metadata: StoredRelationMetadata {
                keys: idx_keys,
                non_keys: non_idx_keys,
            },
            key_bindings,
            dep_bindings,
            span: Default::default(),
        };
        let idx_handle = self.create_relation(idx_handle)?;
        Ok(idx_handle)
    }

    pub(crate) fn create_index(
        &mut self,
        rel_name: &Symbol,
        idx_name: &Symbol,
        cols: &[Symbol],
    ) -> Result<()> {
        // Get relation handle
        let mut rel_handle = self.get_relation(rel_name, true)?;

        // Check if index already exists
        if rel_handle.has_index(&idx_name.name) {
            bail!(IndexAlreadyExists(
                idx_name.name.to_string(),
                rel_name.name.to_string()
            ));
        }

        // Build column definitions
        let mut col_defs = vec![];
        'outer: for col in cols.iter() {
            for orig_col in rel_handle
                .metadata
                .keys
                .iter()
                .chain(rel_handle.metadata.non_keys.iter())
            {
                if orig_col.name == col.name {
                    col_defs.push(orig_col.clone());
                    continue 'outer;
                }
            }

            #[derive(Debug, Error, Diagnostic)]
            #[error("column {0} in index {1} for relation {2} not found")]
            #[diagnostic(code(tx::col_in_idx_not_found))]
            pub(crate) struct ColInIndexNotFound(String, String, String);

            bail!(ColInIndexNotFound(
                col.name.to_string(),
                idx_name.name.to_string(),
                rel_name.name.to_string()
            ));
        }

        'outer: for key in rel_handle.metadata.keys.iter() {
            for col in cols.iter() {
                if col.name == key.name {
                    continue 'outer;
                }
            }
            col_defs.push(key.clone());
        }

        let key_bindings = col_defs
            .iter()
            .map(|col| Symbol::new(col.name.clone(), Default::default()))
            .collect_vec();
        let idx_meta = StoredRelationMetadata {
            keys: col_defs,
            non_keys: vec![],
        };

        // create index relation
        let idx_handle = InputRelationHandle {
            name: Symbol::new(
                format!("{}:{}", rel_name.name, idx_name.name),
                Default::default(),
            ),
            metadata: idx_meta,
            key_bindings,
            dep_bindings: vec![],
            span: Default::default(),
        };

        let idx_handle = self.create_relation(idx_handle)?;

        // populate index
        let extraction_indices = idx_handle
            .metadata
            .keys
            .iter()
            .map(|col| {
                for (i, kc) in rel_handle.metadata.keys.iter().enumerate() {
                    if kc.name == col.name {
                        return i;
                    }
                }
                for (i, kc) in rel_handle.metadata.non_keys.iter().enumerate() {
                    if kc.name == col.name {
                        return i + rel_handle.metadata.keys.len();
                    }
                }
                unreachable!()
            })
            .collect_vec();

        if self.store_tx.supports_par_put() {
            for tuple in rel_handle.scan_all(self) {
                let tuple = tuple?;
                let extracted = extraction_indices
                    .iter()
                    .map(|idx| tuple[*idx].clone())
                    .collect_vec();
                let key = idx_handle.encode_key_for_store(&extracted, Default::default())?;
                self.store_tx.par_put(&key, &[])?;
            }
        } else {
            let mut existing = TempCollector::default();
            for tuple in rel_handle.scan_all(self) {
                existing.push(tuple?);
            }
            for tuple in existing.into_iter() {
                let extracted = extraction_indices
                    .iter()
                    .map(|idx| tuple[*idx].clone())
                    .collect_vec();
                let key = idx_handle.encode_key_for_store(&extracted, Default::default())?;
                self.store_tx.put(&key, &[])?;
            }
        }

        // add index to relation
        rel_handle
            .indices
            .insert(idx_name.name.clone(), (idx_handle, extraction_indices));

        // update relation metadata
        let new_encoded =
            vec![DataValue::from(&rel_name.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel_handle
            .serialize(&mut Serializer::new(&mut meta_val))
            .unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    pub(crate) fn remove_index(
        &mut self,
        rel_name: &Symbol,
        idx_name: &Symbol,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut rel = self.get_relation(rel_name, true)?;
        let is_lsh = rel.lsh_indices.contains_key(&idx_name.name);
        let is_fts = rel.fts_indices.contains_key(&idx_name.name);
        if is_lsh || is_fts {
            self.tokenizers.named_cache.write().unwrap().clear();
            self.tokenizers.hashed_cache.write().unwrap().clear();
        }
        if rel.indices.remove(&idx_name.name).is_none()
            && rel.hnsw_indices.remove(&idx_name.name).is_none()
            && rel.lsh_indices.remove(&idx_name.name).is_none()
            && rel.fts_indices.remove(&idx_name.name).is_none()
        {
            #[derive(Debug, Error, Diagnostic)]
            #[error("index {0} for relation {1} not found")]
            #[diagnostic(code(tx::idx_not_found))]
            pub(crate) struct IndexNotFound(String, String);

            bail!(IndexNotFound(idx_name.to_string(), rel_name.to_string()));
        }

        let mut to_clean =
            self.destroy_relation(&format!("{}:{}", rel_name.name, idx_name.name))?;
        if is_lsh {
            to_clean.extend(
                self.destroy_relation(&format!("{}:{}:inv", rel_name.name, idx_name.name))?,
            );
        }

        let new_encoded =
            vec![DataValue::from(&rel_name.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel.serialize(&mut Serializer::new(&mut meta_val)).unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(to_clean)
    }

    pub(crate) fn rename_relation(&mut self, old: &Symbol, new: &Symbol) -> Result<()> {
        if old.name.starts_with('_') || new.name.starts_with('_') {
            bail!("Bad name given");
        }
        let new_key = DataValue::Str(new.name.clone());
        let new_encoded = vec![new_key].encode_as_key(RelationId::SYSTEM);

        if self.store_tx.exists(&new_encoded, true)? {
            bail!(RelNameConflictError(new.name.to_string()))
        };

        let old_key = DataValue::Str(old.name.clone());
        let old_encoded = vec![old_key].encode_as_key(RelationId::SYSTEM);

        let mut rel = self.get_relation(old, true)?;
        if rel.access_level < AccessLevel::Normal {
            bail!(InsufficientAccessLevel(
                rel.name.to_string(),
                "renaming relation".to_string(),
                rel.access_level
            ));
        }
        rel.name = new.name.clone();

        let mut meta_val = vec![];
        rel.serialize(&mut Serializer::new(&mut meta_val)).unwrap();
        self.store_tx.del(&old_encoded)?;
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }
    pub(crate) fn rename_temp_relation(&mut self, old: Symbol, new: Symbol) -> Result<()> {
        let new_key = DataValue::Str(new.name.clone());
        let new_encoded = vec![new_key].encode_as_key(RelationId::SYSTEM);

        if self.temp_store_tx.exists(&new_encoded, true)? {
            bail!(RelNameConflictError(new.name.to_string()))
        };

        let old_key = DataValue::Str(old.name.clone());
        let old_encoded = vec![old_key].encode_as_key(RelationId::SYSTEM);

        let mut rel = self.get_relation(&old, true)?;
        rel.name = new.name;

        let mut meta_val = vec![];
        rel.serialize(&mut Serializer::new(&mut meta_val)).unwrap();
        self.temp_store_tx.del(&old_encoded)?;
        self.temp_store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Insufficient access level {2} for {1} on stored relation '{0}'")]
#[diagnostic(code(tx::insufficient_access_level))]
pub(crate) struct InsufficientAccessLevel(
    pub(crate) String,
    pub(crate) String,
    pub(crate) AccessLevel,
);
