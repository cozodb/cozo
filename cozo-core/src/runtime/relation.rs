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
use miette::{bail, ensure, Diagnostic, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::relation::StoredRelationMetadata;
use crate::data::symb::Symbol;
use crate::data::tuple::{decode_tuple_from_key, Tuple, TupleT, ENCODED_KEY_MIN_LEN};
use crate::data::value::{DataValue, ValidityTs};
use crate::parse::SourceSpan;
use crate::query::compile::IndexPositionUse;
use crate::runtime::transact::SessionTx;
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

#[derive(Clone, Eq, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
pub(crate) struct RelationHandle {
    pub(crate) name: SmartString<LazyCompact>,
    pub(crate) id: RelationId,
    pub(crate) metadata: StoredRelationMetadata,
    pub(crate) put_triggers: Vec<String>,
    pub(crate) rm_triggers: Vec<String>,
    pub(crate) replace_triggers: Vec<String>,
    pub(crate) access_level: AccessLevel,
    #[serde(default)]
    pub(crate) is_temp: bool,
    #[serde(default)]
    pub(crate) indices: BTreeMap<SmartString<LazyCompact>, (RelationHandle, Vec<usize>)>,
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
    pub(crate) fn encode_key_for_store(&self, tuple: &Tuple, span: SourceSpan) -> Result<Vec<u8>> {
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
    pub(crate) fn encode_val_for_store(&self, tuple: &Tuple, _span: SourceSpan) -> Result<Vec<u8>> {
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
        tuple: &Tuple,
        _span: SourceSpan,
    ) -> Result<Vec<u8>> {
        let mut ret = self.encode_key_prefix(tuple.len());
        tuple.serialize(&mut Serializer::new(&mut ret)).unwrap();
        Ok(ret)
    }
    pub(crate) fn ensure_compatible(
        &self,
        inp: &InputRelationHandle,
        is_remove: bool,
    ) -> Result<()> {
        let InputRelationHandle { metadata, .. } = inp;
        // check that every given key is found and compatible
        for col in &metadata.keys {
            self.metadata.compatible_with_col(col, true)?
        }
        for col in &metadata.non_keys {
            self.metadata.compatible_with_col(col, false)?
        }
        // check that every key is provided or has default
        for col in &self.metadata.keys {
            metadata.satisfied_by_required_col(col, true)?;
        }
        if !is_remove {
            for col in &self.metadata.non_keys {
                metadata.satisfied_by_required_col(col, false)?;
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
                .map(|val_data| decode_tuple_from_kv(&key_data, &val_data)))
        } else {
            Ok(tx
                .store_tx
                .get(&key_data, false)?
                .map(|val_data| decode_tuple_from_kv(&key_data, &val_data)))
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
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
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

/// Decode tuple from key-value pairs. Used for customizing storage
/// in trait [`StoreTx`](crate::StoreTx).
#[inline]
pub fn decode_tuple_from_kv(key: &[u8], val: &[u8]) -> Tuple {
    let mut tup = decode_tuple_from_key(key);
    extend_tuple_from_v(&mut tup, val);
    tup
}

pub fn extend_tuple_from_v(key: &mut Tuple, val: &[u8]) {
    if !val.is_empty() {
        let vals: Vec<DataValue> = rmp_serde::from_slice(&val[ENCODED_KEY_MIN_LEN..]).unwrap();
        key.extend(vals);
    }
}

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
        name: Symbol,
        puts: Vec<String>,
        rms: Vec<String>,
        replaces: Vec<String>,
    ) -> Result<()> {
        if name.name.starts_with('_') {
            bail!("Cannot set triggers for temp store")
        }
        let mut original = self.get_relation(&name, true)?;
        if original.access_level < AccessLevel::Protected {
            bail!(InsufficientAccessLevel(
                original.name.to_string(),
                "set triggers".to_string(),
                original.access_level
            ))
        }
        original.put_triggers = puts;
        original.rm_triggers = rms;
        original.replace_triggers = replaces;

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
    pub(crate) fn destroy_relation(&mut self, name: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let is_temp = name.starts_with('_');

        // if name.starts_with('_') {
        //     bail!("Cannot destroy temp relation");
        // }
        let store = self.get_relation(name, true)?;
        if !store.indices.is_empty() {
            bail!("Cannot remove stored relation `{}` with indices attached.", name);
        }
        if store.access_level < AccessLevel::Normal {
            bail!(InsufficientAccessLevel(
                store.name.to_string(),
                "relation removal".to_string(),
                store.access_level
            ))
        }

        for k in store.indices.keys() {
            // TODO leak
            self.destroy_relation(&format!("{name}:{k}"))?;
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
        Ok((lower_bound, upper_bound))
    }
    pub(crate) fn set_access_level(&mut self, rel: Symbol, level: AccessLevel) -> Result<()> {
        let mut meta = self.get_relation(&rel, true)?;
        meta.access_level = level;

        let name_key = vec![DataValue::Str(meta.name.clone())].encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.store_tx.put(&name_key, &meta_val)?;

        Ok(())
    }

    pub(crate) fn create_index(
        &mut self,
        rel_name: &Symbol,
        idx_name: &Symbol,
        cols: Vec<Symbol>,
    ) -> Result<()> {
        let mut rel_handle = self.get_relation(rel_name, true)?;
        if rel_handle.indices.contains_key(&idx_name.name) {
            #[derive(Debug, Error, Diagnostic)]
            #[error("index {0} for relation {1} already exists")]
            #[diagnostic(code(tx::index_already_exists))]
            pub(crate) struct IndexAlreadyExists(String, String);

            bail!(IndexAlreadyExists(
                idx_name.name.to_string(),
                rel_name.name.to_string()
            ));
        }

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
            for tuple in rel_handle.scan_all(self).collect_vec() {
                let tuple = tuple?;
                let extracted = extraction_indices
                    .iter()
                    .map(|idx| tuple[*idx].clone())
                    .collect_vec();
                let key = idx_handle.encode_key_for_store(&extracted, Default::default())?;
                self.store_tx.put(&key, &[])?;
            }
        }

        rel_handle
            .indices
            .insert(idx_name.name.clone(), (idx_handle, extraction_indices));

        let new_encoded =
            vec![DataValue::from(&rel_name.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel_handle
            .serialize(&mut Serializer::new(&mut meta_val))
            .unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    pub(crate) fn remove_index(&mut self, rel_name: &Symbol, idx_name: &Symbol) -> Result<()> {
        let mut rel = self.get_relation(rel_name, true)?;
        if rel.indices.remove(&idx_name.name).is_none() {
            #[derive(Debug, Error, Diagnostic)]
            #[error("index {0} for relation {1} not found")]
            #[diagnostic(code(tx::idx_not_found))]
            pub(crate) struct IndexNotFound(String, String);

            bail!(IndexNotFound(idx_name.to_string(), rel_name.to_string()));
        }

        // TODO leak
        self.destroy_relation(&format!("{}:{}", rel_name.name, idx_name.name))?;

        let new_encoded =
            vec![DataValue::from(&rel_name.name as &str)].encode_as_key(RelationId::SYSTEM);
        let mut meta_val = vec![];
        rel.serialize(&mut Serializer::new(&mut meta_val)).unwrap();
        self.store_tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }

    pub(crate) fn rename_relation(&mut self, old: Symbol, new: Symbol) -> Result<()> {
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

        let mut rel = self.get_relation(&old, true)?;
        if rel.access_level < AccessLevel::Normal {
            bail!(InsufficientAccessLevel(
                rel.name.to_string(),
                "renaming relation".to_string(),
                rel.access_level
            ));
        }
        rel.name = new.name;

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
