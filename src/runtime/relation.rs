/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::cmp::max;
use std::cmp::Ordering::Greater;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::Ordering;

use log::error;
use miette::{bail, ensure, Diagnostic, Result};
use rmp_serde::Serializer;
use serde::Serialize;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use cozorocks::DbIter;

use crate::data::relation::StoredRelationMetadata;
use crate::data::symb::Symbol;
use crate::data::tuple::{compare_tuple_keys, EncodedTuple, Tuple};
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::transact::SessionTx;
use crate::utils::swap_option_result;

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
            panic!("StoredRelId overflow: {}", u)
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
        ret.extend([
            prefix_bytes[2],
            prefix_bytes[3],
            prefix_bytes[4],
            prefix_bytes[5],
            prefix_bytes[6],
            prefix_bytes[7],
        ]);
        ret.extend((len as u16).to_be_bytes());
        ret.resize(max(6, 4 * (len + 1)), 0);

        ret
    }
    fn encode_key_element(&self, ret: &mut Vec<u8>, idx: usize, val: &DataValue) {
        if idx > 0 {
            let pos = (ret.len() as u32).to_be_bytes();
            for (i, u) in pos.iter().enumerate() {
                ret[4 * (1 + idx) + i] = *u;
            }
        }
        val.serialize(&mut Serializer::new(ret)).unwrap();
    }
    pub(crate) fn adhoc_encode_key(&self, tuple: &Tuple, span: SourceSpan) -> Result<Vec<u8>> {
        let len = self.metadata.keys.len();
        ensure!(
            tuple.0.len() >= len,
            StoredRelArityMismatch {
                name: self.name.to_string(),
                expect_arity: self.arity(),
                actual_arity: tuple.0.len(),
                span
            }
        );
        let mut ret = self.encode_key_prefix(len);
        for i in 0..len {
            self.encode_key_element(&mut ret, i, &tuple.0[i])
        }
        Ok(ret)
    }
    pub(crate) fn adhoc_encode_val(&self, tuple: &Tuple, _span: SourceSpan) -> Result<Vec<u8>> {
        let start = self.metadata.keys.len();
        let len = self.metadata.non_keys.len();
        let mut ret = self.encode_key_prefix(len);
        for i in 0..len {
            self.encode_key_element(&mut ret, i, &tuple.0[i + start])
        }
        Ok(ret)
    }
    pub(crate) fn ensure_compatible(&self, inp: &InputRelationHandle) -> Result<()> {
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
        for col in &self.metadata.non_keys {
            metadata.satisfied_by_required_col(col, false)?;
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
#[diagnostic(help("This could indicate a bug. Consider file a bug report."))]
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
    pub(crate) fn scan_all(&self, tx: &SessionTx) -> impl Iterator<Item = Result<Tuple>> {
        let lower = Tuple::default().encode_as_key(self.id);
        let upper = Tuple::default().encode_as_key(self.id.next());
        RelationIterator::new(tx, &lower, &upper)
    }

    pub(crate) fn scan_prefix(
        &self,
        tx: &SessionTx,
        prefix: &Tuple,
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut lower = prefix.0.clone();
        lower.truncate(self.metadata.keys.len());
        let mut upper = lower.clone();
        upper.push(DataValue::Bot);
        let prefix_encoded = Tuple(lower).encode_as_key(self.id);
        let upper_encoded = Tuple(upper).encode_as_key(self.id);
        RelationIterator::new(tx, &prefix_encoded, &upper_encoded)
    }
    pub(crate) fn scan_bounded_prefix(
        &self,
        tx: &SessionTx,
        prefix: &Tuple,
        lower: &[DataValue],
        upper: &[DataValue],
    ) -> impl Iterator<Item = Result<Tuple>> {
        let mut lower_t = prefix.clone();
        lower_t.0.extend_from_slice(lower);
        let mut upper_t = prefix.clone();
        upper_t.0.extend_from_slice(upper);
        upper_t.0.push(DataValue::Bot);
        let lower_encoded = lower_t.encode_as_key(self.id);
        let upper_encoded = upper_t.encode_as_key(self.id);
        RelationIterator::new(tx, &lower_encoded, &upper_encoded)
    }
}

struct RelationIterator {
    inner: DbIter,
    started: bool,
    upper_bound: Vec<u8>,
}

impl RelationIterator {
    fn new(sess: &SessionTx, lower: &[u8], upper: &[u8]) -> Self {
        let mut inner = sess.tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        Self {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        }
    }
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        if self.started {
            self.inner.next()
        } else {
            self.started = true;
        }
        Ok(match self.inner.pair()? {
            None => None,
            Some((k_slice, v_slice)) => {
                if compare_tuple_keys(&self.upper_bound, k_slice) != Greater {
                    None
                } else {
                    let mut tup = EncodedTuple(k_slice).decode();
                    if !v_slice.is_empty() {
                        let v_tup = EncodedTuple(v_slice);
                        if v_tup.arity() > 0 {
                            tup.0.extend(v_tup.decode().0);
                        }
                    }
                    Some(tup)
                }
            }
        })
    }
}

impl Iterator for RelationIterator {
    type Item = Result<Tuple>;
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("Cannot create relation {0} as one with the same name already exists")]
#[diagnostic(code(eval::rel_name_conflict))]
struct RelNameConflictError(String);

impl SessionTx {
    pub(crate) fn relation_exists(&self, name: &str) -> Result<bool> {
        let key = DataValue::Str(SmartString::from(name));
        let encoded = Tuple(vec![key]).encode_as_key(RelationId::SYSTEM);
        Ok(self.tx.exists(&encoded, false)?)
    }
    pub(crate) fn set_relation_triggers(
        &mut self,
        name: Symbol,
        puts: Vec<String>,
        rms: Vec<String>,
        replaces: Vec<String>,
    ) -> Result<()> {
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
            Tuple(vec![DataValue::Str(original.name.clone())]).encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        original
            .serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.tx.put(&name_key, &meta_val)?;

        Ok(())
    }
    pub(crate) fn create_relation(
        &mut self,
        input_meta: InputRelationHandle,
    ) -> Result<RelationHandle> {
        let key = DataValue::Str(input_meta.name.name.clone());
        let encoded = Tuple(vec![key]).encode_as_key(RelationId::SYSTEM);

        if self.tx.exists(&encoded, true)? {
            bail!(RelNameConflictError(input_meta.name.to_string()))
        };

        let metadata = input_meta.metadata.clone();
        let last_id = self.relation_store_id.fetch_add(1, Ordering::SeqCst);
        let meta = RelationHandle {
            name: input_meta.name.name,
            id: RelationId::new(last_id + 1),
            metadata,
            put_triggers: vec![],
            rm_triggers: vec![],
            replace_triggers: vec![],
            access_level: AccessLevel::Normal,
        };

        self.tx.put(&encoded, &meta.id.raw_encode())?;
        let name_key =
            Tuple(vec![DataValue::Str(meta.name.clone())]).encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.tx.put(&name_key, &meta_val)?;

        let tuple = Tuple(vec![DataValue::Null]);
        let t_encoded = tuple.encode_as_key(RelationId::SYSTEM);
        self.tx.put(&t_encoded, &meta.id.raw_encode())?;
        Ok(meta)
    }
    pub(crate) fn get_relation(&self, name: &str, lock: bool) -> Result<RelationHandle> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Cannot find requested stored relation '{0}'")]
        #[diagnostic(code(query::relation_not_found))]
        struct StoredRelationNotFoundError(String);

        let key = DataValue::Str(SmartString::from(name as &str));
        let encoded = Tuple(vec![key]).encode_as_key(RelationId::SYSTEM);

        let found = self
            .tx
            .get(&encoded, lock)?
            .ok_or_else(|| StoredRelationNotFoundError(name.to_string()))?;
        let metadata = RelationHandle::decode(&found)?;
        Ok(metadata)
    }
    pub(crate) fn destroy_relation(&mut self, name: &str) -> Result<(Vec<u8>, Vec<u8>)> {
        let store = self.get_relation(name, true)?;
        if store.access_level < AccessLevel::Normal {
            bail!(InsufficientAccessLevel(
                store.name.to_string(),
                "relation removal".to_string(),
                store.access_level
            ))
        }
        let key = DataValue::Str(SmartString::from(name as &str));
        let encoded = Tuple(vec![key]).encode_as_key(RelationId::SYSTEM);
        self.tx.del(&encoded)?;
        let lower_bound = Tuple::default().encode_as_key(store.id);
        let upper_bound = Tuple::default().encode_as_key(store.id.next());
        Ok((lower_bound, upper_bound))
    }
    pub(crate) fn set_access_level(&mut self, rel: Symbol, level: AccessLevel) -> Result<()> {
        let mut meta = self.get_relation(&rel, true)?;
        meta.access_level = level;

        let name_key =
            Tuple(vec![DataValue::Str(meta.name.clone())]).encode_as_key(RelationId::SYSTEM);

        let mut meta_val = vec![];
        meta.serialize(&mut Serializer::new(&mut meta_val).with_struct_map())
            .unwrap();
        self.tx.put(&name_key, &meta_val)?;

        Ok(())
    }
    pub(crate) fn rename_relation(&mut self, old: Symbol, new: Symbol) -> Result<()> {
        let new_key = DataValue::Str(new.name.clone());
        let new_encoded = Tuple(vec![new_key]).encode_as_key(RelationId::SYSTEM);

        if self.tx.exists(&new_encoded, true)? {
            bail!(RelNameConflictError(new.name.to_string()))
        };

        let old_key = DataValue::Str(old.name.clone());
        let old_encoded = Tuple(vec![old_key]).encode_as_key(RelationId::SYSTEM);

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
        self.tx.del(&old_encoded)?;
        self.tx.put(&new_encoded, &meta_val)?;

        Ok(())
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Insufficient access level {2} for {1} on stored relation '{0}'")]
pub(crate) struct InsufficientAccessLevel(
    pub(crate) String,
    pub(crate) String,
    pub(crate) AccessLevel,
);
