/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use miette::Result;

use crate::data::program::MagicSymbol;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::in_mem::{InMemRelation, StoredRelationId};
use crate::runtime::relation::RelationId;
use crate::storage::rocks::RocksDbTx;
use crate::storage::StoreTx;

pub struct SessionTx {
    pub(crate) tx: RocksDbTx,
    pub(crate) relation_store_id: Arc<AtomicU64>,
    pub(crate) mem_store_id: Arc<AtomicU32>,
}

impl SessionTx {
    pub(crate) fn new_rule_store(&self, rule_name: MagicSymbol, arity: usize) -> InMemRelation {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        InMemRelation::new(StoredRelationId(old_count), rule_name, arity)
    }

    pub(crate) fn new_temp_store(&self, span: SourceSpan) -> InMemRelation {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        InMemRelation::new(
            StoredRelationId(old_count),
            MagicSymbol::Muggle {
                inner: Symbol::new("", span),
            },
            0,
        )
    }

    pub(crate) fn load_last_relation_store_id(&self) -> Result<RelationId> {
        let tuple = Tuple(vec![DataValue::Null]);
        let t_encoded = tuple.encode_as_key(RelationId::SYSTEM);
        let found = self.tx.get(&t_encoded, false)?;
        Ok(match found {
            None => RelationId::SYSTEM,
            Some(slice) => RelationId::raw_decode(&slice),
        })
    }

    pub fn commit_tx(&mut self) -> Result<()> {
        self.tx.commit()?;
        Ok(())
    }
}
