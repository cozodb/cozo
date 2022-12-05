/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use miette::Result;

use crate::data::program::MagicSymbol;
use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::in_mem::{InMemRelation, StoredRelationId};
use crate::runtime::relation::RelationId;
use crate::storage::StoreTx;

pub struct SessionTx<'a> {
    pub(crate) tx: Box<dyn StoreTx<'a> + 'a>,
    pub(crate) relation_store_id: Arc<AtomicU64>,
    pub(crate) mem_store_id: Arc<AtomicU32>,
}

impl<'a> SessionTx<'a> {
    pub(crate) fn new_rule_store(&self, rule_name: MagicSymbol, arity: usize) -> InMemRelation {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        let ret = InMemRelation::new(StoredRelationId(old_count), rule_name, arity);
        ret.ensure_mem_db_for_epoch(0);
        ret
    }

    pub(crate) fn new_temp_store(&self, span: SourceSpan) -> InMemRelation {
        let old_count = self.mem_store_id.fetch_add(1, Ordering::AcqRel);
        let old_count = old_count & 0x00ff_ffffu32;
        let ret = InMemRelation::new(
            StoredRelationId(old_count),
            MagicSymbol::Muggle {
                inner: Symbol::new("", span),
            },
            0,
        );
        ret.ensure_mem_db_for_epoch(0);
        ret
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
