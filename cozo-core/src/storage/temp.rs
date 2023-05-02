/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::default::Default;

use miette::Result;

use crate::data::tuple::Tuple;
use crate::data::value::ValidityTs;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::mem::SkipIterator;
use crate::storage::{Storage, StoreTx};

#[derive(Default, Clone)]
pub(crate) struct TempStorage;

impl<'s> Storage<'s> for TempStorage {
    type Tx = TempTx;

    fn storage_kind(&self) -> &'static str {
        "temp"
    }

    fn transact(&'s self, _write: bool) -> Result<Self::Tx> {
        Ok(TempTx {
            store: Default::default(),
        })
    }

    fn range_compact(&'s self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        panic!("range compact called on temp store")
    }

    fn batch_put<'a>(
        &'a self,
        _data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        panic!("batch put compact called on temp store")
    }
}

pub(crate) struct TempTx {
    store: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl<'s> StoreTx<'s> for TempTx {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        Ok(self.store.get(key).cloned())
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.store.insert(key.to_vec(), val.to_vec());
        Ok(())
    }

    fn supports_par_put(&self) -> bool {
        false
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.store.remove(key);
        Ok(())
    }

    fn del_range_from_persisted(&mut self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        Ok(self.store.contains_key(key))
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        Box::new(
            self.store
                .range(lower.to_vec()..upper.to_vec())
                .map(|(k, v)| Ok(decode_tuple_from_kv(k, v, None))),
        )
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        Box::new(
            SkipIterator {
                inner: &self.store,
                upper: upper.to_vec(),
                valid_at,
                next_bound: lower.to_vec(),
                size_hint: None,
            }
            .map(Ok),
        )
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        Box::new(
            self.store
                .range(lower.to_vec()..upper.to_vec())
                .map(|(k, v)| Ok((k.clone(), v.clone()))),
        )
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize> where 's: 'a {
        Ok(self.store.range(lower.to_vec()..upper.to_vec()).count())
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        Box::new(self.store.iter().map(|(k, v)| Ok((k.clone(), v.clone()))))
    }
}
