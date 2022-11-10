/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::Result;

use crate::data::tuple::Tuple;

pub(crate) mod rocks;
pub(crate) mod sled;
pub(crate) mod tikv;

pub(crate) trait Storage {
    type Tx: StoreTx;

    fn transact(&self) -> Result<Self::Tx>;
    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()>;
    fn range_compact(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Result<()>;
}

pub(crate) trait StoreTx {
    type ReadSlice: AsRef<[u8]>;

    type KVIter: Iterator<Item = Result<Tuple>>;
    type KVIterRaw: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>;

    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Self::ReadSlice>>;
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn del(&mut self, key: &[u8]) -> Result<()>;
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool>;
    fn commit(&mut self) -> Result<()>;
    fn range_scan(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Self::KVIter;
    fn range_scan_raw(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Self::KVIterRaw;
}
