/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::Result;

use crate::data::tuple::Tuple;

pub(crate) mod rocks;
pub(crate) mod sled;
pub(crate) mod tikv;
// pub(crate) mod re;

pub trait Storage {
    type Tx: StoreTx;

    fn transact(&self, write: bool) -> Result<Self::Tx>;
    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()>;
    fn range_compact(&self, lower: &[u8], upper: &[u8]) -> Result<()>;
}

pub trait StoreTx {
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn del(&mut self, key: &[u8]) -> Result<()>;
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool>;
    fn commit(&mut self) -> Result<()>;
    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>;
    fn range_scan_raw<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;
}
