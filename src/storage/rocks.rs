/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::Result;

use crate::storage::{Storage, StoreTx};

struct RocksDbStorage;

impl Storage for RocksDbStorage {
    type Tx = RocksDbTx;

    fn tx(&self) -> miette::Result<Self::Tx> {
        todo!()
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> miette::Result<()> {
        todo!()
    }
}

struct RocksDbTx;

impl StoreTx for RocksDbTx {
    type ReadSlice = Vec<u8>;
    type IterSlice = Vec<u8>;
    type KeyIter = RocksDbKeyIter;
    type KeyValueIter = RocksDbIter;

    fn get(&self, key: &[u8], for_update: bool) -> miette::Result<Option<Self::ReadSlice>> {
        todo!()
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> miette::Result<()> {
        todo!()
    }

    fn del(&mut self, key: &[u8]) -> miette::Result<()> {
        todo!()
    }

    fn exists(&self, key: &[u8], for_update: bool) -> miette::Result<bool> {
        todo!()
    }

    fn commit(&mut self) -> miette::Result<()> {
        todo!()
    }

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Self::KeyValueIter {
        todo!()
    }

    fn range_key_scan(&self, lower: &[u8], upper: &[u8]) -> Self::KeyIter {
        todo!()
    }
}

struct RocksDbKeyIter;

impl Iterator for RocksDbKeyIter {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct RocksDbIter;

impl Iterator for RocksDbIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}