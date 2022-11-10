/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::{IntoDiagnostic, Result};

use cozorocks::{DbIter, PinSlice, RocksDb, Tx};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;

#[derive(Clone)]
pub(crate) struct RocksDbStorage {
    db: RocksDb,
}

impl RocksDbStorage {
    pub(crate) fn new(db: RocksDb) -> Self {
        Self { db }
    }
}

impl Storage for RocksDbStorage {
    type Tx = RocksDbTx;

    fn transact(&self) -> Result<Self::Tx> {
        let db_tx = self.db.transact().set_snapshot(true).start();
        Ok(RocksDbTx { db_tx })
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        Ok(self.db.range_del(lower, upper)?)
    }

    fn range_compact(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        self.db.range_compact(lower, upper).into_diagnostic()
    }
}

pub(crate) struct RocksDbTx {
    db_tx: Tx,
}

impl StoreTx for RocksDbTx {
    type ReadSlice = PinSlice;
    type KVIter = RocksDbIterator;
    type KVIterRaw = RocksDbIteratorRaw;

    #[inline]
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Self::ReadSlice>> {
        Ok(self.db_tx.get(key, for_update)?)
    }

    #[inline]
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.db_tx.put(key, val)?)
    }

    #[inline]
    fn del(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.db_tx.del(key)?)
    }

    #[inline]
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool> {
        Ok(self.db_tx.exists(key, for_update)?)
    }

    fn commit(&mut self) -> Result<()> {
        Ok(self.db_tx.commit()?)
    }

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Self::KVIter {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        RocksDbIterator {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        }
    }

    fn range_scan_raw(&self, lower: &[u8], upper: &[u8]) -> Self::KVIterRaw {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        RocksDbIteratorRaw {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        }
    }
}

pub(crate) struct RocksDbIterator {
    inner: DbIter,
    started: bool,
    upper_bound: Vec<u8>,
}

impl RocksDbIterator {
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        if self.started {
            self.inner.next()
        } else {
            self.started = true;
        }
        Ok(match self.inner.pair()? {
            None => None,
            Some((k_slice, v_slice)) => {
                if self.upper_bound.as_slice() <= k_slice {
                    None
                } else {
                    Some(decode_tuple_from_kv(k_slice, v_slice))
                }
            }
        })
    }
}

impl Iterator for RocksDbIterator {
    type Item = Result<Tuple>;
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

pub(crate) struct RocksDbIteratorRaw {
    inner: DbIter,
    started: bool,
    upper_bound: Vec<u8>,
}

impl RocksDbIteratorRaw {
    fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.started {
            self.inner.next()
        } else {
            self.started = true;
        }
        Ok(match self.inner.pair()? {
            None => None,
            Some((k_slice, v_slice)) => {
                if self.upper_bound.as_slice() <= k_slice {
                    None
                } else {
                    Some((k_slice.to_vec(), v_slice.to_vec()))
                }
            }
        })
    }
}

impl Iterator for RocksDbIteratorRaw {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
