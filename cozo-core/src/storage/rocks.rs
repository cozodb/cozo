/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::{IntoDiagnostic, Result};

use cozorocks::{DbIter, RocksDb, Tx};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;

/// RocksDB storage engine
#[derive(Clone)]
pub struct RocksDbStorage {
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

pub struct RocksDbTx {
    db_tx: Tx,
}

impl StoreTx for RocksDbTx {
    #[inline]
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>> {
        Ok(self.db_tx.get(key, for_update)?.map(|v| v.to_vec()))
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

    fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Box<dyn Iterator<Item = Result<Tuple>>> {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        Box::new(RocksDbIterator {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        })
    }

    fn range_scan_raw(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>> {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        Box::new(RocksDbIteratorRaw {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        })
    }
}

pub(crate) struct RocksDbIterator {
    inner: DbIter,
    started: bool,
    upper_bound: Vec<u8>,
}

impl RocksDbIterator {
    #[inline]
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
                    // upper bound is exclusive
                    Some(decode_tuple_from_kv(k_slice, v_slice))
                }
            }
        })
    }
}

impl Iterator for RocksDbIterator {
    type Item = Result<Tuple>;
    #[inline]
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
    #[inline]
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
                    // upper bound is exclusive
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
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
