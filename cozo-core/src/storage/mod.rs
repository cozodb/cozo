/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Itertools;
use miette::Result;

use crate::data::tuple::Tuple;
use crate::decode_tuple_from_kv;

pub(crate) mod mem;
#[cfg(feature = "storage-rocksdb")]
pub(crate) mod rocks;
#[cfg(feature = "storage-sled")]
pub(crate) mod sled;
#[cfg(feature = "storage-sqlite")]
pub(crate) mod sqlite;
#[cfg(feature = "storage-tikv")]
pub(crate) mod tikv;
// pub(crate) mod re;

/// Swappable storage trait for Cozo's storage engine
pub trait Storage<'s> {
    /// The associated transaction type used by this engine
    type Tx: StoreTx<'s>;

    /// Returns a string that identifies the storage kind
    fn storage_kind(&self) -> &'static str;

    /// Create a transaction object. Write ops will only be called when `write == true`.
    fn transact(&'s self, write: bool) -> Result<Self::Tx>;

    /// Delete a range. It is ok to return immediately and do the deletion in
    /// the background. It is guaranteed that no keys within the deleted range
    /// will be accessed in any way by any transaction again.
    fn del_range(&'s self, lower: &[u8], upper: &[u8]) -> Result<()>;

    /// Compact the key range. Can be a no-op if the storage engine does not
    /// have the concept of compaction.
    fn range_compact(&'s self, lower: &[u8], upper: &[u8]) -> Result<()>;

    /// Put multiple key-value pairs into the database.
    /// No duplicate data will be sent, and the order data come in is strictly ascending.
    /// There will be no other access to the database while this function is running.
    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()>;
}

/// Trait for the associated transaction type of a storage engine.
/// A transaction needs to guarantee MVCC semantics for all operations.
pub trait StoreTx<'s> {
    /// Get a key. If `for_update` is `true` (only possible in a write transaction),
    /// then the database needs to guarantee that `commit()` can only succeed if
    /// the key has not been modified outside the transaction.
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair into the storage. In case of existing key,
    /// the storage engine needs to overwrite the old value.
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Delete a key-value pair from the storage.
    fn del(&mut self, key: &[u8]) -> Result<()>;

    /// Check if a key exists. If `for_update` is `true` (only possible in a write transaction),
    /// then the database needs to guarantee that `commit()` can only succeed if
    /// the key has not been modified outside the transaction.
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool>;

    /// Commit a transaction. Must return an `Err` if MVCC consistency cannot be guaranteed,
    /// and discard all changes introduced by this transaction.
    fn commit(&mut self) -> Result<()>;

    /// Scan on a range. `lower` is inclusive whereas `upper` is exclusive.
    /// The default implementation calls [`range_scan_owned`](Self::range_scan) and converts the results.
    ///
    /// The implementation must call [`decode_tuple_from_kv`](crate::decode_tuple_from_kv) to obtain
    /// a decoded tuple in the loop of the iterator.
    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        let it = self.range_scan(lower, upper);
        Box::new(it.map_ok(|(k, v)| decode_tuple_from_kv(&k, &v)))
    }

    /// Scan on a range and return the raw results.
    /// `lower` is inclusive whereas `upper` is exclusive.
    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a;

    /// Scan for all rows. The rows are required to be in ascending order.
    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a;
}
