/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use miette::Result;

use crate::data::tuple::Tuple;

pub(crate) mod mem;
pub(crate) mod rocks;
pub(crate) mod sled;
pub(crate) mod sqlite;
pub(crate) mod tikv;
// pub(crate) mod re;

/// Swappable storage trait for Cozo's storage engine
pub trait Storage<'s> {
    /// The associated transaction type used by this engine
    type Tx: StoreTx<'s>;

    /// Create a transaction object. Write ops will only be called when `write == true`.
    fn transact(&'s self, write: bool) -> Result<Self::Tx>;

    /// Delete a range. It is ok to return immediately and do the deletion in
    /// the background. It is guaranteed that no keys within the deleted range
    /// will be accessed in any way by any transaction again.
    fn del_range(&'s self, lower: &[u8], upper: &[u8]) -> Result<()>;

    /// Compact the key range. Can be a no-op if the storage engine does not
    /// have the concept of compaction.
    fn range_compact(&'s self, lower: &[u8], upper: &[u8]) -> Result<()>;
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
    /// The implementation must call [`decode_tuple_from_kv`](crate::decode_tuple_from_kv) to obtain
    /// a decoded tuple in the loop of the iterator.
    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a;

    /// Scan on a range and return the raw results.
    /// `lower` is inclusive whereas `upper` is exclusive.
    fn range_scan_raw<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a;

    /// Put multiple key-value pairs into the database.
    /// The default implementation just calls `put` repeatedly.
    /// Implement if there is a more efficient way.
    fn batch_put(
        &mut self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>,
    ) -> Result<()> {
        for pair in data {
            let (k, v) = pair?;
            self.put(&k, &v)?;
        }
        Ok(())
    }
}
