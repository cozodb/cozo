/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::collections::BTreeMap;

use miette::{IntoDiagnostic, Result};
use sled::transaction::{ConflictableTransactionError, TransactionalTree};
use sled::{Db, IVec};

use crate::data::tuple::Tuple;
use crate::storage::{Storage, StoreTx};

#[derive(Clone)]
struct SledStorage {
    db: Db,
}

impl Storage for SledStorage {
    type Tx = SledTx;

    fn transact(&self) -> Result<Self::Tx> {
        Ok(SledTx {
            db: self.db.clone(),
            changes: Default::default(),
        })
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        todo!()
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

struct SledTx {
    db: Db,
    changes: BTreeMap<Box<[u8]>, Option<Box<[u8]>>>,
}

impl StoreTx for SledTx {
    type ReadSlice = IVec;
    type KVIter = SledIter;
    type KVIterRaw = SledIterRaw;

    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Self::ReadSlice>> {
        Ok(match self.changes.get(key) {
            Some(Some(val)) => Some(IVec::from(val as &[u8])),
            Some(None) => None,
            None => self.db.get(key).into_diagnostic()?,
        })
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.changes.insert(key.into(), Some(val.into()));
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.changes.insert(key.into(), None);
        Ok(())
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        Ok(match self.changes.get(key) {
            Some(Some(_)) => true,
            Some(None) => false,
            None => self.db.get(key).into_diagnostic()?.is_some(),
        })
    }

    fn commit(&mut self) -> Result<()> {
        self.db
            .transaction(
                |db: &TransactionalTree| -> Result<(), ConflictableTransactionError> {
                    for (k, v) in &self.changes {
                        match v {
                            None => {
                                db.remove(k as &[u8])?;
                            }
                            Some(v) => {
                                db.insert(k as &[u8], v as &[u8])?;
                            }
                        }
                    }
                    Ok(())
                },
            )
            .into_diagnostic()?;
        Ok(())
    }

    fn range_scan(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Self::KVIter {
        todo!()
    }

    fn range_scan_raw(&self, lower: &[u8], upper: &[u8]) -> Self::KVIterRaw {
        todo!()
    }
}

struct SledIter {}

impl Iterator for SledIter {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct SledIterRaw {}

impl Iterator for SledIterRaw {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}