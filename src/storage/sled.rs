/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::cmp::Ordering;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::iter::Fuse;
use std::marker::PhantomData;
use std::thread;

use miette::{IntoDiagnostic, Result};
use sled::transaction::{ConflictableTransactionError, TransactionalTree};
use sled::{Db, IVec, Iter};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;

#[derive(Clone)]
struct SledStorage<'a> {
    db: Db,
    _phantom: PhantomData<&'a [u8]>,
}

impl<'a> Storage<'a> for SledStorage<'a> {
    type Tx = SledTx<'a>;

    fn transact(&'a self) -> Result<Self::Tx> {
        Ok(SledTx {
            db: self.db.clone(),
            changes: Default::default(),
            _phantom: Default::default(),
        })
    }

    fn del_range(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let db = self.db.clone();
        let lower_v = lower.to_vec();
        let upper_v = upper.to_vec();
        thread::spawn(move || -> Result<()> {
            for k_res in db.range(lower_v..upper_v).keys() {
                db.remove(k_res.into_diagnostic()?).into_diagnostic()?;
            }
            Ok(())
        });
        Ok(())
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

struct SledTx<'a> {
    db: Db,
    changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    _phantom: PhantomData<&'a [u8]>,
}

impl<'a> StoreTx<'a> for SledTx<'a> {
    type ReadSlice = IVec;
    type KVIter = SledIter<'a>;
    type KVIterRaw = SledIterRaw<'a>;

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

    fn range_scan(&'a self, lower: &[u8], upper: &[u8]) -> Self::KVIter {
        let change_iter = self.changes.range(lower.to_vec()..upper.to_vec()).fuse();
        let db_iter = self.db.range(lower..upper).fuse();
        SledIter {
            change_iter,
            db_iter,
            change_cache: None,
            db_cache: None,
        }
    }

    fn range_scan_raw(&'a self, lower: &[u8], upper: &[u8]) -> Self::KVIterRaw {
        let change_iter = self.changes.range(lower.to_vec()..upper.to_vec()).fuse();
        let db_iter = self.db.range(lower..upper).fuse();
        SledIterRaw {
            change_iter,
            db_iter,
            change_cache: None,
            db_cache: None,
        }
    }
}

struct SledIter<'a> {
    change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
    db_iter: Fuse<Iter>,
    change_cache: Option<(Vec<u8>, Option<Vec<u8>>)>,
    db_cache: Option<(IVec, IVec)>,
}

impl<'a> SledIter<'a> {
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some((k, v)) = self.change_iter.next() {
                self.change_cache = Some((k.to_vec(), v.clone().into()))
            }
        }

        if self.db_cache.is_none() {
            if let Some(res) = self.db_iter.next() {
                self.db_cache = Some(res.into_diagnostic()?);
            }
        }

        Ok(())
    }

    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some((_, None)), None) => {
                    self.change_cache.take();
                    continue;
                }
                (Some((_, Some(_))), None) => {
                    let (k, sv) = self.change_cache.take().unwrap();
                    let v = sv.unwrap();
                    return Ok(Some(decode_tuple_from_kv(&k, &v)));
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some(decode_tuple_from_kv(&k, &v)));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.as_slice().cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        if sv.is_none() {
                            continue;
                        } else {
                            return Ok(Some(decode_tuple_from_kv(&k, &sv.unwrap())));
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some(decode_tuple_from_kv(&k, &v)));
                    }
                    Ordering::Equal => {
                        self.db_cache.take();
                        continue;
                    }
                },
            }
        }
    }
}

impl<'a> Iterator for SledIter<'a> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

struct SledIterRaw<'a> {
    change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
    db_iter: Fuse<Iter>,
    change_cache: Option<(Vec<u8>, Option<Vec<u8>>)>,
    db_cache: Option<(IVec, IVec)>,
}

impl<'a> SledIterRaw<'a> {
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some((k, v)) = self.change_iter.next() {
                self.change_cache = Some((k.to_vec(), v.clone().into()))
            }
        }

        if self.db_cache.is_none() {
            if let Some(res) = self.db_iter.next() {
                self.db_cache = Some(res.into_diagnostic()?);
            }
        }

        Ok(())
    }

    fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some((_, None)), None) => {
                    self.change_cache.take();
                    continue;
                }
                (Some((_, Some(_))), None) => {
                    let (k, sv) = self.change_cache.take().unwrap();
                    let v = sv.unwrap();
                    return Ok(Some((k, v)));
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some((k.to_vec(), v.to_vec())));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.as_slice().cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        if sv.is_none() {
                            continue;
                        } else {
                            return Ok(Some((k, sv.unwrap())));
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some((k.to_vec(), v.to_vec())));
                    }
                    Ordering::Equal => {
                        self.db_cache.take();
                        continue;
                    }
                },
            }
        }
    }
}

impl<'a> Iterator for SledIterRaw<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
