/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Ordering;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::default::Default;
use std::iter::Fuse;
use std::mem;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use itertools::Itertools;
use miette::{bail, Result};

use crate::data::tuple::Tuple;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;

/// Create a database backed by memory.
/// This is the fastest storage, but non-persistent.
/// Supports concurrent readers but only a single writer.
pub fn new_cozo_mem() -> Result<crate::Db<MemStorage>> {
    let ret = crate::Db::new(MemStorage::default())?;

    ret.initialize()?;
    Ok(ret)
}

/// The non-persistent storage
#[derive(Clone, Default)]
pub struct MemStorage {
    store: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl<'s> Storage<'s> for MemStorage {
    type Tx = MemTx<'s>;

    fn transact(&'s self, write: bool) -> Result<Self::Tx> {
        Ok(if write {
            let wtr = self.store.write().unwrap();
            MemTx::Writer(wtr, Default::default())
        } else {
            let rdr = self.store.read().unwrap();
            MemTx::Reader(rdr)
        })
    }

    fn del_range(&'s self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let store = self.store.clone();
        let lower_b = lower.to_vec();
        let upper_b = upper.to_vec();
        let closure = move || {
            let keys = {
                let rdr = store.read().unwrap();
                rdr.range(lower_b..upper_b)
                    .map(|kv| kv.0.clone())
                    .collect_vec()
            };
            let mut wtr = store.write().unwrap();
            for k in keys.iter() {
                wtr.remove(k);
            }
        };
        #[cfg(feature = "nothread")]
        closure();
        #[cfg(not(feature = "nothread"))]
        std::thread::spawn(closure);
        Ok(())
    }

    fn range_compact(&'s self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub enum MemTx<'s> {
    Reader(RwLockReadGuard<'s, BTreeMap<Vec<u8>, Vec<u8>>>),
    Writer(
        RwLockWriteGuard<'s, BTreeMap<Vec<u8>, Vec<u8>>>,
        BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ),
}

impl<'s> StoreTx<'s> for MemTx<'s> {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        Ok(match self {
            MemTx::Reader(rdr) => rdr.get(key).cloned(),
            MemTx::Writer(wtr, cache) => match cache.get(key) {
                Some(r) => r.clone(),
                None => wtr.get(key).cloned(),
            },
        })
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        match self {
            MemTx::Reader(_) => {
                bail!("write in read transaction")
            }
            MemTx::Writer(_, cache) => {
                cache.insert(key.to_vec(), Some(val.to_vec()));
                Ok(())
            }
        }
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        match self {
            MemTx::Reader(_) => {
                bail!("write in read transaction")
            }
            MemTx::Writer(_, cache) => {
                cache.insert(key.to_vec(), None);
                Ok(())
            }
        }
    }

    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        Ok(match self {
            MemTx::Reader(rdr) => rdr.contains_key(key),
            MemTx::Writer(wtr, cache) => match cache.get(key) {
                Some(r) => r.is_some(),
                None => wtr.contains_key(key),
            },
        })
    }

    fn commit(&mut self) -> Result<()> {
        match self {
            MemTx::Reader(_) => Ok(()),
            MemTx::Writer(wtr, cached) => {
                let mut cache = BTreeMap::default();
                mem::swap(&mut cache, cached);
                for (k, mv) in cache {
                    match mv {
                        None => {
                            wtr.remove(&k);
                        }
                        Some(v) => {
                            wtr.insert(k, v);
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        match self {
            MemTx::Reader(rdr) => Box::new(
                rdr.range(lower.to_vec()..upper.to_vec())
                    .map(|(k, v)| Ok(decode_tuple_from_kv(k, v))),
            ),
            MemTx::Writer(wtr, cache) => Box::new(CacheIter {
                change_iter: cache.range(lower.to_vec()..upper.to_vec()).fuse(),
                db_iter: wtr.range(lower.to_vec()..upper.to_vec()).fuse(),
                change_cache: None,
                db_cache: None,
            }),
        }
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        match self {
            MemTx::Reader(rdr) => Box::new(
                rdr.range(lower.to_vec()..upper.to_vec())
                    .map(|(k, v)| Ok((k.clone(), v.clone()))),
            ),
            MemTx::Writer(wtr, cache) => Box::new(CacheIterRaw {
                change_iter: cache.range(lower.to_vec()..upper.to_vec()).fuse(),
                db_iter: wtr.range(lower.to_vec()..upper.to_vec()).fuse(),
                change_cache: None,
                db_cache: None,
            }),
        }
    }

    fn batch_put<'a>(
        &'a mut self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()>
    where
        's: 'a,
    {
        match self {
            MemTx::Reader(_) => {
                bail!("write in read transaction")
            }
            MemTx::Writer(_, cache) => {
                for pair in data {
                    let (k, v) = pair?;
                    cache.insert(k, Some(v));
                }
                Ok(())
            }
        }
    }
}

struct CacheIterRaw<'a> {
    change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
    db_iter: Fuse<Range<'a, Vec<u8>, Vec<u8>>>,
    change_cache: Option<(&'a Vec<u8>, &'a Option<Vec<u8>>)>,
    db_cache: Option<(&'a Vec<u8>, &'a Vec<u8>)>,
}

impl CacheIterRaw<'_> {
    #[inline]
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some(kmv) = self.change_iter.next() {
                self.change_cache = Some(kmv)
            }
        }

        if self.db_cache.is_none() {
            if let Some(kv) = self.db_iter.next() {
                self.db_cache = Some(kv);
            }
        }

        Ok(())
    }

    #[inline]
    fn next_inner(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some(_), None) => {
                    let (k, cv) = self.change_cache.take().unwrap();
                    match cv {
                        None => continue,
                        Some(v) => return Ok(Some((k.clone(), v.clone()))),
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some((k.clone(), v.clone())));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        match sv {
                            None => continue,
                            Some(v) => return Ok(Some((k.clone(), v.clone()))),
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some((k.clone(), v.clone())));
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

impl Iterator for CacheIterRaw<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}

struct CacheIter<'a> {
    change_iter: Fuse<Range<'a, Vec<u8>, Option<Vec<u8>>>>,
    db_iter: Fuse<Range<'a, Vec<u8>, Vec<u8>>>,
    change_cache: Option<(&'a Vec<u8>, &'a Option<Vec<u8>>)>,
    db_cache: Option<(&'a Vec<u8>, &'a Vec<u8>)>,
}

impl CacheIter<'_> {
    #[inline]
    fn fill_cache(&mut self) -> Result<()> {
        if self.change_cache.is_none() {
            if let Some(kmv) = self.change_iter.next() {
                self.change_cache = Some(kmv)
            }
        }

        if self.db_cache.is_none() {
            if let Some(kv) = self.db_iter.next() {
                self.db_cache = Some(kv);
            }
        }

        Ok(())
    }

    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            self.fill_cache()?;
            match (&self.change_cache, &self.db_cache) {
                (None, None) => return Ok(None),
                (Some(_), None) => {
                    let (k, cv) = self.change_cache.take().unwrap();
                    match cv {
                        None => continue,
                        Some(v) => return Ok(Some(decode_tuple_from_kv(k, v))),
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some(decode_tuple_from_kv(k, v)));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        match sv {
                            None => continue,
                            Some(v) => return Ok(Some(decode_tuple_from_kv(k, v))),
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some(decode_tuple_from_kv(k, v)));
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

impl Iterator for CacheIter<'_> {
    type Item = Result<Tuple>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        swap_option_result(self.next_inner())
    }
}
