/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crossbeam::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use std::cmp::Ordering;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::default::Default;
use std::iter::Fuse;
use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use itertools::Itertools;
use miette::{bail, Result};

use crate::data::tuple::{check_key_for_validity, Tuple};
use crate::data::value::ValidityTs;
use crate::runtime::relation::{decode_tuple_from_kv, extend_tuple_from_v};
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
#[derive(Default, Clone)]
pub struct MemStorage {
    store: Arc<ShardedLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl<'s> Storage<'s> for MemStorage {
    type Tx = MemTx<'s>;

    fn storage_kind(&self) -> &'static str {
        "mem"
    }

    fn transact(&'s self, write: bool) -> Result<Self::Tx> {
        Ok(if write {
            let wtr = self.store.write().unwrap();
            MemTx::Writer(wtr, Default::default())
        } else {
            let rdr = self.store.read().unwrap();
            MemTx::Reader(rdr)
        })
    }

    fn range_compact(&'s self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        let mut store = self.store.write().unwrap();
        for pair in data {
            let (k, v) = pair?;
            store.insert(k, v);
        }
        Ok(())
    }
}

pub enum MemTx<'s> {
    Reader(ShardedLockReadGuard<'s, BTreeMap<Vec<u8>, Vec<u8>>>),
    Writer(
        ShardedLockWriteGuard<'s, BTreeMap<Vec<u8>, Vec<u8>>>,
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

    fn supports_par_put(&self) -> bool {
        false
    }

    fn par_put(&self, _key: &[u8], _val: &[u8]) -> Result<()> {
        panic!()
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

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        match self {
            MemTx::Reader(_) => {
                bail!("write in read transaction")
            }
            MemTx::Writer(ref mut wtr, _) => {
                let keys = wtr
                    .range(lower.to_vec()..upper.to_vec())
                    .map(|kv| kv.0.clone())
                    .collect_vec();
                for k in keys.iter() {
                    wtr.remove(k);
                }
            }
        }

        Ok(())
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
                    .map(|(k, v)| Ok(decode_tuple_from_kv(k, v, None))),
            ),
            MemTx::Writer(wtr, cache) => Box::new(CacheIter {
                change_iter: cache.range(lower.to_vec()..upper.to_vec()).fuse(),
                db_iter: wtr.range(lower.to_vec()..upper.to_vec()).fuse(),
                change_cache: None,
                db_cache: None,
            }),
        }
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        match self {
            MemTx::Reader(stored) => Box::new(
                SkipIterator {
                    inner: stored,
                    upper: upper.to_vec(),
                    valid_at,
                    next_bound: lower.to_vec(),
                    size_hint: None,
                }
                .map(Ok),
            ),
            MemTx::Writer(stored, delta) => Box::new(
                SkipDualIterator {
                    stored,
                    delta,
                    upper: upper.to_vec(),
                    valid_at,
                    next_bound: lower.to_vec(),
                }
                .map(Ok),
            ),
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

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        Ok(match self {
            MemTx::Reader(rdr) => rdr.range(lower.to_vec()..upper.to_vec()).count(),
            MemTx::Writer(wtr, cache) => (CacheIterRaw {
                change_iter: cache.range(lower.to_vec()..upper.to_vec()).fuse(),
                db_iter: wtr.range(lower.to_vec()..upper.to_vec()).fuse(),
                change_cache: None,
                db_cache: None,
            })
            .count(),
        })
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        match self {
            MemTx::Reader(rdr) => Box::new(rdr.iter().map(|(k, v)| Ok((k.clone(), v.clone())))),
            MemTx::Writer(wtr, cache) => Box::new(CacheIterRaw {
                change_iter: cache.iter().fuse(),
                db_iter: wtr.iter().fuse(),
                change_cache: None,
                db_cache: None,
            }),
        }
    }
}

struct CacheIterRaw<'a, C, T>
where
    C: Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a,
    T: Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
{
    change_iter: C,
    db_iter: T,
    change_cache: Option<(&'a Vec<u8>, &'a Option<Vec<u8>>)>,
    db_cache: Option<(&'a Vec<u8>, &'a Vec<u8>)>,
}

impl<'a, C, T> CacheIterRaw<'a, C, T>
where
    C: Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a,
    T: Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
{
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

impl<'a, C, T> Iterator for CacheIterRaw<'a, C, T>
where
    C: Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a,
    T: Iterator<Item = (&'a Vec<u8>, &'a Vec<u8>)>,
{
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
                        Some(v) => return Ok(Some(decode_tuple_from_kv(k, v, None))),
                    }
                }
                (None, Some(_)) => {
                    let (k, v) = self.db_cache.take().unwrap();
                    return Ok(Some(decode_tuple_from_kv(k, v, None)));
                }
                (Some((ck, _)), Some((dk, _))) => match ck.cmp(dk) {
                    Ordering::Less => {
                        let (k, sv) = self.change_cache.take().unwrap();
                        match sv {
                            None => continue,
                            Some(v) => return Ok(Some(decode_tuple_from_kv(k, v, None))),
                        }
                    }
                    Ordering::Greater => {
                        let (k, v) = self.db_cache.take().unwrap();
                        return Ok(Some(decode_tuple_from_kv(k, v, None)));
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

/// Keep an eye on https://github.com/rust-lang/rust/issues/49638
pub(crate) struct SkipIterator<'a> {
    pub(crate) inner: &'a BTreeMap<Vec<u8>, Vec<u8>>,
    pub(crate) upper: Vec<u8>,
    pub(crate) valid_at: ValidityTs,
    pub(crate) next_bound: Vec<u8>,
    pub(crate) size_hint: Option<usize>,
}

impl<'a> Iterator for SkipIterator<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let nxt = self
                .inner
                .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                    Bound::Included(&self.next_bound),
                    Bound::Excluded(&self.upper),
                ))
                .next();
            match nxt {
                None => return None,
                Some((candidate_key, candidate_val)) => {
                    let (ret, nxt_bound) =
                        check_key_for_validity(candidate_key, self.valid_at, self.size_hint);
                    self.next_bound = nxt_bound;
                    if let Some(mut nk) = ret {
                        extend_tuple_from_v(&mut nk, candidate_val);
                        return Some(nk);
                    }
                }
            }
        }
    }
}

struct SkipDualIterator<'a> {
    stored: &'a BTreeMap<Vec<u8>, Vec<u8>>,
    delta: &'a BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    upper: Vec<u8>,
    valid_at: ValidityTs,
    next_bound: Vec<u8>,
}

impl<'a> Iterator for SkipDualIterator<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let stored_nxt = self
                .stored
                .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                    Bound::Included(&self.next_bound),
                    Bound::Excluded(&self.upper),
                ))
                .next();
            let delta_nxt = self
                .delta
                .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                    Bound::Included(&self.next_bound),
                    Bound::Excluded(&self.upper),
                ))
                .next();
            let (candidate_key, candidate_val) = match (stored_nxt, delta_nxt) {
                (None, None) => return None,
                (None, Some((delta_key, maybe_delta_val))) => match maybe_delta_val {
                    None => {
                        let (_, nxt_seek) = check_key_for_validity(delta_key, self.valid_at, None);
                        self.next_bound = nxt_seek;
                        continue;
                    }
                    Some(delta_val) => (delta_key, delta_val),
                },
                (Some((stored_key, stored_val)), None) => (stored_key, stored_val),
                (Some((stored_key, stored_val)), Some((delta_key, maybe_delta_val))) => {
                    if stored_key < delta_key {
                        (stored_key, stored_val)
                    } else {
                        match maybe_delta_val {
                            None => {
                                let (_, nxt_seek) =
                                    check_key_for_validity(delta_key, self.valid_at, None);
                                self.next_bound = nxt_seek;
                                continue;
                            }
                            Some(delta_val) => (delta_key, delta_val),
                        }
                    }
                }
            };
            let (ret, nxt_bound) = check_key_for_validity(candidate_key, self.valid_at, None);
            self.next_bound = nxt_bound;
            if let Some(mut nk) = ret {
                extend_tuple_from_v(&mut nk, candidate_val);
                return Some(nk);
            }
        }
    }
}
