/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fs;
use std::path::{Path, PathBuf};

use log::info;
use miette::{miette, IntoDiagnostic, Result, WrapErr};

use cozorocks::{DbBuilder, DbIter, RocksDb, Tx};

use crate::data::tuple::{check_key_for_validity, Tuple};
use crate::data::value::ValidityTs;
use crate::runtime::db::{BadDbInit, DbManifest};
use crate::runtime::relation::{decode_tuple_from_kv, extend_tuple_from_v};
use crate::storage::{Storage, StoreTx};
use crate::utils::swap_option_result;
use crate::Db;

const KEY_PREFIX_LEN: usize = 9;
const CURRENT_STORAGE_VERSION: u64 = 3;

/// Creates a RocksDB database object.
/// This is currently the fastest persistent storage and it can
/// sustain huge concurrency.
/// Supports concurrent readers and writers.
pub fn new_cozo_rocksdb(path: impl AsRef<Path>) -> Result<Db<RocksDbStorage>> {
    let builder = DbBuilder::default().path(path.as_ref());
    fs::create_dir_all(path.as_ref()).map_err(|err| {
        BadDbInit(format!(
            "cannot create directory {}: {}",
            path.as_ref().to_string_lossy(),
            err
        ))
    })?;
    let path_buf = PathBuf::from(path.as_ref());

    let is_new = {
        let mut manifest_path = path_buf.clone();
        manifest_path.push("manifest");

        if manifest_path.exists() {
            let existing: DbManifest = rmp_serde::from_slice(
                &fs::read(manifest_path)
                    .into_diagnostic()
                    .wrap_err_with(|| "when reading manifest")?,
            )
            .into_diagnostic()
            .wrap_err_with(|| "when reading manifest")?;
            assert_eq!(
                existing.storage_version, CURRENT_STORAGE_VERSION,
                "Unknown storage version {}",
                existing.storage_version
            );

            false
        } else {
            fs::write(
                manifest_path,
                rmp_serde::to_vec_named(&DbManifest {
                    storage_version: CURRENT_STORAGE_VERSION,
                })
                .into_diagnostic()
                .wrap_err_with(|| "when serializing manifest")?,
            )
            .into_diagnostic()
            .wrap_err_with(|| "when serializing manifest")?;
            true
        }
    };

    let mut store_path = path_buf.clone();
    store_path.push("data");

    let store_path = store_path
        .to_str()
        .ok_or_else(|| miette!("bad path name"))?;

    let mut options_path = path_buf.clone();
    options_path.push("options");

    let options_path = if Path::exists(&options_path) {
        info!(
            "RockDB storage engine will use options file {}",
            options_path.to_string_lossy()
        );
        options_path
            .to_str()
            .ok_or_else(|| miette!("bad path name"))?
    } else {
        ""
    };

    let db_builder = builder
        .create_if_missing(is_new)
        .use_capped_prefix_extractor(true, KEY_PREFIX_LEN)
        .use_bloom_filter(true, 9.9, true)
        .path(store_path)
        .options_path(options_path);

    let db = db_builder.build()?;

    let ret = Db::new(RocksDbStorage::new(db))?;
    ret.initialize()?;
    Ok(ret)
}

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

impl Storage<'_> for RocksDbStorage {
    type Tx = RocksDbTx;

    fn storage_kind(&self) -> &'static str {
        "rocksdb"
    }

    fn transact(&self, _write: bool) -> Result<Self::Tx> {
        let db_tx = self.db.transact().set_snapshot(true).start();
        Ok(RocksDbTx { db_tx })
    }

    fn range_compact(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        self.db.range_compact(lower, upper).into_diagnostic()
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        for result in data {
            let (key, val) = result?;
            self.db.raw_put(&key, &val)?;
        }
        Ok(())
    }
}

pub struct RocksDbTx {
    db_tx: Tx,
}

unsafe impl Sync for RocksDbTx {}

impl<'s> StoreTx<'s> for RocksDbTx {
    #[inline]
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>> {
        Ok(self.db_tx.get(key, for_update)?.map(|v| v.to_vec()))
    }

    #[inline]
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.db_tx.put(key, val)?)
    }

    fn supports_par_put(&self) -> bool {
        true
    }

    #[inline]
    fn par_put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.db_tx.put(key, val)?)
    }

    #[inline]
    fn del(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.db_tx.del(key)?)
    }

    #[inline]
    fn par_del(&self, key: &[u8]) -> Result<()> {
        Ok(self.db_tx.del(key)?)
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        while let Some(key) = inner.key()? {
            if key >= upper {
                break;
            }
            self.db_tx.del(key)?;
            inner.next();
        }
        Ok(())
    }

    #[inline]
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool> {
        Ok(self.db_tx.exists(key, for_update)?)
    }

    fn commit(&mut self) -> Result<()> {
        Ok(self.db_tx.commit()?)
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>>>
    where
        's: 'a,
    {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        Box::new(RocksDbIterator {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        })
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        let inner = self.db_tx.iterator().upper_bound(upper).start();
        Box::new(RocksDbSkipIterator {
            inner,
            upper_bound: upper.to_vec(),
            next_bound: lower.to_owned(),
            valid_at,
        })
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>
    where
        's: 'a,
    {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        Box::new(RocksDbIteratorRaw {
            inner,
            started: false,
            upper_bound: upper.to_vec(),
        })
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        let mut inner = self.db_tx.iterator().upper_bound(upper).start();
        inner.seek(lower);
        let mut count = 0;
        while let Some(k) = inner.key()? {
            if k >= upper {
                break;
            }
            count += 1;
            inner.next();
        }
        Ok(count)
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        self.range_scan(&[], &[u8::MAX])
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
                    Some(decode_tuple_from_kv(k_slice, v_slice, None))
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

pub(crate) struct RocksDbSkipIterator {
    inner: DbIter,
    upper_bound: Vec<u8>,
    next_bound: Vec<u8>,
    valid_at: ValidityTs,
}

impl RocksDbSkipIterator {
    #[inline]
    fn next_inner(&mut self) -> Result<Option<Tuple>> {
        loop {
            self.inner.seek(&self.next_bound);
            match self.inner.pair()? {
                None => return Ok(None),
                Some((k_slice, v_slice)) => {
                    if self.upper_bound.as_slice() <= k_slice {
                        return Ok(None);
                    }

                    let (ret, nxt_bound) = check_key_for_validity(k_slice, self.valid_at, None);
                    self.next_bound = nxt_bound;
                    if let Some(mut tup) = ret {
                        extend_tuple_from_v(&mut tup, v_slice);
                        return Ok(Some(tup));
                    }
                }
            }
        }
    }
}

impl Iterator for RocksDbSkipIterator {
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
