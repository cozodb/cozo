/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cxx::*;

use crate::bridge::ffi::*;
use crate::bridge::tx::TxBuilder;

#[derive(Default, Clone)]
pub struct DbBuilder<'a> {
    pub opts: DbOpts<'a>,
}

impl<'a> Default for DbOpts<'a> {
    fn default() -> Self {
        Self {
            db_path: "",
            options_path: "",
            prepare_for_bulk_load: false,
            increase_parallelism: 0,
            optimize_level_style_compaction: false,
            create_if_missing: false,
            paranoid_checks: true,
            enable_blob_files: false,
            min_blob_size: 0,
            blob_file_size: 1 << 28,
            enable_blob_garbage_collection: false,
            use_bloom_filter: false,
            bloom_filter_bits_per_key: 0.0,
            bloom_filter_whole_key_filtering: false,
            use_capped_prefix_extractor: false,
            capped_prefix_extractor_len: 0,
            use_fixed_prefix_extractor: false,
            fixed_prefix_extractor_len: 0,
            destroy_on_exit: false,
            block_cache_size: 0,
        }
    }
}

impl<'a> DbBuilder<'a> {
    pub fn path(mut self, path: &'a str) -> Self {
        self.opts.db_path = path;
        self
    }
    pub fn options_path(mut self, options_path: &'a str) -> Self {
        self.opts.options_path = options_path;
        self
    }
    pub fn prepare_for_bulk_load(mut self, val: bool) -> Self {
        self.opts.prepare_for_bulk_load = val;
        self
    }
    pub fn increase_parallelism(mut self, val: usize) -> Self {
        self.opts.increase_parallelism = val;
        self
    }
    pub fn optimize_level_style_compaction(mut self, val: bool) -> Self {
        self.opts.optimize_level_style_compaction = val;
        self
    }
    pub fn create_if_missing(mut self, val: bool) -> Self {
        self.opts.create_if_missing = val;
        self
    }
    pub fn paranoid_checks(mut self, val: bool) -> Self {
        self.opts.paranoid_checks = val;
        self
    }
    pub fn enable_blob_files(
        mut self,
        enable: bool,
        min_blob_size: usize,
        blob_file_size: usize,
        garbage_collection: bool,
    ) -> Self {
        self.opts.enable_blob_files = enable;
        self.opts.min_blob_size = min_blob_size;
        self.opts.blob_file_size = blob_file_size;
        self.opts.enable_blob_garbage_collection = garbage_collection;
        self
    }
    pub fn use_bloom_filter(
        mut self,
        enable: bool,
        bits_per_key: f64,
        whole_key_filtering: bool,
    ) -> Self {
        self.opts.use_bloom_filter = enable;
        self.opts.bloom_filter_bits_per_key = bits_per_key;
        self.opts.bloom_filter_whole_key_filtering = whole_key_filtering;
        self
    }
    pub fn use_capped_prefix_extractor(mut self, enable: bool, len: usize) -> Self {
        self.opts.use_capped_prefix_extractor = enable;
        self.opts.capped_prefix_extractor_len = len;
        self
    }
    pub fn use_fixed_prefix_extractor(mut self, enable: bool, len: usize) -> Self {
        self.opts.use_fixed_prefix_extractor = enable;
        self.opts.fixed_prefix_extractor_len = len;
        self
    }
    pub fn build(self) -> Result<RocksDb, RocksDbStatus> {
        let mut status = RocksDbStatus::default();

        let result = open_db(
            &self.opts,
            &mut status,
        );
        if status.is_ok() {
            Ok(RocksDb { inner: result })
        } else {
            Err(status)
        }
    }
}

#[derive(Clone)]
pub struct RocksDb {
    inner: SharedPtr<RocksDbBridge>,
}

impl RocksDb {
    pub fn db_path(&self) -> std::string::String {
        self.inner.get_db_path().to_string_lossy().to_string()
    }
    pub fn transact(&self) -> TxBuilder {
        TxBuilder {
            inner: self.inner.transact(),
        }
    }
    #[inline]
    pub fn range_del(&self, lower: &[u8], upper: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.del_range(lower, upper, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn range_compact(&self, lower: &[u8], upper: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.compact_range(lower, upper, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    pub fn get_sst_writer(&self, path: &str) -> Result<SstWriter, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        let ret = self.inner.get_sst_writer(path, &mut status);
        if status.is_ok() {
            Ok(SstWriter { inner: ret })
        } else {
            Err(status)
        }
    }
    pub fn ingest_sst_file(&self, path: &str) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.ingest_sst(path, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
}

pub struct SstWriter {
    inner: UniquePtr<SstFileWriterBridge>,
}

impl SstWriter {
    #[inline]
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().put(key, val, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    pub fn finish(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().finish(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
}

unsafe impl Send for RocksDb {}

unsafe impl Sync for RocksDb {}
