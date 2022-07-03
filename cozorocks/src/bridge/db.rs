use crate::bridge::ffi::*;
use crate::bridge::tx::TxBuilder;
use cxx::*;
use std::borrow::Cow;
use std::ptr::null;

#[derive(Default, Debug)]
pub struct DbBuilder<'a> {
    opts: DbOpts<'a>,
}

impl<'a> Default for DbOpts<'a> {
    fn default() -> Self {
        Self {
            db_path: "",
            optimistic: false,
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
            comparator_impl: null(),
            comparator_name: "",
            comparator_different_bytes_can_be_equal: false,
            destroy_on_exit: false,
        }
    }
}

impl<'a> DbBuilder<'a> {
    pub fn path(mut self, path: &'a str) -> Self {
        self.opts.db_path = path;
        self
    }
    pub fn optimistic(mut self, val: bool) -> Self {
        self.opts.optimistic = val;
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
    pub fn use_custom_comparator(
        mut self,
        name: &'a str,
        cmp: fn(&[u8], &[u8]) -> i8,
        different_bytes_can_be_equal: bool,
    ) -> Self {
        self.opts.comparator_name = name;
        self.opts.comparator_impl = cmp as *const u8;
        self.opts.comparator_different_bytes_can_be_equal = different_bytes_can_be_equal;
        self
    }
    pub fn destroy_on_exit(mut self, destroy: bool) -> Self {
        self.opts.destroy_on_exit = destroy;
        self
    }
    pub fn build(self) -> Result<RocksDb, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        let result = open_db(&self.opts, &mut status);
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
    pub fn db_path(&self) -> Cow<str> {
        self.inner.get_db_path().to_string_lossy()
    }
    pub fn transact(&self) -> TxBuilder {
        TxBuilder {
            inner: self.inner.transact(),
        }
    }
}

unsafe impl Send for RocksDb {}
unsafe impl Sync for RocksDb {}
