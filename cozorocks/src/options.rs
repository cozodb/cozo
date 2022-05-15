use crate::bridge::*;
use cxx::UniquePtr;
use std::ops::{Deref, DerefMut};

pub struct RustComparatorPtr(UniquePtr<RustComparator>);

impl RustComparatorPtr {
    #[inline]
    pub fn new(name: &str, cmp: fn(&[u8], &[u8]) -> i8, diff_bytes_can_equal: bool) -> Self {
        Self(new_rust_comparator(name, cmp, diff_bytes_can_equal))
    }
}

impl Deref for RustComparatorPtr {
    type Target = UniquePtr<RustComparator>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct OptionsPtr(UniquePtr<Options>);

impl Deref for OptionsPtr {
    type Target = UniquePtr<Options>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_options())
    }
    #[inline]
    pub fn prepare_for_bulk_load(&mut self) -> &mut Self {
        prepare_for_bulk_load(self.pin_mut());
        self
    }
    #[inline]
    pub fn increase_parallelism(&mut self) -> &mut Self {
        increase_parallelism(self.pin_mut());
        self
    }
    #[inline]
    pub fn optimize_level_style_compaction(&mut self) -> &mut Self {
        optimize_level_style_compaction(self.pin_mut());
        self
    }
    #[inline]
    pub fn set_create_if_missing(&mut self, v: bool) -> &mut Self {
        set_create_if_missing(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_comparator(&mut self, cmp: &RustComparatorPtr) -> &mut Self {
        set_comparator(self.pin_mut(), cmp);
        self
    }
    #[inline]
    pub fn set_paranoid_checks(&mut self, v: bool) -> &mut Self {
        set_paranoid_checks(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_bloom_filter(&mut self, bits_per_key: f64, whole_key_filtering: bool) -> &mut Self {
        set_bloom_filter(self.pin_mut(), bits_per_key, whole_key_filtering);
        self
    }
    #[inline]
    pub fn set_capped_prefix_extractor(&mut self, cap_len: usize) -> &mut Self {
        set_capped_prefix_extractor(self.pin_mut(), cap_len);
        self
    }
    #[inline]
    pub fn set_fixed_prefix_extractor(&mut self, prefix_len: usize) -> &mut Self {
        set_fixed_prefix_extractor(self.pin_mut(), prefix_len);
        self
    }
    #[inline]
    pub fn set_enable_blob_files(&mut self, v: bool) -> &mut Self {
        set_enable_blob_files(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_enable_blob_garbage_collection(&mut self, v: bool) -> &mut Self {
        set_enable_blob_garbage_collection(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_min_blob_size(&mut self, size: u64) -> &mut Self {
        set_min_blob_size(self.pin_mut(), size);
        self
    }
    #[inline]
    pub fn set_blob_file_size(&mut self, size: u64) -> &mut Self {
        set_blob_file_size(self.pin_mut(), size);
        self
    }
}

pub struct ReadOptionsPtr(UniquePtr<ReadOptions>);

impl Deref for ReadOptionsPtr {
    type Target = UniquePtr<ReadOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ReadOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ReadOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_read_options())
    }
    #[inline]
    pub fn set_verify_checksums(&mut self, v: bool) -> &mut Self {
        set_verify_checksums(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_total_order_seek(&mut self, v: bool) -> &mut Self {
        set_total_order_seek(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_prefix_same_as_start(&mut self, v: bool) -> &mut Self {
        set_prefix_same_as_start(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_auto_prefix_mode(&mut self, v: bool) -> &mut Self {
        set_auto_prefix_mode(self.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_txn_snapshot(&mut self, txn: &TransactionBridge) -> bool {
        TransactionBridge::set_readoption_snapshot_to_current(txn, self.pin_mut())
    }
}

pub struct WriteOptionsPtr(pub(crate) UniquePtr<WriteOptions>);

impl Deref for WriteOptionsPtr {
    type Target = UniquePtr<WriteOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WriteOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl WriteOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_write_options())
    }
    #[inline]
    pub fn set_disable_wal(&mut self, v: bool) -> &mut Self {
        set_disable_wal(self.pin_mut(), v);
        self
    }
}

pub struct FlushOptionsPtr(UniquePtr<FlushOptions>);

impl Deref for FlushOptionsPtr {
    type Target = UniquePtr<FlushOptions>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FlushOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_flush_options())
    }
    #[inline]
    pub fn set_allow_write_stall(&mut self, v: bool) -> &mut Self {
        set_allow_write_stall(self.0.pin_mut(), v);
        self
    }
    #[inline]
    pub fn set_flush_wait(&mut self, v: bool) -> &mut Self {
        set_flush_wait(self.0.pin_mut(), v);
        self
    }
}

pub struct PTxnOptionsPtr(pub(crate) UniquePtr<TransactionOptions>);

impl Deref for PTxnOptionsPtr {
    type Target = UniquePtr<TransactionOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PTxnOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl PTxnOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_transaction_options())
    }
    #[inline]
    pub fn set_deadlock_detect(&mut self, v: bool) -> &mut Self {
        set_deadlock_detect(self.pin_mut(), v);
        self
    }
}

pub struct OTxnOptionsPtr(pub(crate) UniquePtr<OptimisticTransactionOptions>);

impl Deref for OTxnOptionsPtr {
    type Target = UniquePtr<OptimisticTransactionOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OTxnOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OTxnOptionsPtr {
    #[inline]
    pub fn new(cmp: &RustComparatorPtr) -> Self {
        Self(new_optimistic_transaction_options(cmp))
    }
}

pub struct PTxnDBOptionsPtr(UniquePtr<TransactionDBOptions>);

impl Deref for PTxnDBOptionsPtr {
    type Target = UniquePtr<TransactionDBOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PTxnDBOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl PTxnDBOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_tdb_options())
    }
}

pub struct OTxnDBOptionsPtr(UniquePtr<OptimisticTransactionDBOptions>);

impl Deref for OTxnDBOptionsPtr {
    type Target = UniquePtr<OptimisticTransactionDBOptions>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for OTxnDBOptionsPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl OTxnDBOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_odb_options())
    }
}

pub enum TransactOptions {
    Pessimistic(PTxnOptionsPtr),
    Optimistic(OTxnOptionsPtr),
}

pub enum TDBOptions {
    Pessimistic(PTxnDBOptionsPtr),
    Optimistic(OTxnDBOptionsPtr),
}
