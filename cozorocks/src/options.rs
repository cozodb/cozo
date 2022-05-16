use crate::bridge::*;
use cxx::{SharedPtr, UniquePtr};
use std::ops::Deref;
use std::pin::Pin;

pub struct RustComparatorPtr(UniquePtr<RustComparator>);

unsafe impl Send for RustComparatorPtr {}
unsafe impl Sync for RustComparatorPtr {}

impl RustComparatorPtr {
    #[inline]
    pub fn new(name: &str, cmp: fn(&[u8], &[u8]) -> i8, diff_bytes_can_equal: bool) -> Self {
        Self(new_rust_comparator(name, cmp, diff_bytes_can_equal))
    }
}

impl Deref for RustComparatorPtr {
    type Target = RustComparator;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct OptionsPtr(UniquePtr<Options>);

impl Deref for OptionsPtr {
    type Target = Options;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct OptionsPtrShared(SharedPtr<Options>);

impl Deref for OptionsPtrShared {
    type Target = Options;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl OptionsPtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut Options> {
        self.0.pin_mut()
    }
    #[inline]
    pub fn make_shared(self) -> OptionsPtrShared {
        OptionsPtrShared(make_shared_options(self.0))
    }
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
    pub fn increase_parallelism(&mut self, n_threads: u32) -> &mut Self {
        increase_parallelism(self.pin_mut(), n_threads);
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

impl ReadOptionsPtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut ReadOptions> {
        self.0.pin_mut()
    }
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

impl WriteOptionsPtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut WriteOptions> {
        self.0.pin_mut()
    }
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

impl PTxnOptionsPtr {
    #[inline]
    pub fn pin_mut(&mut self) -> Pin<&mut TransactionOptions> {
        self.0.pin_mut()
    }
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
    type Target = OptimisticTransactionOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl OTxnOptionsPtr {
    #[inline]
    pub fn new(cmp: &RustComparatorPtr) -> Self {
        Self(new_optimistic_transaction_options(cmp))
    }
}

#[derive(Clone)]
pub struct PTxnDbOptionsPtr(SharedPtr<TransactionDBOptions>);

impl Deref for PTxnDbOptionsPtr {
    type Target = TransactionDBOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PTxnDbOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_tdb_options())
    }
}

#[derive(Clone)]
pub struct OTxnDbOptionsPtr(SharedPtr<OptimisticTransactionDBOptions>);

impl Deref for OTxnDbOptionsPtr {
    type Target = OptimisticTransactionDBOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl OTxnDbOptionsPtr {
    #[inline]
    pub fn default() -> Self {
        Self(new_odb_options())
    }
}

pub enum TransactOptions {
    Pessimistic(PTxnOptionsPtr),
    Optimistic(OTxnOptionsPtr),
}

#[derive(Clone)]
pub enum TDbOptions {
    Pessimistic(PTxnDbOptionsPtr),
    Optimistic(OTxnDbOptionsPtr),
}
