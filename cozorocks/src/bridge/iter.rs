use crate::bridge::ffi::*;
use cxx::UniquePtr;

pub struct IterBuilder {
    pub(crate) inner: UniquePtr<IterBridge>,
}

pub struct DbIter {
    pub(crate) inner: UniquePtr<IterBridge>,
}

impl IterBuilder {
    pub fn start(mut self) -> DbIter {
        self.inner.pin_mut().start();
        DbIter { inner: self.inner }
    }
    pub fn clear_bounds(&mut self) {
        self.inner.pin_mut().clear_bounds();
    }
    pub fn lower_bound(mut self, bound: &[u8]) -> Self {
        self.inner.pin_mut().set_lower_bound(bound);
        self
    }
    pub fn upper_bound(mut self, bound: &[u8]) -> Self {
        self.inner.pin_mut().set_upper_bound(bound);
        self
    }

    #[inline]
    pub fn verify_checksums(mut self, val: bool) -> Self {
        self.inner.pin_mut().verify_checksums(val);
        self
    }

    #[inline]
    pub fn fill_cache(mut self, val: bool) -> Self {
        self.inner.pin_mut().fill_cache(val);
        self
    }

    #[inline]
    pub fn tailing(mut self, val: bool) -> Self {
        self.inner.pin_mut().tailing(val);
        self
    }

    #[inline]
    pub fn total_order_seek(mut self, val: bool) -> Self {
        self.inner.pin_mut().total_order_seek(val);
        self
    }
    #[inline]
    pub fn auto_prefix_mode(mut self, val: bool) -> Self {
        self.inner.pin_mut().auto_prefix_mode(val);
        self
    }
    #[inline]
    pub fn prefix_same_as_start(mut self, val: bool) -> Self {
        self.inner.pin_mut().prefix_same_as_start(val);
        self
    }
    #[inline]
    pub fn pin_data(mut self, val: bool) -> Self {
        self.inner.pin_mut().pin_data(val);
        self
    }
}

impl DbIter {
    #[inline]
    pub fn reset(mut self) -> IterBuilder {
        self.inner.pin_mut().reset();
        IterBuilder { inner: self.inner }
    }
    #[inline]
    pub fn seek_to_start(&mut self) {
        self.inner.pin_mut().to_start();
    }
    #[inline]
    pub fn seek_to_end(&mut self) {
        self.inner.pin_mut().to_end();
    }
    #[inline]
    pub fn seek(&mut self, key: &[u8]) {
        self.inner.pin_mut().seek(key);
    }
    #[inline]
    pub fn seek_back(&mut self, key: &[u8]) {
        self.inner.pin_mut().seek_backward(key);
    }
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
    #[inline]
    pub fn next(&mut self) {
        self.inner.pin_mut().next();
    }
    #[inline]
    pub fn prev(&mut self) {
        self.inner.pin_mut().prev();
    }
    #[inline]
    pub fn status(&self) -> RocksDbStatus {
        let mut status = RocksDbStatus::default();
        self.inner.status(&mut status);
        status
    }
    #[inline]
    pub fn key(&self) -> Result<Option<&[u8]>, RocksDbStatus> {
        if self.is_valid() {
            Ok(Some(self.inner.key()))
        } else {
            let status = self.status();
            if status.is_ok() {
                Ok(None)
            } else {
                Err(status)
            }
        }
    }
    #[inline]
    pub fn val(&self) -> Result<Option<&[u8]>, RocksDbStatus> {
        if self.is_valid() {
            Ok(Some(self.inner.val()))
        } else {
            let status = self.status();
            if status.is_ok() {
                Ok(None)
            } else {
                Err(status)
            }
        }
    }
    #[inline]
    pub fn pair(&self) -> Result<Option<(&[u8], &[u8])>, RocksDbStatus> {
        if self.is_valid() {
            Ok(Some((self.inner.key(), self.inner.val())))
        } else {
            let status = self.status();
            if status.is_ok() {
                Ok(None)
            } else {
                Err(status)
            }
        }
    }
}
