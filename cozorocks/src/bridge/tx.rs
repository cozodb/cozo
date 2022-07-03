use crate::bridge::ffi::*;
use cxx::*;
use std::ptr::null;

pub struct TxBuilder {
    pub(crate) inner: UniquePtr<TxBridge>,
}

impl TxBuilder {
    #[inline]
    pub fn start(mut self, with_snapshot: bool) -> Tx {
        if with_snapshot {
            self.inner.pin_mut().set_snapshot();
        }
        Tx { inner: self.inner }
    }
    #[inline]
    pub fn sync(mut self, val: bool) -> Self {
        set_w_opts_sync(self.inner.pin_mut().get_w_opts(), val);
        self
    }

    #[inline]
    pub fn no_slowdown(mut self, val: bool) -> Self {
        set_w_opts_no_slowdown(self.inner.pin_mut().get_w_opts(), val);
        self
    }

    #[inline]
    pub fn disable_wal(mut self, val: bool) -> Self {
        set_w_opts_disable_wal(self.inner.pin_mut().get_w_opts(), val);
        self
    }
}

pub struct Tx {
    pub(crate) inner: UniquePtr<TxBridge>,
}

impl Tx {
    #[inline]
    pub fn set_snapshot(&mut self) {
        self.inner.pin_mut().set_snapshot()
    }
}
