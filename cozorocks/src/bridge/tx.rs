/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

use std::fmt::{Debug, Formatter};
use std::ops::Deref;

use cxx::*;

use crate::bridge::ffi::*;
use crate::bridge::iter::IterBuilder;

pub struct TxBuilder {
    pub(crate) inner: UniquePtr<TxBridge>,
}

pub struct PinSlice {
    pub(crate) inner: UniquePtr<PinnableSlice>,
}

impl Deref for PinSlice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        convert_pinnable_slice_back(&self.inner)
    }
}

impl Debug for PinSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let to_d: &[u8] = self;
        write!(f, "{:?}", to_d)
    }
}

impl TxBuilder {
    #[inline]
    pub fn start(mut self) -> Tx {
        self.inner.pin_mut().start();
        Tx { inner: self.inner }
    }
    #[inline]
    pub fn set_snapshot(mut self, val: bool) -> Self {
        self.inner.pin_mut().set_snapshot(val);
        self
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
}

pub struct Tx {
    pub(crate) inner: UniquePtr<TxBridge>,
}

impl Tx {
    #[inline]
    pub fn set_snapshot(&mut self) {
        self.inner.pin_mut().set_snapshot(true)
    }
    #[inline]
    pub fn clear_snapshot(&mut self) {
        self.inner.pin_mut().clear_snapshot()
    }
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
    #[inline]
    pub fn del(&mut self, key: &[u8]) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().del(key, &mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn get(&self, key: &[u8], for_update: bool) -> Result<Option<PinSlice>, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        let ret = self.inner.get(key, for_update, &mut status);
        match status.code {
            StatusCode::kOk => Ok(Some(PinSlice { inner: ret })),
            StatusCode::kNotFound => Ok(None),
            _ => Err(status),
        }
    }
    #[inline]
    pub fn exists(&self, key: &[u8], for_update: bool) -> Result<bool, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.exists(key, for_update, &mut status);
        match status.code {
            StatusCode::kOk => Ok(true),
            StatusCode::kNotFound => Ok(false),
            _ => Err(status),
        }
    }
    #[inline]
    pub fn commit(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().commit(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn rollback(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().rollback(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn rollback_to_save(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().rollback_to_savepoint(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn save(&mut self) {
        self.inner.pin_mut().set_savepoint();
    }
    #[inline]
    pub fn pop_save(&mut self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.pin_mut().pop_savepoint(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }
    #[inline]
    pub fn iterator(&self) -> IterBuilder {
        IterBuilder {
            inner: self.inner.iterator(),
        }
            .auto_prefix_mode(true)
    }
}
