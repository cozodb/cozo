use std::fmt::{Debug, Formatter};
use log::error;

use cozorocks::{DbIter, RawRocksDb, RocksDbStatus};

use crate::data::tuple::{EncodedTuple, Tuple};
use crate::data::value::DataValue;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct ThrowawayId(pub(crate) u32);

impl Debug for ThrowawayId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

#[derive(Clone)]
pub struct ThrowawayArea {
    pub(crate) db: RawRocksDb,
    pub(crate) id: ThrowawayId,
}

impl Debug for ThrowawayArea {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Throwaway<{}>", self.id.0)
    }
}

impl ThrowawayArea {
    pub(crate) fn put(&self, tuple: &Tuple, epoch: u32) -> Result<(), RocksDbStatus> {
        let key_encoded = tuple.encode_as_key_for_epoch(self.id, epoch);
        self.db.put(&key_encoded, &[])
    }
    pub(crate) fn exists(&self, tuple: &Tuple, epoch: u32) -> Result<bool, RocksDbStatus> {
        let key_encoded = tuple.encode_as_key_for_epoch(self.id, epoch);
        self.db.exists(&key_encoded)
    }
    pub fn scan_all(&self) -> impl Iterator<Item = anyhow::Result<Tuple>> {
        self.scan_all_for_epoch(0)
    }
    pub fn scan_all_for_epoch(
        &self,
        epoch: u32,
    ) -> impl Iterator<Item = anyhow::Result<Tuple>> {
        let (lower, upper) = EncodedTuple::bounds_for_prefix_and_epoch(self.id, epoch);
        let mut it = self
            .db
            .iterator()
            .upper_bound(&upper)
            .prefix_same_as_start(true)
            .start();
        it.seek(&lower);
        ThrowawayIter { it, started: false }
    }
    pub(crate) fn scan_prefix(
        &self,
        prefix: &Tuple,
    ) -> impl Iterator<Item = anyhow::Result<Tuple>> {
        self.scan_prefix_for_epoch(prefix, 0)
    }
    pub(crate) fn scan_prefix_for_epoch(
        &self,
        prefix: &Tuple,
        epoch: u32,
    ) -> impl Iterator<Item = anyhow::Result<Tuple>> {
        let mut upper = prefix.0.clone();
        upper.push(DataValue::Bottom);
        let upper = Tuple(upper);
        let upper = upper.encode_as_key_for_epoch(self.id, epoch);
        let lower = prefix.encode_as_key_for_epoch(self.id, epoch);
        let mut it = self
            .db
            .iterator()
            .upper_bound(&upper)
            .prefix_same_as_start(true)
            .start();
        it.seek(&lower);
        ThrowawayIter { it, started: false }
    }
}

struct ThrowawayIter {
    it: DbIter,
    started: bool,
}

impl Iterator for ThrowawayIter {
    type Item = anyhow::Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
        } else {
            self.it.next();
        }
        match self.it.pair() {
            Err(e) => Some(Err(e.into())),
            Ok(None) => None,
            Ok(Some((k_slice, _v_slice))) => match EncodedTuple(k_slice).decode() {
                Err(e) => Some(Err(e)),
                Ok(t) => Some(Ok(t)),
            },
        }
    }
}

impl Drop for ThrowawayArea {
    fn drop(&mut self) {
        let (lower, upper) = EncodedTuple::bounds_for_prefix(self.id);
        if let Err(e) = self.db.range_del(&lower, &upper) {
            error!("{}", e);
        }
    }
}
