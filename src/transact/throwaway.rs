use std::fmt::{Debug, Formatter};
use cozorocks::{DbIter, PinSlice, RawRocksDb, RocksDbStatus};

use crate::data::tuple::{EncodedTuple, Tuple};
use crate::data::value::DataValue;

pub(crate) struct ThrowawayArea {
    pub(crate) db: RawRocksDb,
    pub(crate) prefix: u32,
}

impl Debug for ThrowawayArea {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Throwaway<{}>", self.prefix)
    }
}

impl ThrowawayArea {
    pub(crate) fn put(&mut self, tuple: &Tuple, value: &[u8]) -> Result<(), RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.prefix);
        self.db.put(&key_encoded, value)
    }
    pub(crate) fn get(&self, tuple: &Tuple) -> Result<Option<PinSlice>, RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.prefix);
        self.db.get(&key_encoded)
    }
    pub(crate) fn del(&mut self, tuple: &Tuple) -> Result<(), RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.prefix);
        self.db.del(&key_encoded)
    }
    pub(crate) fn scan_all(&self) -> impl Iterator<Item = anyhow::Result<(Tuple, Vec<u8>)>> {
        let (lower, upper) = EncodedTuple::bounds_for_prefix(self.prefix);
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
    ) -> impl Iterator<Item = anyhow::Result<(Tuple, Vec<u8>)>> {
        let mut upper = prefix.0.clone();
        upper.push(DataValue::Null);
        let upper = Tuple(upper);
        let upper = upper.encode_as_key(self.prefix);
        let lower = prefix.encode_as_key(self.prefix);
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
    type Item = anyhow::Result<(Tuple, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
        } else {
            self.it.next();
        }
        match self.it.pair() {
            Err(e) => Some(Err(e.into())),
            Ok(None) => None,
            Ok(Some((k_slice, v_slice))) => match EncodedTuple(k_slice).decode() {
                Err(e) => Some(Err(e)),
                Ok(t) => Some(Ok((t, v_slice.to_vec()))),
            },
        }
    }
}

impl Drop for ThrowawayArea {
    fn drop(&mut self) {
        let (lower, upper) = EncodedTuple::bounds_for_prefix(self.prefix);
        if let Err(e) = self.db.range_del(&lower, &upper) {
            eprintln!("{}", e);
        }
    }
}
