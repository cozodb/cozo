use std::fmt::{Debug, Formatter};

use cozorocks::{DbIter, PinSlice, RawRocksDb, RocksDbStatus};

use crate::data::tuple::{EncodedTuple, Tuple};
use crate::data::value::DataValue;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ThrowawayId(pub(crate) u32);

#[derive(Clone)]
pub(crate) struct ThrowawayArea {
    pub(crate) db: RawRocksDb,
    pub(crate) id: ThrowawayId,
}

impl Debug for ThrowawayArea {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Throwaway<{}>", self.id.0)
    }
}

impl ThrowawayArea {
    pub(crate) fn put(&self, tuple: &Tuple, value: &[u8]) -> Result<(), RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.id);
        self.db.put(&key_encoded, value)
    }
    pub(crate) fn put_if_absent(&self, tuple: &Tuple, value: &[u8]) -> Result<bool, RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.id);
        Ok(if !self.db.exists(&key_encoded)? {
            self.db.put(&key_encoded, value)?;
            true
        } else {
            false
        })
    }
    pub(crate) fn get(&self, tuple: &Tuple) -> Result<Option<PinSlice>, RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.id);
        self.db.get(&key_encoded)
    }
    pub(crate) fn exists(&self, tuple: &Tuple) -> Result<bool, RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.id);
        self.db.exists(&key_encoded)
    }
    pub(crate) fn del(&self, tuple: &Tuple) -> Result<(), RocksDbStatus> {
        let key_encoded = tuple.encode_as_key(self.id);
        self.db.del(&key_encoded)
    }
    pub(crate) fn scan_all(&self) -> impl Iterator<Item = anyhow::Result<(Tuple, Option<u32>)>> {
        let (lower, upper) = EncodedTuple::bounds_for_prefix(self.id.0);
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
    ) -> impl Iterator<Item = anyhow::Result<(Tuple, Option<u32>)>> {
        let mut upper = prefix.0.clone();
        upper.push(DataValue::Null);
        let upper = Tuple(upper);
        let upper = upper.encode_as_key(self.id);
        let lower = prefix.encode_as_key(self.id);
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
    type Item = anyhow::Result<(Tuple, Option<u32>)>;

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
                Ok(t) => {
                    let epoch = if v_slice.is_empty() {
                        None
                    } else {
                        Some(u32::from_be_bytes([
                            v_slice[0], v_slice[1], v_slice[2], v_slice[3],
                        ]))
                    };
                    Some(Ok((t, epoch)))
                }
            },
        }
    }
}

impl Drop for ThrowawayArea {
    fn drop(&mut self) {
        let (lower, upper) = EncodedTuple::bounds_for_prefix(self.id.0);
        if let Err(e) = self.db.range_del(&lower, &upper) {
            eprintln!("{}", e);
        }
    }
}
