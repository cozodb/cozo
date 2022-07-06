use crate::data::triple::StoreOp;
use serde_derive::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub struct EntityId(pub u64);

impl EntityId {
    pub(crate) const MAX_SYS: EntityId = EntityId(1000);
    pub(crate) const MAX_TEMP: EntityId = EntityId(10_000_000);
    pub(crate) const MIN_PERM: EntityId = EntityId(10_000_001);
    pub(crate) const MAX_PERM: EntityId = EntityId(0x00ff_ffff_ff00_0000);

    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        EntityId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    pub(crate) fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub(crate) fn is_perm(&self) -> bool {
        *self >= Self::MIN_PERM
    }
}

impl From<u64> for EntityId {
    fn from(u: u64) -> Self {
        EntityId(u)
    }
}

impl Debug for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Deserialize, Serialize, Hash)]
pub struct AttrId(pub u64);

impl AttrId {
    pub(crate) const MAX_SYS: AttrId = AttrId(1000);
    pub(crate) const MAX_TEMP: AttrId = AttrId(10_000_000);
    pub(crate) const MIN_PERM: AttrId = AttrId(10_000_001);
    pub(crate) const MAX_PERM: AttrId = AttrId(0x00ff_ffff_ff00_0000);

    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        AttrId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

impl From<u64> for AttrId {
    fn from(u: u64) -> Self {
        AttrId(u)
    }
}

impl Debug for AttrId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "a{}", self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Deserialize, Serialize, Hash)]
pub struct TxId(pub u64);

impl TxId {
    pub(crate) const ZERO: TxId = TxId(0);
    pub(crate) const NO_HISTORY: TxId = TxId(1000);
    pub(crate) const MAX_SYS: TxId = TxId(10000);
    pub(crate) const MIN_USER: TxId = TxId(10001);
    pub(crate) const MAX_USER: TxId = TxId(0x00ff_ffff_ffff_ffff);

    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        TxId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub(crate) fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub(crate) fn bytes_with_op(&self, op: StoreOp) -> [u8; 8] {
        let mut bytes = self.0.to_be_bytes();
        bytes[0] = op as u8;
        bytes
    }
}

impl From<u64> for TxId {
    fn from(u: u64) -> Self {
        TxId(u)
    }
}

impl Debug for TxId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}
