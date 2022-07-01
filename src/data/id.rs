use serde_derive::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub struct EntityId(pub u64);

impl EntityId {
    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        EntityId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

impl Debug for EntityId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.0)
    }
}

pub(crate) const MAX_SYS_ENTITY_ID: EntityId = EntityId(1000);
pub(crate) const MAX_TEMP_ENTITY_ID: EntityId = EntityId(10_000_000);
pub(crate) const MAX_PERM_ENTITY_ID: EntityId = EntityId(0x00ff_ffff_ff00_0000);

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Deserialize, Serialize, Hash)]
pub struct AttrId(pub u32);

impl AttrId {
    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        AttrId(u32::from_be_bytes([0, b[1], b[2], b[3]]))
    }
}

impl Debug for AttrId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "a{}", self.0)
    }
}

pub(crate) const MAX_SYS_ATTR_ID: AttrId = AttrId(1000);
pub(crate) const MAX_TEMP_ATTR_ID: AttrId = AttrId(1000000);
pub(crate) const MAX_PERM_ATTR_ID: AttrId = AttrId(0x00ff_ffff);

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Deserialize, Serialize, Hash)]
pub struct TxId(pub u64);

impl TxId {
    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        TxId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

impl Debug for TxId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

pub(crate) const MAX_SYS_TX_ID: TxId = TxId(1000);
pub(crate) const MAX_USER_TX_ID: TxId = TxId(0x00ff_ffff_ffff_ffff);
