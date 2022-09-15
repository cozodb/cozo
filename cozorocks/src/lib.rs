pub use bridge::db::DbBuilder;
pub use bridge::db::RocksDb;
pub use bridge::ffi::RocksDbStatus;
pub use bridge::ffi::SnapshotBridge;
pub use bridge::ffi::StatusCode;
pub use bridge::ffi::StatusSeverity;
pub use bridge::ffi::StatusSubCode;
pub use bridge::iter::DbIter;
pub use bridge::iter::IterBuilder;
pub use bridge::tx::PinSlice;
pub use bridge::tx::Tx;
pub use bridge::tx::TxBuilder;

pub(crate) mod bridge;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CfHandle {
    Pri,
    Snd,
}

impl From<CfHandle> for usize {
    fn from(s: CfHandle) -> Self {
        match s {
            CfHandle::Pri => 0,
            CfHandle::Snd => 1,
        }
    }
}
