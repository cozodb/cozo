use serde_derive::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum StoreOpError {
    #[error("unexpected value for StoreOp: {0}")]
    UnexpectedValue(u8),
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub enum StoreOp {
    Retract = 0,
    Assert = 1,
}

impl StoreOp {
    pub(crate) fn is_assert(&self) -> bool{
        *self == StoreOp::Assert
    }
    pub(crate) fn is_retract(&self) -> bool{
        *self == StoreOp::Retract
    }

}

impl TryFrom<u8> for StoreOp {
    type Error = StoreOpError;

    fn try_from(u: u8) -> Result<Self, Self::Error> {
        match u {
            0 => Ok(StoreOp::Retract),
            1 => Ok(StoreOp::Assert),
            n => Err(StoreOpError::UnexpectedValue(n)),
        }
    }
}
