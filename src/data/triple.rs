use anyhow::bail;
use std::fmt::{Display, Formatter};

use serde_derive::{Deserialize, Serialize};

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Debug, Deserialize, Serialize)]
pub enum StoreOp {
    Retract = 0,
    Assert = 1,
}

impl Display for StoreOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreOp::Retract => write!(f, "-"),
            StoreOp::Assert => write!(f, "+"),
        }
    }
}

impl StoreOp {
    pub(crate) fn is_assert(&self) -> bool {
        *self == StoreOp::Assert
    }
    pub(crate) fn is_retract(&self) -> bool {
        *self == StoreOp::Retract
    }
}

impl TryFrom<u8> for StoreOp {
    type Error = anyhow::Error;

    fn try_from(u: u8) -> Result<Self, Self::Error> {
        Ok(match u {
            0 => StoreOp::Retract,
            1 => StoreOp::Assert,
            n => bail!("unexpect tag for store op: {}", n),
        })
    }
}
