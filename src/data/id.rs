use std::fmt::{Debug, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeZone, Utc};
use miette::Diagnostic;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::data::expr::Expr;
use crate::data::triple::StoreOp;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Deserialize, Serialize, Hash)]
pub(crate) struct Validity(pub(crate) i64);

impl Validity {
    pub(crate) const MAX: Validity = Validity(i64::MAX);
    pub(crate) const NO_HISTORY: Validity = Validity(i64::MIN + 1);
    pub(crate) const MIN: Validity = Validity(i64::MIN);
    pub(crate) fn current() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        Self(timestamp)
    }
    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        Validity(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    pub(crate) fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl From<i64> for Validity {
    fn from(i: i64) -> Self {
        Validity(i)
    }
}

#[derive(Debug, Error, Diagnostic)]
#[error("Cannot convert {0:?} to Validity")]
#[diagnostic(code(parser::bad_validity))]
#[diagnostic(help(
    "Validity can be represented by integers, or strings according to RFC2822 or RFC3339"
))]
struct BadValidityError(DataValue, #[label] SourceSpan);

impl TryFrom<Expr> for Validity {
    type Error = miette::Error;

    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        let span = expr.span();
        let value = expr.eval_to_const()?;
        if let Some(v) = value.get_int() {
            return Ok(v.into());
        }
        if let Some(s) = value.get_string() {
            let dt = DateTime::parse_from_rfc2822(s)
                .or_else(|_| DateTime::parse_from_rfc3339(s))
                .map_err(|_| BadValidityError(value, span))?;
            let sysdt: SystemTime = dt.into();
            let timestamp = sysdt.duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
            return Ok(Self(timestamp));
        }
        Err(BadValidityError(value, span).into())
    }
}

impl Debug for Validity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if *self == Validity::MIN {
            write!(f, "MIN")
        } else if *self == Validity::NO_HISTORY {
            write!(f, "NO_HISTORY")
        } else if *self == Validity::MAX {
            write!(f, "MAX")
        } else {
            let dt = Utc.timestamp(self.0 / 1_000_000, (self.0 % 1_000_000) as u32 * 1000);
            write!(f, "{}", dt.to_rfc3339())
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub(crate) struct EntityId(pub(crate) u64);

impl EntityId {
    pub(crate) const ZERO: EntityId = EntityId(0);
    pub(crate) const MAX_TEMP: EntityId = EntityId(10_000_000);
    pub(crate) const MIN_PERM: EntityId = EntityId(10_000_001);
    pub(crate) const MAX_PERM: EntityId = EntityId(0x00ff_ffff_ff00_0000);

    pub(crate) fn as_datavalue(&self) -> DataValue {
        DataValue::from(self.0 as i64)
    }

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
    pub(crate) fn is_placeholder(&self) -> bool {
        self.0 == 0
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
pub(crate) struct AttrId(pub(crate) u64);

impl AttrId {
    pub(crate) const MAX_TEMP: AttrId = AttrId(10_000_000);
    pub(crate) const MIN_PERM: AttrId = AttrId(10_000_001);
    pub(crate) const MAX_PERM: AttrId = AttrId(0x00ff_ffff_ff00_0000);

    pub(crate) fn bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub(crate) fn from_bytes(b: &[u8]) -> Self {
        AttrId(u64::from_be_bytes([
            0, b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    pub(crate) fn is_perm(&self) -> bool {
        *self > Self::MAX_TEMP && *self <= Self::MAX_PERM
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
pub(crate) struct TxId(pub(crate) u64);

impl TxId {
    pub(crate) const MAX_SYS: TxId = TxId(10000);
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
