use crate::bridge::{BridgeStatus, StatusBridgeCode, StatusCode, StatusSeverity, StatusSubCode};
use std::fmt::Debug;
use std::fmt::{Display, Formatter};

pub(crate) type Result<T> = std::result::Result<T, BridgeError>;

impl std::fmt::Display for BridgeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BridgeStatus({}, {}, {}, {})",
            self.code, self.subcode, self.severity, self.bridge_code
        )
    }
}

#[derive(Debug)]
pub struct BridgeError {
    pub(crate) status: BridgeStatus,
}

impl Display for BridgeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BridgeError({}, {}, {}, {})",
            self.status.code, self.status.subcode, self.status.severity, self.status.bridge_code
        )
    }
}

impl std::error::Error for BridgeError {}

impl Default for BridgeStatus {
    #[inline]
    fn default() -> Self {
        BridgeStatus {
            code: StatusCode::kOk,
            subcode: StatusSubCode::kNone,
            severity: StatusSeverity::kNoError,
            bridge_code: StatusBridgeCode::OK,
        }
    }
}

impl BridgeStatus {
    #[inline]
    pub(crate) fn check_err<T>(self, data: T) -> Result<T> {
        let err: Option<BridgeError> = self.into();
        match err {
            Some(e) => Err(e),
            None => Ok(data),
        }
    }
}

impl From<BridgeStatus> for Option<BridgeError> {
    #[inline]
    fn from(s: BridgeStatus) -> Self {
        if s.severity == StatusSeverity::kNoError
            && s.bridge_code == StatusBridgeCode::OK
            && s.code == StatusCode::kOk
        {
            None
        } else {
            Some(BridgeError { status: s })
        }
    }
}
