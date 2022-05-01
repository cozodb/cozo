use std::result;
use std::time::SystemTimeError;
use thiserror::Error;
use cozorocks::BridgeError;
use crate::parser::Rule;
use crate::relation::data::DataKind;
use crate::relation::tuple::OwnTuple;
use crate::relation::value::{StaticValue};

#[derive(Error, Debug)]
pub enum CozoError {
    #[error("Invalid UTF code")]
    InvalidUtfCode,

    #[error("Invalid escape sequence")]
    InvalidEscapeSequence,

//     #[error("Type mismatch")]
//     TypeError,

    #[error("Reserved identifier")]
    ReservedIdent,

    //     #[error("The requested name exists")]
//     NameConflict,
//
    #[error("Undefined type '{0}'")]
    UndefinedType(String),

    #[error("Undefined data kind {0}")]
    UndefinedDataKind(u32),

    #[error("Unexpected data kind {0:?}")]
    UnexpectedDataKind(DataKind),

    #[error("Logic error: {0}")]
    LogicError(String),

    #[error("Key conflict: {0:?}")]
    KeyConflict(OwnTuple),

    #[error("Key not found: {0:?}")]
    KeyNotFound(OwnTuple),

    #[error("Bad data format {0:?}")]
    BadDataFormat(Vec<u8>),

    #[error("Duplicate names in {0:?}")]
    DuplicateNames(Vec<String>),

    #[error("Undefined parameter {0}")]
    UndefinedParam(String),
//
//     #[error("Undefined table")]
//     UndefinedTable,
//
//     #[error("Undefined parameter")]
//     UndefinedParam,
//
//     #[error("Value required")]
//     ValueRequired,
//
//     #[error("Incompatible value")]
//     IncompatibleValue,
//
//     #[error("Wrong type")]
//     WrongType,
//
//     #[error("Cannot have global edge between local nodes")]
//     IncompatibleEdge,
//
//     #[error("Unexpected index columns found")]
//     UnexpectedIndexColumns,
//
//     #[error("Database already closed")]
//     DatabaseClosed,

    #[error("InvalidArgument")]
    InvalidArgument,

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    Parse(#[from] pest::error::Error<Rule>),

    #[error("Not null constraint violated")]
    NotNullViolated(StaticValue),

    #[error("Type mismatch")]
    TypeMismatch,

    //     #[error(transparent)]
//     Storage(#[from] cozo_rocks::BridgeStatus),
//
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Session error")]
    SessionErr,

    #[error("Poisoned locks")]
    Poisoned,

    #[error(transparent)]
    SysTime(#[from] SystemTimeError),

    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    #[error(transparent)]
    Bridge(#[from] BridgeError),
}

pub type Result<T> = result::Result<T, CozoError>;