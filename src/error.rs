use thiserror::Error;
use crate::parser::Rule;

#[derive(Error, Debug)]
pub enum CozoError {
    #[error("Invalid UTF code")]
    InvalidUtfCode,

    #[error("Invalid escape sequence")]
    InvalidEscapeSequence,

    #[error("Type mismatch")]
    TypeError,

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    Parse(#[from] pest::error::Error<Rule>),
}
