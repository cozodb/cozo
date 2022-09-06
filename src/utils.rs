use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter;

use miette::{Diagnostic, LabeledSpan, SourceSpan};

#[inline(always)]
pub(crate) fn swap_option_result<T, E>(d: Result<Option<T>, E>) -> Option<Result<T, E>> {
    match d {
        Ok(Some(s)) => Some(Ok(s)),
        Ok(None) => None,
        Err(e) => Some(Err(e)),
    }
}

#[derive(Debug)]
pub(crate) struct CozoError {
    span: Option<SourceSpan>,
    span_msg: Option<String>,
    code: String,
    help: Option<String>,
    msg: String,
}

impl Display for CozoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for CozoError {}

impl Diagnostic for CozoError {
    fn code<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        Some(Box::new(&self.code))
    }
    fn help<'a>(&'a self) -> Option<Box<dyn Display + 'a>> {
        match &self.help {
            None => None,
            Some(s) => Some(Box::new(s)),
        }
    }
    fn labels(&self) -> Option<Box<dyn Iterator<Item = LabeledSpan> + '_>> {
        match &self.span {
            None => None,
            Some(span) => Some(Box::new(iter::once(LabeledSpan::new_with_span(
                self.span_msg.clone(),
                *span,
            )))),
        }
    }
}

pub(crate) fn cozo_err(code: impl Into<String>, msg: impl Into<String>) -> CozoError {
    CozoError {
        span: None,
        span_msg: None,
        code: code.into(),
        help: None,
        msg: msg.into(),
    }
}

impl CozoError {
    pub(crate) fn span(self, span: SourceSpan) -> Self {
        Self {
            span: Some(span),
            ..self
        }
    }
    #[allow(dead_code)]
    pub(crate) fn span_with_message(self, span: SourceSpan, msg: impl Into<String>) -> Self {
        Self {
            span: Some(span),
            span_msg: Some(msg.into()),
            ..self
        }
    }
    pub(crate) fn help(self, help: impl Into<String>) -> Self {
        Self {
            help: Some(help.into()),
            ..self
        }
    }
}
