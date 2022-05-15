use std::fmt::{Debug, Formatter};
use std::result;

#[derive(thiserror::Error, Debug)]
pub(crate) enum TypingError {
    #[error("table id not allowed: {0}")]
    InvalidTableId(u32),
}

type Result<T> = result::Result<T, TypingError>;

const MIN_TABLE_ID: u32 = 10001;

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Hash)]
pub(crate) struct TableId {
    pub(crate) in_root: bool,
    pub(crate) id: u32,
}

impl Debug for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}{}", if self.in_root { 'G' } else { 'L' }, self.id)
    }
}

impl TableId {
    pub(crate) fn new(in_root: bool, id: u32) -> Result<Self> {
        if id < MIN_TABLE_ID {
            Err(TypingError::InvalidTableId(id))
        } else {
            Ok(TableId { in_root, id })
        }
    }
    pub(crate) fn is_valid(&self) -> bool {
        self.id >= MIN_TABLE_ID
    }
}

impl From<(bool, u32)> for TableId {
    fn from((in_root, id): (bool, u32)) -> Self {
        Self { in_root, id }
    }
}

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
pub(crate) struct ColId {
    pub(crate) is_key: bool,
    pub(crate) id: usize,
}

impl Debug for ColId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, ".{}{}", if self.is_key { 'K' } else { 'D' }, self.id)
    }
}

impl From<(bool, usize)> for ColId {
    fn from((is_key, id): (bool, usize)) -> Self {
        Self { is_key, id: id }
    }
}

#[derive(Clone, Copy, PartialEq, Ord, PartialOrd, Eq)]
pub(crate) struct TupleSetIdx {
    pub(crate) is_key: bool,
    pub(crate) t_set: usize,
    pub(crate) col_idx: usize,
}

impl Debug for TupleSetIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "@{}{}{}",
            self.t_set,
            if self.is_key { 'K' } else { 'D' },
            self.col_idx
        )
    }
}
