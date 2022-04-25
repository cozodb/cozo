use std::borrow::Borrow;
use crate::relation::tuple::Tuple;
use crate::error::{CozoError, Result};
use crate::relation::typing::Typing;
use crate::relation::value::Value;

#[repr(u32)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub enum DataKind {
    DataTuple = 0,
    Node = 1,
    Edge = 2,
    Associate = 3,
    Index = 4,
    Value = 5,
    TypeAlias = 6,
}
// In storage, key layout is `[0, name, stack_depth]` where stack_depth is a non-positive number as zigzag
// Also has inverted index `[0, stack_depth, name]` for easy popping of stacks


impl<T: AsRef<[u8]>> Tuple<T> {
    pub fn data_kind(&self) -> Result<DataKind> {
        use DataKind::*;
        Ok(match self.get_prefix() {
            0 => DataTuple,
            1 => Node,
            2 => Edge,
            3 => Associate,
            4 => Index,
            5 => Value,
            6 => TypeAlias,
            v => return Err(CozoError::UndefinedDataKind(v))
        })
    }
    pub fn interpret_as_type(&self) -> Result<Typing> {
        if let Value::Text(s) = self.get(0).ok_or_else(|| CozoError::BadDataFormat(self.as_ref().to_vec()))? {
            Ok(Typing::try_from(s.borrow())?)
        } else {
            return Err(CozoError::BadDataFormat(self.as_ref().to_vec()));
        }
    }
}