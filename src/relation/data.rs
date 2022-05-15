use crate::error::{CozoError, Result};
use crate::relation::tuple::Tuple;
use crate::relation::typing::Typing;
use std::borrow::Borrow;
//
// #[repr(u32)]
// #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
// pub enum DataKind {
//     Data = 0,
//     Node = 1,
//     Edge = 2,
//     Assoc = 3,
//     Index = 4,
//     Val = 5,
//     Type = 6,
//     Empty = u32::MAX,
// }
// // In storage, key layout is `[0, name, stack_depth]` where stack_depth is a non-positive number as zigzag
// // Also has inverted index `[0, stack_depth, name]` for easy popping of stacks
//
// pub const EMPTY_DATA: [u8; 4] = u32::MAX.to_be_bytes();
//
// impl<T: AsRef<[u8]>> Tuple<T> {
//     pub fn data_kind(&self) -> Result<DataKind> {
//         use DataKind::*;
//         Ok(match self.get_prefix() {
//             0 => Data,
//             1 => Node,
//             2 => Edge,
//             3 => Assoc,
//             4 => Index,
//             5 => Val,
//             6 => Type,
//             u32::MAX => Empty,
//             v => return Err(CozoError::UndefinedDataKind(v)),
//         })
//     }
//     pub fn interpret_as_type(&self) -> Result<Typing> {
//         let text = self
//             .get_text(0)
//             .ok_or_else(|| CozoError::BadDataFormat(self.as_ref().to_vec()))?;
//         Typing::try_from(text.borrow())
//     }
// }
