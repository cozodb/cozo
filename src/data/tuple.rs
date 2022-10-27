/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::fmt::{Debug, Formatter};

use miette::Result;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::value::DataValue;
use crate::runtime::relation::RelationId;

pub(crate) const SCRATCH_DB_KEY_PREFIX_LEN: usize = 6;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Default)]
pub(crate) struct Tuple(pub(crate) Vec<DataValue>);

impl Debug for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(&self.0).finish()
    }
}

pub(crate) type TupleIter<'a> = Box<dyn Iterator<Item = Result<Tuple>> + 'a>;

impl Tuple {
    pub(crate) fn encode_as_key(&self, prefix: RelationId) -> Vec<u8> {
        let len = self.0.len();
        let mut ret = Vec::with_capacity(4 + 4 * len + 10 * len);
        let prefix_bytes = prefix.0.to_be_bytes();
        ret.extend(prefix_bytes);
        for val in self.0.iter() {
            ret.encode_datavalue(val);
        }
        // println!("encoded as key {:?}", ret);
        ret
        // for (idx, val) in self.0.iter().enumerate() {
        //     if idx > 0 {
        //         let pos = (ret.len() as u32).to_be_bytes();
        //         for (i, u) in pos.iter().enumerate() {
        //             ret[4 * (1 + idx) + i] = *u;
        //         }
        //     }
        //     val.serialize(&mut Serializer::new(&mut ret)).unwrap();
        // }
        // ret
    }
    pub(crate) fn decode_from_key(key: &[u8]) -> Self {
        let mut remaining = &key[ENCODED_KEY_MIN_LEN..];
        let mut ret = vec![];
        while !remaining.is_empty() {
            let (val, next) = DataValue::decode_from_key(remaining);
            ret.push(val);
            remaining = next;
        }
        Tuple(ret)
    }
}
pub(crate) const ENCODED_KEY_MIN_LEN: usize = 8;
//
// #[derive(Copy, Clone, Debug)]
// pub(crate) struct EncodedTuple<'a>(pub(crate) &'a [u8]);
//
// impl<'a> From<&'a [u8]> for EncodedTuple<'a> {
//     fn from(s: &'a [u8]) -> Self {
//         EncodedTuple(s)
//     }
// }
//
// impl<'a> EncodedTuple<'a> {
//     pub(crate) fn prefix(&self) -> RelationId {
//         debug_assert!(self.0.len() >= 6, "bad data: {:x?}", self.0);
//         let id = u64::from_be_bytes([
//             0, 0, self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5],
//         ]);
//         RelationId(id)
//     }
//     pub(crate) fn arity(&self) -> usize {
//         if self.0.len() == 6 {
//             return 0;
//         }
//         debug_assert!(self.0.len() >= 8, "bad data: {:x?}", self.0);
//         u16::from_be_bytes([self.0[6], self.0[7]]) as usize
//     }
//     fn force_get(&self, idx: usize) -> DataValue {
//         let pos = if idx == 0 {
//             let arity = u16::from_be_bytes([self.0[6], self.0[7]]) as usize;
//             4 * (arity + 1)
//         } else {
//             let len_pos = (idx + 1) * 4;
//             u32::from_be_bytes([
//                 self.0[len_pos],
//                 self.0[len_pos + 1],
//                 self.0[len_pos + 2],
//                 self.0[len_pos + 3],
//             ]) as usize
//         };
//         rmp_serde::from_slice(&self.0[pos..]).unwrap()
//     }
//     pub(crate) fn get(&self, idx: usize) -> DataValue {
//         let pos = if idx == 0 {
//             4 * (self.arity() + 1)
//         } else {
//             let len_pos = (idx + 1) * 4;
//             debug_assert!(self.0.len() >= len_pos + 4, "bad data: {:x?}", self.0);
//             u32::from_be_bytes([
//                 self.0[len_pos],
//                 self.0[len_pos + 1],
//                 self.0[len_pos + 2],
//                 self.0[len_pos + 3],
//             ]) as usize
//         };
//         debug_assert!(
//             pos < self.0.len(),
//             "bad data length for data: {:x?}",
//             self.0
//         );
//         rmp_serde::from_slice(&self.0[pos..]).expect("data corruption when getting from tuple")
//     }
//
//     pub(crate) fn iter(&self) -> EncodedTupleIter<'a> {
//         EncodedTupleIter {
//             tuple: *self,
//             size: 0,
//             pos: 0,
//         }
//     }
//     pub(crate) fn decode(&self) -> Tuple {
//         Tuple(self.iter().collect())
//     }
// }
//
// pub(crate) struct EncodedTupleIter<'a> {
//     tuple: EncodedTuple<'a>,
//     size: usize,
//     pos: usize,
// }
//
// impl<'a> Iterator for EncodedTupleIter<'a> {
//     type Item = DataValue;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.size == 0 {
//             let arity = self.tuple.arity();
//             self.size = arity;
//         }
//         if self.pos == self.size {
//             None
//         } else {
//             let pos = self.pos;
//             self.pos += 1;
//             Some(self.tuple.get(pos))
//         }
//     }
// }
//
// pub(crate) fn rusty_scratch_cmp(a: &[u8], b: &[u8]) -> i8 {
//     match compare_tuple_keys(a, b) {
//         Ordering::Greater => 1,
//         Ordering::Equal => 0,
//         Ordering::Less => -1,
//     }
// }
//
//
// pub(crate) fn compare_tuple_keys(a: &[u8], b: &[u8]) -> Ordering {
//     let a = EncodedTuple(a);
//     let b = EncodedTuple(b);
//     match a.prefix().cmp(&b.prefix()) {
//         Ordering::Equal => {}
//         o => return o,
//     }
//     let a_len = a.arity();
//     let b_len = b.arity();
//     for idx in 0..min(a_len, b_len) {
//         let av = a.force_get(idx);
//         let bv = b.force_get(idx);
//         match av.cmp(&bv) {
//             Ordering::Equal => {}
//             o => return o,
//         }
//     }
//     a_len.cmp(&b_len)
// }
