/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

use std::fmt::{Debug, Formatter};

use miette::Result;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::value::DataValue;
use crate::runtime::relation::RelationId;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Default)]
pub struct Tuple(pub(crate) Vec<DataValue>);

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
        ret
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