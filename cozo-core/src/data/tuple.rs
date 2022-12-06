/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use miette::Result;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::value::DataValue;
use crate::runtime::relation::RelationId;

pub type Tuple = Vec<DataValue>;

pub(crate) type TupleIter<'a> = Box<dyn Iterator<Item = Result<Tuple>> + 'a>;

pub(crate) trait TupleT {
    fn encode_as_key(&self, prefix: RelationId) -> Vec<u8>;
    fn decode_from_key(key: &[u8]) -> Self;
}

impl TupleT for  Tuple {
    fn encode_as_key(&self, prefix: RelationId) -> Vec<u8> {
        let len = self.len();
        let mut ret = Vec::with_capacity(4 + 4 * len + 10 * len);
        let prefix_bytes = prefix.0.to_be_bytes();
        ret.extend(prefix_bytes);
        for val in self.iter() {
            ret.encode_datavalue(val);
        }
        ret
    }
    fn decode_from_key(key: &[u8]) -> Self {
        let mut remaining = &key[ENCODED_KEY_MIN_LEN..];
        let mut ret = vec![];
        while !remaining.is_empty() {
            let (val, next) = DataValue::decode_from_key(remaining);
            ret.push(val);
            remaining = next;
        }
        ret
    }
}
pub(crate) const ENCODED_KEY_MIN_LEN: usize = 8;