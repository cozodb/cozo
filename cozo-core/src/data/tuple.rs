/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::data::functions::TERMINAL_VALIDITY;
use miette::Result;
use std::cmp::Reverse;

use crate::data::memcmp::MemCmpEncoder;
use crate::data::value::{DataValue, Validity, ValidityTs};
use crate::runtime::relation::RelationId;

pub type Tuple = Vec<DataValue>;

pub(crate) type TupleIter<'a> = Box<dyn Iterator<Item = Result<Tuple>> + 'a>;

pub(crate) trait TupleT {
    fn encode_as_key(&self, prefix: RelationId) -> Vec<u8>;
}

impl<T> TupleT for T
where
    T: AsRef<[DataValue]>,
{
    fn encode_as_key(&self, prefix: RelationId) -> Vec<u8> {
        let len = self.as_ref().len();
        let mut ret = Vec::with_capacity(4 + 4 * len + 10 * len);
        let prefix_bytes = prefix.0.to_be_bytes();
        ret.extend(prefix_bytes);
        for val in self.as_ref().iter() {
            ret.encode_datavalue(val);
        }
        ret
    }
}

pub fn decode_tuple_from_key(key: &[u8], size_hint: usize) -> Tuple {
    let mut remaining = &key[ENCODED_KEY_MIN_LEN..];
    let mut ret = Vec::with_capacity(size_hint);
    while !remaining.is_empty() {
        let (val, next) = DataValue::decode_from_key(remaining);
        ret.push(val);
        remaining = next;
    }
    ret
}

const DEFAULT_SIZE_HINT: usize = 16;

/// Check if the tuple key passed in should be a valid return for a validity query.
///
/// Returns two elements, the first element contains `Some(tuple)` if the key should be included
/// in the return set and `None` otherwise,
/// the second element gives the next binary key for the seek to be used as an inclusive
/// lower bound.
pub fn check_key_for_validity(key: &[u8], valid_at: ValidityTs, size_hint: Option<usize>) -> (Option<Tuple>, Vec<u8>) {
    let mut decoded = decode_tuple_from_key(key, size_hint.unwrap_or(DEFAULT_SIZE_HINT));
    let rel_id = RelationId::raw_decode(key);
    let vld = match decoded.last().unwrap() {
        DataValue::Validity(vld) => vld,
        _ => unreachable!(),
    };
    if vld.timestamp < valid_at {
        *decoded.last_mut().unwrap() = DataValue::Validity(Validity {
            timestamp: valid_at,
            is_assert: Reverse(true),
        });
        let nxt_seek = decoded.encode_as_key(rel_id);
        (None, nxt_seek)
    } else if !vld.is_assert.0 {
        *decoded.last_mut().unwrap() = DataValue::Validity(TERMINAL_VALIDITY);
        let nxt_seek = decoded.encode_as_key(rel_id);
        (None, nxt_seek)
    } else {
        let ret = decoded.clone();
        *decoded.last_mut().unwrap() = DataValue::Validity(TERMINAL_VALIDITY);
        let nxt_seek = decoded.encode_as_key(rel_id);
        (Some(ret), nxt_seek)
    }
}

pub(crate) const ENCODED_KEY_MIN_LEN: usize = 8;
