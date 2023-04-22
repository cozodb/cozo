/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::io::Write;
use std::str::FromStr;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use regex::Regex;

use crate::data::value::{
    DataValue, JsonData, Num, RegexWrapper, UuidWrapper, Validity, ValidityTs, Vector,
};

const INIT_TAG: u8 = 0x00;
const NULL_TAG: u8 = 0x01;
const FALSE_TAG: u8 = 0x02;
const TRUE_TAG: u8 = 0x03;
const VEC_TAG: u8 = 0x04;
const NUM_TAG: u8 = 0x05;
const STR_TAG: u8 = 0x06;
const BYTES_TAG: u8 = 0x07;
const UUID_TAG: u8 = 0x08;
const REGEX_TAG: u8 = 0x09;
const LIST_TAG: u8 = 0x0A;
const SET_TAG: u8 = 0x0B;
const VLD_TAG: u8 = 0x0C;
const JSON_TAG: u8 = 0x0D;
const BOT_TAG: u8 = 0xFF;

const VEC_F32: u8 = 0x01;
const VEC_F64: u8 = 0x02;

const IS_FLOAT: u8 = 0b00010000;
const IS_APPROX_INT: u8 = 0b00000100;
const IS_EXACT_INT: u8 = 0b00000000;
const EXACT_INT_BOUND: i64 = 0x20_0000_0000_0000;

pub(crate) trait MemCmpEncoder: Write {
    fn encode_datavalue(&mut self, v: &DataValue) {
        match v {
            DataValue::Null => self.write_u8(NULL_TAG).unwrap(),
            DataValue::Bool(false) => self.write_u8(FALSE_TAG).unwrap(),
            DataValue::Bool(true) => self.write_u8(TRUE_TAG).unwrap(),
            DataValue::Vec(arr) => {
                self.write_u8(VEC_TAG).unwrap();
                match arr {
                    Vector::F32(a) => {
                        self.write_u8(VEC_F32).unwrap();
                        let l = a.len();
                        self.write_u64::<BigEndian>(l as u64).unwrap();
                        for el in a {
                            self.write_f32::<BigEndian>(*el).unwrap();
                        }
                    }
                    Vector::F64(a) => {
                        self.write_u8(VEC_F64).unwrap();
                        let l = a.len();
                        self.write_u64::<BigEndian>(l as u64).unwrap();
                        for el in a {
                            self.write_f64::<BigEndian>(*el).unwrap();
                        }
                    }
                }
            }
            DataValue::Num(n) => {
                self.write_u8(NUM_TAG).unwrap();
                self.encode_num(*n);
            }
            DataValue::Str(s) => {
                self.write_u8(STR_TAG).unwrap();
                self.encode_bytes(s.as_bytes());
            }
            DataValue::Json(j) => {
                self.write_u8(JSON_TAG).unwrap();
                let s = j.0.to_string();
                self.encode_bytes(s.as_bytes());
            }
            DataValue::Bytes(b) => {
                self.write_u8(BYTES_TAG).unwrap();
                self.encode_bytes(b)
            }
            DataValue::Uuid(u) => {
                self.write_u8(UUID_TAG).unwrap();
                let (s_l, s_m, s_h, s_rest) = u.0.as_fields();
                self.write_u16::<BigEndian>(s_h).unwrap();
                self.write_u16::<BigEndian>(s_m).unwrap();
                self.write_u32::<BigEndian>(s_l).unwrap();
                self.write_all(s_rest.as_ref()).unwrap();
            }
            DataValue::Regex(rx) => {
                self.write_u8(REGEX_TAG).unwrap();
                let s = rx.0.as_str().as_bytes();
                self.encode_bytes(s)
            }
            DataValue::List(l) => {
                self.write_u8(LIST_TAG).unwrap();
                for el in l {
                    self.encode_datavalue(el);
                }
                self.write_u8(INIT_TAG).unwrap()
            }
            DataValue::Set(s) => {
                self.write_u8(SET_TAG).unwrap();
                for el in s {
                    self.encode_datavalue(el);
                }
                self.write_u8(INIT_TAG).unwrap()
            }
            DataValue::Validity(vld) => {
                let ts = vld.timestamp.0 .0;
                let ts_u64 = order_encode_i64(ts);
                let ts_flipped = !ts_u64;
                self.write_u8(VLD_TAG).unwrap();
                self.write_u64::<BigEndian>(ts_flipped).unwrap();
                self.write_u8(!vld.is_assert.0 as u8).unwrap();
            }
            DataValue::Bot => self.write_u8(BOT_TAG).unwrap(),
        }
    }
    fn encode_num(&mut self, v: Num) {
        let f = v.get_float();
        let u = order_encode_f64(f);
        self.write_u64::<BigEndian>(u).unwrap();
        match v {
            Num::Int(i) => {
                if i > -EXACT_INT_BOUND && i < EXACT_INT_BOUND {
                    self.write_u8(IS_EXACT_INT).unwrap();
                } else {
                    self.write_u8(IS_APPROX_INT).unwrap();
                    let en = order_encode_i64(i);
                    self.write_u64::<BigEndian>(en).unwrap();
                }
            }
            Num::Float(_) => {
                self.write_u8(IS_FLOAT).unwrap();
            }
        }
    }

    fn encode_bytes(&mut self, key: &[u8]) {
        let len = key.len();
        let mut index = 0;
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                self.write_all(&key[index..index + ENC_GROUP_SIZE]).unwrap();
            } else {
                pad = ENC_GROUP_SIZE - remain;
                self.write_all(&key[index..]).unwrap();
                self.write_all(&ENC_ASC_PADDING[..pad]).unwrap();
            }
            self.write_all(&[ENC_MARKER - (pad as u8)]).unwrap();
            index += ENC_GROUP_SIZE;
        }
    }
}

pub fn decode_bytes(data: &[u8]) -> (Vec<u8>, &[u8]) {
    let mut key = Vec::with_capacity(data.len() / (ENC_GROUP_SIZE + 1) * ENC_GROUP_SIZE);
    let mut offset = 0;
    let chunk_len = ENC_GROUP_SIZE + 1;
    loop {
        let next_offset = offset + chunk_len;
        debug_assert!(next_offset <= data.len());
        let chunk = &data[offset..next_offset];
        offset = next_offset;

        let (&marker, bytes) = chunk.split_last().unwrap();
        let pad_size = (ENC_MARKER - marker) as usize;

        if pad_size == 0 {
            key.write_all(bytes).unwrap();
            continue;
        }
        debug_assert!(pad_size <= ENC_GROUP_SIZE);

        let (bytes, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
        key.write_all(bytes).unwrap();

        debug_assert!(!padding.iter().any(|x| *x != 0));

        return (key, &data[offset..]);
    }
}

const SIGN_MARK: u64 = 0x8000000000000000;

fn order_encode_i64(v: i64) -> u64 {
    v as u64 ^ SIGN_MARK
}

fn order_decode_i64(u: u64) -> i64 {
    (u ^ SIGN_MARK) as i64
}

fn order_encode_f64(v: f64) -> u64 {
    let u = v.to_bits();
    if v.is_sign_positive() {
        u | SIGN_MARK
    } else {
        !u
    }
}

fn order_decode_f64(u: u64) -> f64 {
    let u = if u & SIGN_MARK > 0 {
        u & (!SIGN_MARK)
    } else {
        !u
    };
    f64::from_bits(u)
}

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

impl Num {
    pub(crate) fn decode_from_key(bs: &[u8]) -> (Self, &[u8]) {
        let (float_part, remaining) = bs.split_at(8);
        let fu = BigEndian::read_u64(float_part);
        let f = order_decode_f64(fu);
        let (tag, remaining) = remaining.split_first().unwrap();
        match *tag {
            IS_FLOAT => (Num::Float(f), remaining),
            IS_EXACT_INT => (Num::Int(f as i64), remaining),
            IS_APPROX_INT => {
                let (int_part, remaining) = remaining.split_at(8);
                let iu = BigEndian::read_u64(int_part);
                let i = order_decode_i64(iu);
                (Num::Int(i), remaining)
            }
            _ => unreachable!(),
        }
        // if *tag == 0x80 {
        //     return (Num::F(f), remaining);
        // }
        // let (subtag, remaining) = remaining.split_first().unwrap();
        // let n = f as i64;
        // let mut n_bytes = n.to_be_bytes();
        // n_bytes[6] &= 0x80;
        // n_bytes[6] |= tag;
        // n_bytes[7] = *subtag;
        // let n = BigEndian::read_i64(&n_bytes);
        // (Num::I(n), remaining)
    }
}

impl DataValue {
    pub(crate) fn decode_from_key(bs: &[u8]) -> (Self, &[u8]) {
        let (tag, remaining) = bs.split_first().unwrap();
        match *tag {
            NULL_TAG => (DataValue::Null, remaining),
            FALSE_TAG => (DataValue::from(false), remaining),
            TRUE_TAG => (DataValue::from(true), remaining),
            NUM_TAG => {
                let (n, remaining) = Num::decode_from_key(remaining);
                (DataValue::Num(n), remaining)
            }
            STR_TAG => {
                let (bytes, remaining) = decode_bytes(remaining);
                let s = unsafe { String::from_utf8_unchecked(bytes) };
                (DataValue::Str(s.into()), remaining)
            }
            JSON_TAG => {
                let (bytes, remaining) = decode_bytes(remaining);
                (
                    DataValue::Json(JsonData(serde_json::from_slice(&bytes).unwrap())),
                    remaining,
                )
            }
            BYTES_TAG => {
                let (bytes, remaining) = decode_bytes(remaining);
                (DataValue::Bytes(bytes), remaining)
            }
            UUID_TAG => {
                let (uuid_data, remaining) = remaining.split_at(16);
                let s_h = BigEndian::read_u16(&uuid_data[0..2]);
                let s_m = BigEndian::read_u16(&uuid_data[2..4]);
                let s_l = BigEndian::read_u32(&uuid_data[4..8]);
                let mut s_rest = [0u8; 8];
                s_rest.copy_from_slice(&uuid_data[8..]);
                let uuid = uuid::Uuid::from_fields(s_l, s_m, s_h, &s_rest);
                (DataValue::Uuid(UuidWrapper(uuid)), remaining)
            }
            REGEX_TAG => {
                let (bytes, remaining) = decode_bytes(remaining);
                let s = unsafe { String::from_utf8_unchecked(bytes) };
                (
                    DataValue::Regex(RegexWrapper(Regex::from_str(&s).unwrap())),
                    remaining,
                )
            }
            LIST_TAG => {
                let mut collected = vec![];
                let mut remaining = remaining;
                while remaining[0] != INIT_TAG {
                    let (val, next_chunk) = DataValue::decode_from_key(remaining);
                    remaining = next_chunk;
                    collected.push(val);
                }
                (DataValue::List(collected), &remaining[1..])
            }
            SET_TAG => {
                let mut collected = BTreeSet::default();
                let mut remaining = remaining;
                while remaining[0] != INIT_TAG {
                    let (val, next_chunk) = DataValue::decode_from_key(remaining);
                    remaining = next_chunk;
                    collected.insert(val);
                }
                (DataValue::Set(collected), &remaining[1..])
            }
            VLD_TAG => {
                let (ts_flipped_bytes, rest) = remaining.split_at(8);
                let ts_flipped = BigEndian::read_u64(ts_flipped_bytes);
                let ts_u64 = !ts_flipped;
                let ts = order_decode_i64(ts_u64);
                let (is_assert_byte, rest) = rest.split_first().unwrap();
                let is_assert = *is_assert_byte == 0;
                (
                    DataValue::Validity(Validity {
                        timestamp: ValidityTs(Reverse(ts)),
                        is_assert: Reverse(is_assert),
                    }),
                    rest,
                )
            }
            BOT_TAG => (DataValue::Bot, remaining),
            VEC_TAG => {
                let (t_tag, remaining) = remaining.split_first().unwrap();
                let (len_bytes, mut rest) = remaining.split_at(8);
                let len = BigEndian::read_u64(len_bytes) as usize;
                match *t_tag {
                    VEC_F32 => {
                        let mut res_arr = ndarray::Array1::zeros(len);
                        for mut row in res_arr.axis_iter_mut(ndarray::Axis(0)) {
                            let (f_bytes, next_chunk) = rest.split_at(4);
                            rest = next_chunk;
                            let f = BigEndian::read_f32(f_bytes);
                            row.fill(f);
                        }
                        (DataValue::Vec(Vector::F32(res_arr)), rest)
                    }
                    VEC_F64 => {
                        let mut res_arr = ndarray::Array1::zeros(len);
                        for mut row in res_arr.axis_iter_mut(ndarray::Axis(0)) {
                            let (f_bytes, next_chunk) = rest.split_at(8);
                            rest = next_chunk;
                            let f = BigEndian::read_f64(f_bytes);
                            row.fill(f);
                        }
                        (DataValue::Vec(Vector::F64(res_arr)), rest)
                    }
                    _ => unreachable!(),
                }
            }
            _ => unreachable!("{:?}", bs),
        }
    }
}

impl<T: Write> MemCmpEncoder for T {}
