/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

use std::collections::BTreeSet;
use std::io::Write;
use std::str::FromStr;

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use regex::Regex;

use crate::data::value::{DataValue, Num, RegexWrapper, UuidWrapper};

const INIT_TAG: u8 = 0x00;
const NULL_TAG: u8 = 0x01;
const FALSE_TAG: u8 = 0x02;
const TRUE_TAG: u8 = 0x03;
const NUM_TAG: u8 = 0x05;
const STR_TAG: u8 = 0x06;
const BYTES_TAG: u8 = 0x07;
const UUID_TAG: u8 = 0x08;
const REGEX_TAG: u8 = 0x09;
const LIST_TAG: u8 = 0x0A;
const SET_TAG: u8 = 0x0B;
const GUARD_TAG: u8 = 0xFE;
const BOT_TAG: u8 = 0xFF;

pub(crate) trait MemCmpEncoder: Write {
    fn encode_datavalue(&mut self, v: &DataValue) {
        match v {
            DataValue::Null => self.write_u8(NULL_TAG).unwrap(),
            DataValue::Bool(false) => self.write_u8(FALSE_TAG).unwrap(),
            DataValue::Bool(true) => self.write_u8(TRUE_TAG).unwrap(),
            DataValue::Num(n) => {
                self.write_u8(NUM_TAG).unwrap();
                self.encode_num(*n);
            }
            DataValue::Str(s) => {
                self.write_u8(STR_TAG).unwrap();
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
                self.encode_bytes(s_rest)
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
            DataValue::Guard => self.write_u8(GUARD_TAG).unwrap(),
            DataValue::Bot => self.write_u8(BOT_TAG).unwrap(),
        }
    }
    fn encode_num(&mut self, v: Num) {
        let f = v.get_float();
        let u = order_encode_f64(f);
        self.write_u64::<BigEndian>(u).unwrap();
        match v {
            Num::I(i) => {
                // self.write_u8(0b0).unwrap();
                let i_lsb = (order_encode_i64(i) as u16) & 0x7fff;
                self.write_u16::<BigEndian>(i_lsb).unwrap();
            }
            Num::F(_) => {
                self.write_u8(0x80).unwrap();
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
        if *tag == 0x80 {
            return (Num::F(f), remaining);
        }
        let (subtag, remaining) = remaining.split_first().unwrap();
        let n = f as i64;
        let mut n_bytes = n.to_be_bytes();
        n_bytes[6] &= 0x80;
        n_bytes[6] |= tag;
        n_bytes[7] = *subtag;
        let n = BigEndian::read_i64(&n_bytes);
        (Num::I(n), remaining)
    }
}

impl DataValue {
    pub(crate) fn decode_from_key(bs: &[u8]) -> (Self, &[u8]) {
        let (tag, remaining) = bs.split_first().unwrap();
        match *tag {
            NULL_TAG => (DataValue::Null, remaining),
            FALSE_TAG => (DataValue::Bool(false), remaining),
            TRUE_TAG => (DataValue::Bool(true), remaining),
            NUM_TAG => {
                let (n, remaining) = Num::decode_from_key(remaining);
                (DataValue::Num(n), remaining)
            }
            STR_TAG => {
                let (bytes, remaining) = decode_bytes(remaining);
                let s = unsafe { String::from_utf8_unchecked(bytes) };
                (DataValue::Str(s.into()), remaining)
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
            GUARD_TAG => (DataValue::Guard, remaining),
            BOT_TAG => (DataValue::Bot, remaining),
            _ => unreachable!("{:?}", bs),
        }
    }
}

impl<T: Write> MemCmpEncoder for T {}

#[cfg(test)]
mod tests {
    use smartstring::SmartString;

    use crate::data::memcmp::{decode_bytes, MemCmpEncoder};
    use crate::data::value::DataValue;

    #[test]
    fn encode_decode_bytes() {
        let target = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit...";
        for i in 0..target.len() {
            let bs = &target[i..];
            let mut encoder: Vec<u8> = vec![];
            encoder.encode_bytes(bs);
            let (decoded, remaining) = decode_bytes(&encoder);
            assert!(remaining.is_empty());
            assert_eq!(bs, decoded);

            let mut encoder: Vec<u8> = vec![];
            encoder.encode_bytes(target);
            encoder.encode_bytes(bs);
            encoder.encode_bytes(bs);
            encoder.encode_bytes(target);

            let (decoded, remaining) = decode_bytes(&encoder);
            assert_eq!(&target[..], decoded);

            let (decoded, remaining) = decode_bytes(remaining);
            assert_eq!(bs, decoded);

            let (decoded, remaining) = decode_bytes(remaining);
            assert_eq!(bs, decoded);

            let (decoded, remaining) = decode_bytes(remaining);
            assert_eq!(&target[..], decoded);
            assert!(remaining.is_empty());
        }
    }

    #[test]
    fn specific_encode() {
        let mut encoder = vec![];
        encoder.encode_datavalue(&DataValue::from(2095));
        // println!("e1 {:?}", encoder);
        encoder.encode_datavalue(&DataValue::Str(SmartString::from("MSS")));
        // println!("e2 {:?}", encoder);
        let (a, remaining) = DataValue::decode_from_key(&encoder);
        // println!("r  {:?}", remaining);
        let (b, remaining) = DataValue::decode_from_key(remaining);
        assert!(remaining.is_empty());
        assert_eq!(a, DataValue::from(2095));
        assert_eq!(b, DataValue::Str(SmartString::from("MSS")));
    }

    #[test]
    fn encode_decode_datavalues() {
        let mut dv = vec![
            DataValue::Null,
            DataValue::Bool(false),
            DataValue::Bool(true),
            DataValue::from(1),
            DataValue::from(1.0),
            DataValue::from(i64::MAX),
            DataValue::from(i64::MAX - 1),
            DataValue::from(i64::MAX - 2),
            DataValue::from(i64::MIN),
            DataValue::from(i64::MIN + 1),
            DataValue::from(i64::MIN + 2),
            DataValue::from(f64::INFINITY),
            DataValue::from(f64::NEG_INFINITY),
            DataValue::List(vec![]),
        ];
        dv.push(DataValue::List(dv.clone()));
        dv.push(DataValue::List(dv.clone()));
        let mut encoded = vec![];
        let v = DataValue::List(dv);
        encoded.encode_datavalue(&v);
        let (decoded, remaining) = DataValue::decode_from_key(&encoded);
        assert!(remaining.is_empty());
        assert_eq!(decoded, v);
    }
}
