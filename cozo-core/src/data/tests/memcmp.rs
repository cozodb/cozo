/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use uuid::Uuid;

use crate::data::memcmp::{decode_bytes, MemCmpEncoder};
use crate::data::value::{DataValue, Num, UuidWrapper};

#[test]
fn encode_decode_num() {
    use rand::prelude::*;

    let n = i64::MAX;
    let mut collected = vec![];

    let mut test_num = |n: Num| {
        let mut encoder = vec![];
        encoder.encode_num(n);
        let (decoded, rest) = Num::decode_from_key(&encoder);
        assert_eq!(decoded, n);
        assert!(rest.is_empty());
        collected.push(encoder);
    };
    for i in 0..54 {
        for j in 0..1000 {
            let vb = (n >> i) - j;
            for v in [vb, -vb - 1] {
                test_num(Num::Int(v));
            }
        }
    }
    test_num(Num::Float(f64::INFINITY));
    test_num(Num::Float(f64::NEG_INFINITY));
    test_num(Num::Float(f64::NAN));
    for _ in 0..100000 {
        let f = (thread_rng().gen::<f64>() - 0.5) * 2.0;
        test_num(Num::Float(f));
        test_num(Num::Float(1. / f));
    }
    let mut collected_copy = collected.clone();
    collected.sort();
    collected_copy.sort_by_key(|c| Num::decode_from_key(c).0);
    assert_eq!(collected, collected_copy);
}

#[test]
fn test_encode_decode_uuid() {
    let uuid = DataValue::Uuid(UuidWrapper(
        Uuid::parse_str("dd85b19a-5fde-11ed-a88e-1774a7698039").unwrap(),
    ));
    let mut encoder = vec![];
    encoder.encode_datavalue(&uuid);
    let (decoded, remaining) = DataValue::decode_from_key(&encoder);
    assert_eq!(decoded, uuid);
    assert!(remaining.is_empty());
}

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
    encoder.encode_datavalue(&DataValue::from("MSS"));
    // println!("e2 {:?}", encoder);
    let (a, remaining) = DataValue::decode_from_key(&encoder);
    // println!("r  {:?}", remaining);
    let (b, remaining) = DataValue::decode_from_key(remaining);
    assert!(remaining.is_empty());
    assert_eq!(a, DataValue::from(2095));
    assert_eq!(b, DataValue::from("MSS"));
}

#[test]
fn encode_decode_datavalues() {
    let mut dv = vec![
        DataValue::Null,
        DataValue::from(false),
        DataValue::from(true),
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
