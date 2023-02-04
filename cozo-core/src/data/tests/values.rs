/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use crate::data::symb::Symbol;
use crate::data::tuple::Tuple;
use crate::data::value::DataValue;

#[test]
fn show_size() {
    dbg!(size_of::<DataValue>());
    dbg!(size_of::<Symbol>());
    dbg!(size_of::<String>());
    dbg!(size_of::<HashMap<String, String>>());
    dbg!(size_of::<BTreeMap<String, String>>());
}

#[test]
fn utf8() {
    let c = char::from_u32(0x10FFFF).unwrap();
    let mut s = String::new();
    s.push(c);
    println!("{}", s);
    println!(
        "{:b} {:b} {:b} {:b}",
        s.as_bytes()[0],
        s.as_bytes()[1],
        s.as_bytes()[2],
        s.as_bytes()[3]
    );
    dbg!(s);
}

#[test]
fn display_datavalues() {
    println!("{}", DataValue::Null);
    println!("{}", DataValue::from(true));
    println!("{}", DataValue::from(-1));
    println!("{}", DataValue::from(-1121212121.331212121));
    println!("{}", DataValue::from(f64::NAN));
    println!("{}", DataValue::from(f64::NEG_INFINITY));
    println!(
        "{}",
        DataValue::List(vec![
            DataValue::from(false),
            DataValue::from(r###"abc"你"好'啊👌"###),
            DataValue::from(f64::NEG_INFINITY),
        ])
    );
}

#[test]
fn parse_values_from_json() {
    let json_str_value = r#""a string""#;
    let str_value: DataValue = serde_json::from_str(json_str_value).unwrap();
    println!("{:?}", str_value);

    let json_num_value = "123";
    let num_value: DataValue = serde_json::from_str(json_num_value).unwrap();
    println!("{:?}", num_value);

    let json_tuple_str = r#"[true, false, 123, 456.78, "a string"]"#;
    let tuple: Tuple = serde_json::from_str(json_tuple_str).unwrap();
    assert_eq!(tuple[0].get_bool().unwrap(), true);
    assert_eq!(tuple[1].get_bool().unwrap(), false);
    assert_eq!(tuple[2].get_int().unwrap(), 123);
    assert_eq!(tuple[3].get_float().unwrap(), 456.78);
    assert_eq!(tuple[4].get_str().unwrap(), "a string");
}
