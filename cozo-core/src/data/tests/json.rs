/*
 *  Copyright 2022, The Cozo Project Authors.
 *
 *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 *  If a copy of the MPL was not distributed with this file,
 *  You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 */

use serde_json::json;

use crate::data::json::JsonValue;
use crate::data::value::DataValue;

#[test]
fn bad_values() {
    println!("{}", json!(f64::INFINITY));
    println!("{}", JsonValue::from(DataValue::from(f64::INFINITY)));
    println!("{}", JsonValue::from(DataValue::from(f64::NEG_INFINITY)));
    println!("{}", JsonValue::from(DataValue::from(f64::NAN)));
}
