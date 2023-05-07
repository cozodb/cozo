/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde_json::json;
pub(crate) use serde_json::Value as JsonValue;

use crate::data::value::{DataValue, Num, Vector};
use crate::JsonData;

impl From<JsonValue> for DataValue {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => DataValue::Null,
            JsonValue::Bool(b) => DataValue::Bool(b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => DataValue::from(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::from(f),
                    None => DataValue::from(n.to_string()),
                },
            },
            JsonValue::String(s) => DataValue::from(s),
            JsonValue::Array(arr) => DataValue::List(arr.iter().map(DataValue::from).collect()),
            JsonValue::Object(d) => DataValue::Json(JsonData(JsonValue::Object(d))),
        }
    }
}

impl<'a> From<&'a JsonValue> for DataValue {
    fn from(v: &'a JsonValue) -> Self {
        match v {
            JsonValue::Null => DataValue::Null,
            JsonValue::Bool(b) => DataValue::Bool(*b),
            JsonValue::Number(n) => match n.as_i64() {
                Some(i) => DataValue::from(i),
                None => match n.as_f64() {
                    Some(f) => DataValue::from(f),
                    None => DataValue::from(n.to_string()),
                },
            },
            JsonValue::String(s) => DataValue::Str(s.into()),
            JsonValue::Array(arr) => DataValue::List(arr.iter().map(DataValue::from).collect()),
            JsonValue::Object(d) => DataValue::Json(JsonData(JsonValue::Object(d.clone()))),
        }
    }
}

impl From<DataValue> for JsonValue {
    fn from(v: DataValue) -> Self {
        match v {
            DataValue::Null => JsonValue::Null,
            DataValue::Bool(b) => JsonValue::Bool(b),
            DataValue::Num(Num::Int(i)) => JsonValue::Number(i.into()),
            DataValue::Num(Num::Float(f)) => {
                if f.is_finite() {
                    json!(f)
                } else if f.is_nan() {
                    json!(())
                } else if f.is_infinite() {
                    if f.is_sign_negative() {
                        json!("NEGATIVE_INFINITY")
                    } else {
                        json!("INFINITY")
                    }
                } else {
                    unreachable!()
                }
            }
            DataValue::Str(t) => JsonValue::String(t.into()),
            DataValue::Bytes(bytes) => JsonValue::String(STANDARD.encode(bytes)),
            DataValue::List(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            DataValue::Bot => panic!("found bottom"),
            DataValue::Set(l) => {
                JsonValue::Array(l.iter().map(|v| JsonValue::from(v.clone())).collect())
            }
            DataValue::Regex(r) => {
                json!(r.0.as_str())
            }
            DataValue::Uuid(u) => {
                json!(u.0)
            }
            DataValue::Vec(arr) => match arr {
                Vector::F32(a) => json!(a.as_slice().unwrap()),
                Vector::F64(a) => json!(a.as_slice().unwrap()),
            },
            DataValue::Validity(v) => {
                json!([v.timestamp.0, v.is_assert])
            }
            DataValue::Json(j) => j.0,
        }
    }
}
