/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */
#![warn(rust_2018_idioms, future_incompatible)]

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;

use cozo::Db;

#[derive(Default)]
struct Handles {
    current: AtomicI32,
    dbs: Mutex<BTreeMap<i32, Db>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles::default();
}

#[no_mangle]
pub unsafe extern "C" fn cozo_open_db(path: *const i8, db_id: &mut i32) -> *mut i8 {
    let path = match CStr::from_ptr(path).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };

    match Db::new(path) {
        Ok(db) => {
            let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
            let mut dbs = HANDLES.dbs.lock().unwrap();
            dbs.insert(id, db);
            *db_id = id;
            null_mut()
        }
        Err(err) => CString::new(format!("{}", err)).unwrap().into_raw(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cozo_close_db(id: i32) -> bool {
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&id)
    };
    db.is_some()
}

#[no_mangle]
pub unsafe extern "C" fn cozo_run_query(
    db_id: i32,
    script_raw: *const i8,
    params_raw: *const i8,
    errored: &mut bool,
) -> *mut i8 {
    let script = match CStr::from_ptr(script_raw).to_str() {
        Ok(p) => p,
        Err(err) => {
            *errored = true;
            return CString::new(format!("{}", err)).unwrap().into_raw();
        }
    };
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                *errored = true;
                return CString::new("database already closed").unwrap().into_raw();
            }
            Some(db) => db,
        }
    };
    let params_str = match CStr::from_ptr(params_raw).to_str() {
        Ok(p) => p,
        Err(err) => {
            *errored = true;
            return CString::new(format!("{}", err)).unwrap().into_raw();
        }
    };

    let params_map: serde_json::Value = match serde_json::from_str(&params_str) {
        Ok(m) => m,
        Err(_) => {
            *errored = true;
            return CString::new("the given params argument is not valid JSON")
                .unwrap()
                .into_raw();
        }
    };

    let params_arg: BTreeMap<_, _> = match params_map {
        serde_json::Value::Object(m) => m.into_iter().collect(),
        _ => {
            *errored = true;
            return CString::new("the given params argument is not a JSON map")
                .unwrap()
                .into_raw();
        }
    };
    let result = db.run_script(script, &params_arg);
    match result {
        Ok(json) => {
            let json_str = json.to_string();
            CString::new(json_str).unwrap().into_raw()
        }
        Err(err) => {
            let err_str = format!("{:?}", err);
            *errored = true;
            CString::new(err_str).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cozo_free_str(s: *mut i8) {
    let _ = CString::from_raw(s);
}
