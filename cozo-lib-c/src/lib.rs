/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#![warn(rust_2018_idioms, future_incompatible)]
#![allow(clippy::missing_safety_doc)]

use std::collections::BTreeMap;
use std::ffi::{c_char, CStr, CString};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;

use cozo::*;

struct Handles {
    current: AtomicI32,
    dbs: Mutex<BTreeMap<i32, DbInstance>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles {
        current: Default::default(),
        dbs: Mutex::new(Default::default())
    };
}

/// Open a database.
///
/// `engine`:  which storage engine to use, can be "mem", "sqlite" or "rocksdb".
/// `path`:    should contain the UTF-8 encoded path name as a null-terminated C-string.
/// `db_id`:   will contain the ID of the database opened.
/// `options`: options for the DB constructor: engine dependent.
///
/// When the function is successful, null pointer is returned,
/// otherwise a pointer to a C-string containing the error message will be returned.
/// The returned C-string must be freed with `cozo_free_str`.
#[no_mangle]
pub unsafe extern "C" fn cozo_open_db(
    engine: *const c_char,
    path: *const c_char,
    options: *const c_char,
    db_id: &mut i32,
) -> *mut c_char {
    let engine = match CStr::from_ptr(engine).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };

    let path = match CStr::from_ptr(path).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };

    let options = match CStr::from_ptr(options).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };

    let db = match DbInstance::new_with_str(engine, path, options) {
        Ok(db) => db,
        Err(err) => return CString::new(err).unwrap().into_raw(),
    };

    let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
    let mut dbs = HANDLES.dbs.lock().unwrap();
    dbs.insert(id, db);
    *db_id = id;
    null_mut()
}

/// Close a database.
///
/// `db_id`: the ID representing the database to close.
///
/// Returns `true` if the database is closed,
/// `false` if it has already been closed, or does not exist.
#[no_mangle]
pub unsafe extern "C" fn cozo_close_db(db_id: i32) -> bool {
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&db_id)
    };
    db.is_some()
}

/// Run query against a database.
///
/// `db_id`:           the ID representing the database to run the query.
/// `script_raw`:      a UTF-8 encoded C-string for the CozoScript to execute.
/// `params_raw`:      a UTF-8 encoded C-string for the params of the query,
///                    in JSON format. You must always pass in a valid JSON map,
///                    even if you do not use params in your query
///                    (pass "{}" in this case).
/// `immutable_query`: whether the query is read-only.
///
/// Returns a UTF-8-encoded C-string that **must** be freed with `cozo_free_str`.
/// The string contains the JSON return value of the query.
#[no_mangle]
pub unsafe extern "C" fn cozo_run_query(
    db_id: i32,
    script_raw: *const c_char,
    params_raw: *const c_char,
    immutable_query: bool,
) -> *mut c_char {
    let script = match CStr::from_ptr(script_raw).to_str() {
        Ok(p) => p,
        Err(_) => {
            return CString::new(r##"{"ok":false,"message":"script is not UTF-8 encoded"}"##)
                .unwrap()
                .into_raw();
        }
    };
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };
    let params_str = match CStr::from_ptr(params_raw).to_str() {
        Ok(p) => p,
        Err(_) => {
            return CString::new(
                r##"{"ok":false,"message":"params argument is not UTF-8 encoded"}"##,
            )
            .unwrap()
            .into_raw();
        }
    };

    let result = db.run_script_str(script, params_str, immutable_query);
    CString::new(result).unwrap().into_raw()
}

#[no_mangle]
/// Import data into relations
///
/// Note that triggers are _not_ run for the relations, if any exists.
/// If you need to activate triggers, use queries with parameters.
///
/// `db_id`:        the ID representing the database.
/// `json_payload`: a UTF-8 encoded JSON payload, in the same form as returned by exporting relations.
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_import_relations(
    db_id: i32,
    json_payload: *const c_char,
) -> *mut c_char {
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };
    let data = match CStr::from_ptr(json_payload).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };
    CString::new(db.import_relations_str(data))
        .unwrap()
        .into_raw()
}

#[no_mangle]
/// Export relations into JSON
///
/// `db_id`:        the ID representing the database.
/// `json_payload`: a UTF-8 encoded JSON payload, see the manual for the expected fields.
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_export_relations(
    db_id: i32,
    json_payload: *const c_char,
) -> *mut c_char {
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };
    let data = match CStr::from_ptr(json_payload).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };
    CString::new(db.export_relations_str(data))
        .unwrap()
        .into_raw()
}

#[no_mangle]
/// Backup the database.
///
/// `db_id`:    the ID representing the database.
/// `out_path`: path of the output file.
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_backup(db_id: i32, out_path: *const c_char) -> *mut c_char {
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };
    let data = match CStr::from_ptr(out_path).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };
    CString::new(db.backup_db_str(data)).unwrap().into_raw()
}

#[no_mangle]
/// Restore the database from a backup.
///
/// `db_id`:   the ID representing the database.
/// `in_path`: path of the input file.
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_restore(db_id: i32, in_path: *const c_char) -> *mut c_char {
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };
    let data = match CStr::from_ptr(in_path).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };
    CString::new(db.restore_backup_str(data))
        .unwrap()
        .into_raw()
}

#[no_mangle]
/// Import data into relations from a backup
///
/// Note that triggers are _not_ run for the relations, if any exists.
/// If you need to activate triggers, use queries with parameters.
///
/// `db_id`:        the ID representing the database.
/// `json_payload`: a UTF-8 encoded JSON payload: `{"path": ..., "relations": [...]}`
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_import_from_backup(
    db_id: i32,
    json_payload: *const c_char,
) -> *mut c_char {
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&db_id).cloned()
        };
        match db_ref {
            None => {
                return CString::new(r##"{"ok":false,"message":"database closed"}"##)
                    .unwrap()
                    .into_raw();
            }
            Some(db) => db,
        }
    };

    let data = match CStr::from_ptr(json_payload).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{err}")).unwrap().into_raw(),
    };

    CString::new(db.import_from_backup_str(data))
        .unwrap()
        .into_raw()
}

/// Free any C-string returned from the Cozo C API.
/// Must be called exactly once for each returned C-string.
///
/// `s`: the C-string to free.
#[no_mangle]
pub unsafe extern "C" fn cozo_free_str(s: *mut c_char) {
    let _ = CString::from_raw(s);
}
