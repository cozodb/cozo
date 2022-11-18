/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
#![warn(rust_2018_idioms, future_incompatible)]

use std::collections::BTreeMap;
use std::ffi::{c_char, CStr, CString};
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;

use cozo::*;

#[derive(Clone)]
enum DbInstance {
    Mem(Db<MemStorage>),
    #[cfg(feature = "storage-sqlite")]
    Sqlite(Db<SqliteStorage>),
    #[cfg(feature = "storage-rocksdb")]
    RocksDb(Db<RocksDbStorage>),
}

impl DbInstance {
    fn new(engine: &str, path: &str) -> Result<Self, String> {
        match engine {
            "mem" => Ok(Self::Mem(new_cozo_mem().map_err(|err| err.to_string())?)),
            "sqlite" => {
                #[cfg(feature = "storage-sqlite")]
                {
                    return Ok(Self::Sqlite(
                        new_cozo_sqlite(path.to_string()).map_err(|err| err.to_string())?,
                    ));
                }

                #[cfg(not(feature = "storage-sqlite"))]
                {
                    return Err("support for sqlite not compiled".to_string());
                }
            }
            "rocksdb" => {
                #[cfg(feature = "storage-rocksdb")]
                {
                    return Ok(Self::RocksDb(
                        new_cozo_rocksdb(path.to_string()).map_err(|err| err.to_string())?,
                    ));
                }

                #[cfg(not(feature = "storage-rocksdb"))]
                {
                    return Err("support for rocksdb not compiled".to_string());
                }
            }
            _ => Err(format!("unsupported engine: {}", engine)),
        }
    }
    fn run_script_str(&self, payload: &str, params: &str) -> String {
        match self {
            DbInstance::Mem(db) => db.run_script_str(payload, params),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.run_script_str(payload, params),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.run_script_str(payload, params),
        }
    }
    fn import_relations(&self, data: &str) -> String {
        match self {
            DbInstance::Mem(db) => db.import_relation_str(data),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.import_relation_str(data),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.import_relation_str(data),
        }
    }
    fn export_relations(&self, data: &str) -> String {
        match self {
            DbInstance::Mem(db) => db.export_relations_str(data),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.export_relations_str(data),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.export_relations_str(data),
        }
    }
    fn backup(&self, path: &str) -> String {
        match self {
            DbInstance::Mem(db) => db.backup_db_str(path),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.backup_db_str(path),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.backup_db_str(path),
        }
    }
    fn restore(&self, path: &str) -> String {
        match self {
            DbInstance::Mem(db) => db.restore_backup_str(path),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.restore_backup_str(path),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.restore_backup_str(path),
        }
    }
}

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
/// `engine`: Which storage engine to use, can be "mem", "sqlite" or "rocksdb".
/// `path`:   should contain the UTF-8 encoded path name as a null-terminated C-string.
/// `db_id`:  will contain the id of the database opened.
///
/// When the function is successful, null pointer is returned,
/// otherwise a pointer to a C-string containing the error message will be returned.
/// The returned C-string must be freed with `cozo_free_str`.
#[no_mangle]
pub unsafe extern "C" fn cozo_open_db(
    engine: *const c_char,
    path: *const c_char,
    db_id: &mut i32,
) -> *mut c_char {
    let path = match CStr::from_ptr(path).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };

    let engine = match CStr::from_ptr(engine).to_str() {
        Ok(p) => p,
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };

    let db = match DbInstance::new(engine, path) {
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
/// `id`: the ID representing the database to close.
///
/// Returns `true` if the database is closed,
/// `false` if it has already been closed, or does not exist.
#[no_mangle]
pub unsafe extern "C" fn cozo_close_db(id: i32) -> bool {
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&id)
    };
    db.is_some()
}

/// Run query against a database.
///
/// `db_id`: the ID representing the database to run the query.
/// `script_raw`: a UTF-8 encoded C-string for the CozoScript to execute.
/// `params_raw`: a UTF-8 encoded C-string for the params of the query,
///               in JSON format. You must always pass in a valid JSON map,
///               even if you do not use params in your query
///               (pass "{}" in this case).
/// `errored`:    will point to `false` if the query is successful,
///               `true` if an error occurred.
///
/// Returns a UTF-8-encoded C-string that **must** be freed with `cozo_free_str`.
/// The string contains the JSON return value of the query.
#[no_mangle]
pub unsafe extern "C" fn cozo_run_query(
    db_id: i32,
    script_raw: *const c_char,
    params_raw: *const c_char,
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

    let result = db.run_script_str(script, params_str);
    CString::new(result).unwrap().into_raw()
}

#[no_mangle]
/// Import data into a relation
/// `db_id`:        the ID representing the database.
/// `json_payload`: a UTF-8 encoded JSON payload, see the manual for the expected fields.
///
/// Returns a UTF-8-encoded C-string indicating the result that **must** be freed with `cozo_free_str`.
pub unsafe extern "C" fn cozo_import_relation(
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
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };
    CString::new(db.import_relations(data)).unwrap().into_raw()
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
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };
    CString::new(db.export_relations(data)).unwrap().into_raw()
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
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };
    CString::new(db.backup(data)).unwrap().into_raw()
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
        Err(err) => return CString::new(format!("{}", err)).unwrap().into_raw(),
    };
    CString::new(db.restore(data)).unwrap().into_raw()
}

/// Free any C-string returned from the Cozo C API.
/// Must be called exactly once for each returned C-string.
///
/// `s`: the C-string to free.
#[no_mangle]
pub unsafe extern "C" fn cozo_free_str(s: *mut c_char) {
    let _ = CString::from_raw(s);
}
