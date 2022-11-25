/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jint, jstring};
use jni::JNIEnv;
use lazy_static::lazy_static;

use cozo::*;

#[derive(Default)]
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

fn get_db(id: i32) -> Option<DbInstance> {
    let dbs = HANDLES.dbs.lock().unwrap();
    dbs.get(&id).cloned()
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_openDb(
    env: JNIEnv,
    _class: JClass,
    engine: JString,
    path: JString,
    options: JString,
) -> jint {
    let engine: String = env.get_string(engine).unwrap().into();
    let path: String = env.get_string(path).unwrap().into();
    let options: String = env.get_string(options).unwrap().into();
    let id = match DbInstance::new(&engine, &path, &options) {
        Ok(db) => {
            let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
            let mut dbs = HANDLES.dbs.lock().unwrap();
            dbs.insert(id, db);
            id
        }
        Err(err) => {
            eprintln!("{:?}", err);
            -1
        }
    };
    id
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_closeDb(
    _env: JNIEnv,
    _class: JClass,
    id: jint,
) -> jboolean {
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&id)
    };
    db.is_some().into()
}

const DB_NOT_FOUND: &str = r#"{"ok":false,"message":"database not found"}"#;

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_runQuery(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    script: JString,
    params_str: JString,
) -> jstring {
    let script: String = env.get_string(script).unwrap().into();
    let params_str: String = env.get_string(params_str).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.run_script_str(&script, &params_str);
            env.new_string(res).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_exportRelations(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    rel: JString,
) -> jstring {
    let rel: String = env.get_string(rel).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.export_relations_str(&rel);
            env.new_string(res).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_importRelations(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    data: JString,
) -> jstring {
    let data: String = env.get_string(data).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.import_relations_str(&data);
            env.new_string(res).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_backup(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    file: JString,
) -> jstring {
    let file: String = env.get_string(file).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.backup_db_str(&file);
            env.new_string(res).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_restore(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    file: JString,
) -> jstring {
    let file: String = env.get_string(file).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.restore_backup_str(&file);
            env.new_string(res).unwrap().into_raw()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_cozodb_CozoJavaBridge_importFromBackup(
    env: JNIEnv,
    _class: JClass,
    id: jint,
    data: JString,
) -> jstring {
    let data: String = env.get_string(data).unwrap().into();
    match get_db(id) {
        None => env.new_string(DB_NOT_FOUND).unwrap().into_raw(),
        Some(db) => {
            let res = db.import_from_backup_str(&data);
            env.new_string(res).unwrap().into_raw()
        }
    }
}