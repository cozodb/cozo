/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI32;
use std::sync::Mutex;

use lazy_static::lazy_static;
use robusta_jni::bridge;
use robusta_jni::jni::errors::Error as JniError;
use robusta_jni::jni::errors::Result as JniResult;

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

fn get_db(id: i32) -> JniResult<DbInstance> {
    let db_ref = {
        let dbs = HANDLES.dbs.lock().unwrap();
        dbs.get(&id).cloned()
    };
    db_ref.ok_or_else(|| JniError::from("database already closed"))
}

#[bridge]
mod jni {
    use std::sync::atomic::Ordering;

    use robusta_jni::convert::{IntoJavaValue, Signature, TryFromJavaValue, TryIntoJavaValue};
    use robusta_jni::jni::errors::Error as JniError;
    use robusta_jni::jni::errors::Result as JniResult;
    use robusta_jni::jni::objects::AutoLocal;

    use cozo::*;

    use crate::{get_db, HANDLES};

    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue)]
    #[package(org.cozodb)]
    pub struct CozoDb<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> CozoDb<'env, 'borrow> {
        pub extern "jni" fn openDb(kind: String, path: String) -> JniResult<i32> {
            match DbInstance::new(&kind, &path, Default::default()) {
                Ok(db) => {
                    let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
                    let mut dbs = HANDLES.dbs.lock().unwrap();
                    dbs.insert(id, db);
                    Ok(id)
                }
                Err(err) => Err(JniError::from(format!("{:?}", err))),
            }
        }
        pub extern "jni" fn closeDb(id: i32) -> JniResult<bool> {
            let db = {
                let mut dbs = HANDLES.dbs.lock().unwrap();
                dbs.remove(&id)
            };
            Ok(db.is_some())
        }
        pub extern "jni" fn runQuery(
            id: i32,
            script: String,
            params_str: String,
        ) -> JniResult<String> {
            let db = get_db(id)?;
            Ok(db.run_script_str(&script, &params_str))
        }
        pub extern "jni" fn exportRelations(id: i32, relations_str: String) -> JniResult<String> {
            let db = get_db(id)?;
            Ok(db.export_relations_str(&relations_str))
        }
        pub extern "jni" fn importRelation(id: i32, data: String) -> JniResult<String> {
            let db = get_db(id)?;
            Ok(db.import_relation_str(&data))
        }
        pub extern "jni" fn backup(id: i32, out_file: String) -> JniResult<String> {
            let db = get_db(id)?;
            Ok(db.backup_db_str(&out_file))
        }
        pub extern "jni" fn restore(id: i32, in_file: String) -> JniResult<String> {
            let db = get_db(id)?;
            Ok(db.restore_backup_str(&in_file))
        }
    }
}
