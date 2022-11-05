/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI32;
use std::sync::Mutex;

use lazy_static::lazy_static;
use robusta_jni::bridge;

use cozo::Db;

#[derive(Default)]
struct Handles {
    current: AtomicI32,
    dbs: Mutex<BTreeMap<i32, Db>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles::default();
}

#[bridge]
mod jni {
    use std::sync::atomic::Ordering;

    use robusta_jni::convert::{IntoJavaValue, Signature, TryFromJavaValue, TryIntoJavaValue};
    use robusta_jni::jni::errors::Error as JniError;
    use robusta_jni::jni::errors::Result as JniResult;
    use robusta_jni::jni::objects::AutoLocal;

    use cozo::Db;

    use crate::HANDLES;

    #[derive(Signature, TryIntoJavaValue, IntoJavaValue, TryFromJavaValue)]
    #[package(org.cozodb)]
    pub struct CozoDb<'env: 'borrow, 'borrow> {
        #[instance]
        raw: AutoLocal<'env, 'borrow>,
    }

    impl<'env: 'borrow, 'borrow> CozoDb<'env, 'borrow> {
        pub extern "jni" fn openDb(path: String) -> JniResult<i32> {
            match Db::new(path) {
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
            let db = {
                let db_ref = {
                    let dbs = HANDLES.dbs.lock().unwrap();
                    dbs.get(&id).cloned()
                };
                let db = db_ref.ok_or_else(|| JniError::from("database already closed"))?;
                db
            };
            Ok(db.run_script_str(&script, &params_str))
        }
    }
}
