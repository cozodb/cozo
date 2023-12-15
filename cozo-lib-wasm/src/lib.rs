/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use wasm_bindgen::prelude::*;

use cozo::*;
use js_sys::{Uint8Array, Array};

mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
use std::collections::BTreeMap;
use wasm_bindgen_futures::JsFuture;


#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(s: &str);
}

// Next let's define a macro that's like `println!`, only it works for
// `console.log`. Note that `println!` doesn't actually work on the wasm target
// because the standard library currently just eats all output. To get
// `println!`-like behavior in your app you'll likely want a macro like this.
macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
pub struct CozoDb {
    db: DbInstance,
}


#[wasm_bindgen(raw_module = "./indexeddb.js")]
extern "C" {
    fn loadAllFromIndexedDb(db_name: &str, db_value: &str, on_write_callback: &JsValue) -> js_sys::Promise;
    fn flushPendingWrites() -> js_sys::Promise;
}

fn array_to_vec_of_vecs(arr: Array) -> Vec<Vec<u8>> {
    let mut result = Vec::new();

    for i in 0..arr.length() {
        if let Ok(uint8_array) = arr.get(i).dyn_into::<Uint8Array>() {
            result.push(uint8_array.to_vec());
        } else {
            panic!("Failed to convert Uint8Array to Vec<u8>")
        }
    }

    result
}

#[wasm_bindgen]
impl CozoDb {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        utils::set_panic_hook();
        let db = DbInstance::new("mem", "", "").unwrap();
        Self { db }
    }

    /// Create CozoDb from IndexedDB
    pub async fn new_from_indexed_db(db_name: &str, store_name: &str, on_write_callback: &JsValue)-> Result<CozoDb, JsValue> {
        utils::set_panic_hook();
        log("CozoDB: loadig from IndexedDb...");

        let result = JsFuture::from(loadAllFromIndexedDb(db_name, store_name, on_write_callback)).await?;

        match result.dyn_into::<js_sys::Array>() {
            Ok(array) => {
                let keys = array_to_vec_of_vecs(array.get(0).dyn_into()?);
                let values = array_to_vec_of_vecs(array.get(1).dyn_into()?);

                let keys_len = keys.len();

                let mut db_snap: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();


                for (key, value) in keys.into_iter().zip(values.into_iter()) {
                    db_snap.insert(key, value);
                }
                console_log!("CozoDb: Loaded {:?} rows from IndexedDB", keys_len);

                let ret = crate::Db::new(MemStorage::new(db_snap)).map_err(|_| {
                    JsValue::from_str("Error creating DbInstance")
                })?;

                ret.initialize().map_err(|_| {
                    JsValue::from_str("Error initializ DbInstance")
                })?;


                let db = DbInstance::Mem(ret);


                Ok(CozoDb { db })
            },
            Err(_) => {
                Err(JsValue::from_str("Unexpected result from loadIndexedDb"))
            }
        }
    }

    pub async fn run(&self, script: &str, params: &str, immutable: bool) -> Result<String, JsValue> {
        let result = self.db.run_script_str(script, params, immutable);
        JsFuture::from(flushPendingWrites()).await?;
        Ok(result)
    }

    pub fn export_relations(&self, data: &str) -> String {
        self.db.export_relations_str(data)
    }
    pub fn import_relations(&self, data: &str) -> String {
        self.db.import_relations_str(data)
    }
}
