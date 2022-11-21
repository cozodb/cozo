/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use wasm_bindgen::prelude::*;

use cozo::*;

mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub struct CozoDb {
    db: DbInstance,
}

#[wasm_bindgen]
impl CozoDb {
    pub fn new() -> Self {
        utils::set_panic_hook();
        let db = DbInstance::new("mem", "", Default::default()).unwrap();
        Self { db }
    }
    pub fn run(&self, script: &str, params: &str) -> String {
        self.db.run_script_str(script, params)
    }
    pub fn export_relations(&self, rels: &str) -> String {
        self.db.export_relations_str(rels)
    }
    pub fn import_relation(&self, data: &str) -> String {
        self.db.import_relation_str(data)
    }
}
