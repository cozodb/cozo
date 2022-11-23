/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! This crate provides the core functionalities of [CozoDB](https://github.com/cozodb/cozo).
//! It may be used directly for embedding CozoDB in other applications.
//!
//! This doc describes the Rust API. For general information about how to use Cozo, see:
//!
//! * [Installation and first queries](https://github.com/cozodb/cozo#install)
//! * [Tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
//! * [Manual for CozoScript](https://cozodb.github.io/current/manual/)
//!
//! Example usage:
//! ```
//! use cozo::*;
//!
//! let db = DbInstance::new("mem", "", Default::default()).unwrap();
//! let script = "?[a] := a in [1, 2, 3]";
//! let result = db.run_script(script, &Default::default()).unwrap();
//! println!("{:?}", result);
//! ```
//! We created an in-memory database above. There are other persistent options:
//! see [DbInstance::new]. It is perfectly fine to run multiple storage engines in the same process.
//!
#![doc = document_features::document_features!()]
#![warn(rust_2018_idioms, future_incompatible)]
#![warn(missing_docs)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

use itertools::Itertools;
use lazy_static::lazy_static;
pub use miette::Error;
use miette::{
    bail, miette, GraphicalReportHandler, GraphicalTheme, IntoDiagnostic, JSONReportHandler,
    Result, ThemeCharacters, ThemeStyles,
};
use serde_json::{json, Map};

pub use runtime::db::Db;
pub use runtime::relation::decode_tuple_from_kv;
pub use storage::mem::{new_cozo_mem, MemStorage};
#[cfg(feature = "storage-rocksdb")]
pub use storage::rocks::{new_cozo_rocksdb, RocksDbStorage};
#[cfg(feature = "storage-sled")]
pub use storage::sled::{new_cozo_sled, SledStorage};
#[cfg(feature = "storage-sqlite")]
pub use storage::sqlite::{new_cozo_sqlite, SqliteStorage};
#[cfg(feature = "storage-tikv")]
pub use storage::tikv::{new_cozo_tikv, TiKvStorage};
pub use storage::{Storage, StoreTx};

use crate::data::json::JsonValue;

// pub use storage::re::{new_cozo_redb, ReStorage};

pub(crate) mod algo;
pub(crate) mod data;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod runtime;
pub(crate) mod storage;
pub(crate) mod utils;

#[derive(Clone)]
/// A dispatcher for concrete storage implementations, wrapping [Db]. This is done so that
/// client code does not have to deal with generic code constantly. You may prefer to use
/// [Db] directly, especially if you provide a custom storage engine.
///
/// Many methods are dispatching methods for the corresponding methods on [Db].
///
/// Other methods are wrappers simplifying signatures to deal with only strings.
/// These methods made code for interop with other languages much easier,
/// but are not desirable if you are using Rust.
pub enum DbInstance {
    /// In memory storage (not persistent)
    Mem(Db<MemStorage>),
    #[cfg(feature = "storage-sqlite")]
    /// Sqlite storage
    Sqlite(Db<SqliteStorage>),
    #[cfg(feature = "storage-rocksdb")]
    /// RocksDB storage
    RocksDb(Db<RocksDbStorage>),
    #[cfg(feature = "storage-sled")]
    /// Sled storage (experimental)
    Sled(Db<SledStorage>),
    #[cfg(feature = "storage-tikv")]
    /// TiKV storage (experimental)
    TiKv(Db<TiKvStorage>),
}

impl DbInstance {
    /// Create a DbInstance, which is a dispatcher for various concrete implementations.
    /// The valid kinds are:
    ///
    /// * `mem`
    /// * `sqlite`
    /// * `rocksdb`
    /// * `sled`
    /// * `tikv`
    ///
    /// assuming all features are enabled during compilation. Otherwise only
    /// some of the kinds are available. The `mem` kind is always available.
    ///
    /// `path` is ignored for `mem` and `tikv` kinds.
    /// `options` is ignored for every kind except `tikv`.
    #[allow(unused_variables)]
    pub fn new(kind: &str, path: &str, options: JsonValue) -> Result<Self> {
        Ok(match kind {
            "mem" => Self::Mem(new_cozo_mem()?),
            #[cfg(feature = "storage-sqlite")]
            "sqlite" => Self::Sqlite(new_cozo_sqlite(path.to_string())?),
            #[cfg(feature = "storage-rocksdb")]
            "rocksdb" => Self::RocksDb(new_cozo_rocksdb(path)?),
            #[cfg(feature = "storage-sled")]
            "sled" => Self::Sled(new_cozo_sled(path)?),
            #[cfg(feature = "storage-tikv")]
            "tikv" => {
                let end_points = options
                    .get("pd_endpoints")
                    .ok_or_else(|| miette!("required option 'pd_endpoints' not found"))?;
                let end_points = end_points
                    .as_array()
                    .ok_or_else(|| miette!("option 'pd_endpoints' must be an array"))?;
                let end_points: Vec<_> = end_points
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| "option 'pd_endpoints' must contain strings")
                    })
                    .try_collect()?;
                let optimistic = options.get("optimistic").unwrap_or(&JsonValue::Bool(true));
                let optimistic = optimistic
                    .as_bool()
                    .ok_or_else(|| miette!("option 'optimistic' must be a bool"))?;
                Self::TiKv(new_cozo_tikv(end_points, optimistic)?)
            }
            kind => bail!(
                "database kind '{}' not supported (maybe not compiled in)",
                kind
            ),
        })
    }
    /// Same as [Self::new], but inputs and error messages are all in strings
    pub fn new_with_str(
        kind: &str,
        path: &str,
        options: &str,
    ) -> std::result::Result<Self, String> {
        let options: JsonValue = serde_json::from_str(options).map_err(|e| e.to_string())?;
        Self::new(kind, path, options).map_err(|err| err.to_string())
    }
    /// Dispatcher method. See [crate::Db::run_script].
    pub fn run_script(&self, payload: &str, params: &Map<String, JsonValue>) -> Result<JsonValue> {
        match self {
            DbInstance::Mem(db) => db.run_script(payload, params),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.run_script(payload, params),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.run_script(payload, params),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.run_script(payload, params),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.run_script(payload, params),
        }
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters.
    /// Fold any error into the return JSON itself.
    pub fn run_script_fold_err(&self, payload: &str, params: &Map<String, JsonValue>) -> JsonValue {
        match self.run_script(payload, params) {
            Ok(json) => json,
            Err(mut err) => {
                if err.source_code().is_none() {
                    err = err.with_source_code(payload.to_string());
                }
                let mut text_err = String::new();
                let mut json_err = String::new();
                TEXT_ERR_HANDLER
                    .render_report(&mut text_err, err.as_ref())
                    .expect("render text error failed");
                JSON_ERR_HANDLER
                    .render_report(&mut json_err, err.as_ref())
                    .expect("render json error failed");
                let mut json: serde_json::Value =
                    serde_json::from_str(&json_err).expect("parse rendered json error failed");
                let map = json.as_object_mut().unwrap();
                map.insert("ok".to_string(), json!(false));
                map.insert("display".to_string(), json!(text_err));
                json
            }
        }
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters formatted as JSON.
    pub fn run_script_str(&self, payload: &str, params: &str) -> String {
        let params_json = if params.is_empty() {
            Map::default()
        } else {
            match serde_json::from_str::<serde_json::Value>(params) {
                Ok(serde_json::Value::Object(map)) => map,
                Ok(_) => {
                    return json!({"ok": false, "message": "params argument is not valid JSON"})
                        .to_string()
                }
                Err(_) => {
                    return json!({"ok": false, "message": "params argument is not a JSON map"})
                        .to_string()
                }
            }
        };
        self.run_script_fold_err(payload, &params_json).to_string()
    }
    /// Dispatcher method. See [crate::Db::export_relations].
    pub fn export_relations<'a>(
        &self,
        relations: impl Iterator<Item = &'a str>,
        as_objects: bool,
    ) -> Result<JsonValue> {
        match self {
            DbInstance::Mem(db) => db.export_relations(relations, as_objects),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.export_relations(relations, as_objects),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.export_relations(relations, as_objects),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.export_relations(relations, as_objects),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.export_relations(relations, as_objects),
        }
    }
    /// Export relations to JSON-encoded string
    pub fn export_relations_str(&self, data: &str) -> String {
        match self.export_relations_str_inner(data) {
            Ok(s) => {
                let ret = json!({"ok": true, "data": s});
                format!("{}", ret)
            }
            Err(err) => {
                let ret = json!({"ok": false, "message": err.to_string()});
                format!("{}", ret)
            }
        }
    }
    fn export_relations_str_inner(&self, data: &str) -> Result<JsonValue> {
        let j_val: JsonValue = serde_json::from_str(data).into_diagnostic()?;
        let relations = j_val
            .get("relations")
            .ok_or_else(|| miette!("field 'relations' expected"))?;
        let v = relations
            .as_array()
            .ok_or_else(|| miette!("expects field 'relations' to be an array"))?;
        let relations: Vec<_> = v
            .iter()
            .map(|name| {
                name.as_str().ok_or_else(|| {
                    miette!("expects field 'relations' to be an array of string names")
                })
            })
            .try_collect()?;
        let as_objects = j_val.get("as_objects").unwrap_or(&JsonValue::Bool(false));
        let as_objects = as_objects
            .as_bool()
            .ok_or_else(|| miette!("expects field 'as_objects' to be a boolean"))?;
        let results = self.export_relations(relations.into_iter(), as_objects)?;
        Ok(results)
    }
    /// Dispatcher method. See [crate::Db::import_relations].
    pub fn import_relations(&self, data: &Map<String, JsonValue>) -> Result<()> {
        match self {
            DbInstance::Mem(db) => db.import_relations(data),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.import_relations(data),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.import_relations(data),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.import_relations(data),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.import_relations(data),
        }
    }
    /// Import a relation, the data is given as a JSON string, and the returned result is converted into a string
    pub fn import_relations_str(&self, data: &str) -> String {
        match self.import_relations_str_inner(data) {
            Ok(()) => {
                format!("{}", json!({"ok": true}))
            }
            Err(err) => {
                format!("{}", json!({"ok": false, "message": err.to_string()}))
            }
        }
    }
    fn import_relations_str_inner(&self, data: &str) -> Result<()> {
        let j_obj: JsonValue = serde_json::from_str(data).into_diagnostic()?;
        let j_obj = j_obj
            .as_object()
            .ok_or_else(|| miette!("expect an object"))?;
        self.import_relations(j_obj)
    }
    /// Dispatcher method. See [crate::Db::backup_db].
    pub fn backup_db(&self, out_file: String) -> Result<()> {
        match self {
            DbInstance::Mem(db) => db.backup_db(out_file),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.backup_db(out_file),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.backup_db(out_file),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.backup_db(out_file),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.backup_db(out_file),
        }
    }
    /// Backup the running database into an Sqlite file, with JSON string return value
    pub fn backup_db_str(&self, out_file: &str) -> String {
        match self.backup_db(out_file.to_string()) {
            Ok(_) => json!({"ok": true}).to_string(),
            Err(err) => json!({"ok": false, "message": err.to_string()}).to_string(),
        }
    }
    /// Restore from an Sqlite backup
    pub fn restore_backup(&self, in_file: String) -> Result<()> {
        match self {
            DbInstance::Mem(db) => db.restore_backup(in_file),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.restore_backup(in_file),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.restore_backup(in_file),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.restore_backup(in_file),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.restore_backup(in_file),
        }
    }
    /// Restore from an Sqlite backup, with JSON string return value
    pub fn restore_backup_str(&self, in_file: &str) -> String {
        match self.restore_backup(in_file.to_string()) {
            Ok(_) => json!({"ok": true}).to_string(),
            Err(err) => json!({"ok": false, "message": err.to_string()}).to_string(),
        }
    }
}

lazy_static! {
    static ref TEXT_ERR_HANDLER: GraphicalReportHandler = miette::GraphicalReportHandler::new()
        .with_theme(GraphicalTheme {
            characters: ThemeCharacters::unicode(),
            styles: ThemeStyles::ansi()
        });
    static ref JSON_ERR_HANDLER: JSONReportHandler = miette::JSONReportHandler::new();
}
