/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! This crate provides the core functionalities of [CozoDB](https://cozodb.org).
//! It may be used to embed CozoDB in your application.
//!
//! This doc describes the Rust API. To learn how to use CozoDB to query (CozoScript), see:
//!
//! * [The CozoDB documentation](https://docs.cozodb.org)
//!
//! Rust API usage:
//! ```
//! use cozo::*;
//!
//! let db = DbInstance::new("mem", "", Default::default()).unwrap();
//! let script = "?[a] := a in [1, 2, 3]";
//! let result = db.run_script(script, Default::default()).unwrap();
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

use std::collections::BTreeMap;
#[allow(unused_imports)]
use std::time::Instant;

use lazy_static::lazy_static;
pub use miette::Error;
use miette::Report;
#[allow(unused_imports)]
use miette::{
    bail, miette, GraphicalReportHandler, GraphicalTheme, IntoDiagnostic, JSONReportHandler,
    Result, ThemeCharacters, ThemeStyles,
};
use serde_json::json;

pub use fixed_rule::FixedRule;
pub use runtime::db::Db;
pub use runtime::db::NamedRows;
pub use runtime::relation::decode_tuple_from_kv;
pub use runtime::temp_store::RegularTempStore;
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

pub(crate) mod data;
pub(crate) mod fixed_rule;
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
    /// The valid engines are:
    ///
    /// * `mem`
    /// * `sqlite`
    /// * `rocksdb`
    /// * `sled`
    /// * `tikv`
    ///
    /// assuming all features are enabled during compilation. Otherwise only
    /// some of the engines are available. The `mem` engine is always available.
    ///
    /// `path` is ignored for `mem` and `tikv` engines.
    /// `options` is ignored for every engine except `tikv`.
    #[allow(unused_variables)]
    pub fn new(engine: &str, path: &str, options: &str) -> Result<Self> {
        let options = if options.is_empty() { "{}" } else { options };
        Ok(match engine {
            "mem" => Self::Mem(new_cozo_mem()?),
            #[cfg(feature = "storage-sqlite")]
            "sqlite" => Self::Sqlite(new_cozo_sqlite(path.to_string())?),
            #[cfg(feature = "storage-rocksdb")]
            "rocksdb" => Self::RocksDb(new_cozo_rocksdb(path)?),
            #[cfg(feature = "storage-sled")]
            "sled" => Self::Sled(new_cozo_sled(path)?),
            #[cfg(feature = "storage-tikv")]
            "tikv" => {
                #[derive(serde_derive::Deserialize)]
                struct TiKvOpts {
                    end_points: Vec<String>,
                    #[serde(default = "Default::default")]
                    optimistic: bool,
                }
                let opts: TiKvOpts = serde_json::from_str(options).into_diagnostic()?;
                Self::TiKv(new_cozo_tikv(opts.end_points.clone(), opts.optimistic)?)
            }
            k => bail!(
                "database engine '{}' not supported (maybe not compiled in)",
                k
            ),
        })
    }
    /// Same as [Self::new], but inputs and error messages are all in strings
    pub fn new_with_str(
        engine: &str,
        path: &str,
        options: &str,
    ) -> std::result::Result<Self, String> {
        Self::new(engine, path, options).map_err(|err| err.to_string())
    }
    /// Dispatcher method. See [crate::Db::run_script].
    pub fn run_script(
        &self,
        payload: &str,
        params: BTreeMap<String, JsonValue>,
    ) -> Result<NamedRows> {
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
    pub fn run_script_fold_err(
        &self,
        payload: &str,
        params: BTreeMap<String, JsonValue>,
    ) -> JsonValue {
        #[cfg(not(target_arch = "wasm32"))]
        let start = Instant::now();

        match self.run_script(payload, params) {
            Ok(named_rows) => {
                let mut j_val = named_rows.into_json();
                #[cfg(not(target_arch = "wasm32"))]
                let took = start.elapsed().as_secs_f64();
                let map = j_val.as_object_mut().unwrap();
                map.insert("ok".to_string(), json!(true));
                #[cfg(not(target_arch = "wasm32"))]
                map.insert("took".to_string(), json!(took));

                j_val
            }
            Err(err) => format_error_as_json(err, Some(payload)),
        }
    }
    /// Run the CozoScript passed in. The `params` argument is a map of parameters formatted as JSON.
    pub fn run_script_str(&self, payload: &str, params: &str) -> String {
        let params_json = if params.is_empty() {
            BTreeMap::default()
        } else {
            match serde_json::from_str::<BTreeMap<String, JsonValue>>(params) {
                Ok(map) => map,
                Err(_) => {
                    return json!({"ok": false, "message": "params argument is not a JSON map"})
                        .to_string()
                }
            }
        };
        self.run_script_fold_err(payload, params_json).to_string()
    }
    /// Dispatcher method. See [crate::Db::export_relations].
    pub fn export_relations<'a>(
        &self,
        relations: impl Iterator<Item = &'a str>,
    ) -> Result<BTreeMap<String, NamedRows>> {
        match self {
            DbInstance::Mem(db) => db.export_relations(relations),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.export_relations(relations),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.export_relations(relations),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.export_relations(relations),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.export_relations(relations),
        }
    }
    /// Export relations to JSON-encoded string
    pub fn export_relations_str(&self, data: &str) -> String {
        match self.export_relations_str_inner(data) {
            Ok(s) => {
                let ret = json!({"ok": true, "data": s});
                format!("{ret}")
            }
            Err(err) => {
                let ret = json!({"ok": false, "message": err.to_string()});
                format!("{ret}")
            }
        }
    }
    fn export_relations_str_inner(&self, data: &str) -> Result<JsonValue> {
        #[derive(serde_derive::Deserialize)]
        struct Payload {
            relations: Vec<String>,
        }
        let j_val: Payload = serde_json::from_str(data).into_diagnostic()?;
        let results = self.export_relations(j_val.relations.iter().map(|s| s as &str))?;
        Ok(results
            .into_iter()
            .map(|(k, v)| (k, v.into_json()))
            .collect())
    }
    /// Dispatcher method. See [crate::Db::import_relations].
    pub fn import_relations(&self, data: BTreeMap<String, NamedRows>) -> Result<()> {
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
    ///
    /// Note that triggers are _not_ run for the relations, if any exists.
    /// If you need to activate triggers, use queries with parameters.
    pub fn import_relations_str(&self, data: &str) -> String {
        match self.import_relations_str_with_err(data) {
            Ok(()) => {
                format!("{}", json!({"ok": true}))
            }
            Err(err) => {
                format!("{}", json!({"ok": false, "message": err.to_string()}))
            }
        }
    }
    /// Import a relation, the data is given as a JSON string
    ///
    /// Note that triggers are _not_ run for the relations, if any exists.
    /// If you need to activate triggers, use queries with parameters.
    pub fn import_relations_str_with_err(&self, data: &str) -> Result<()> {
        let j_obj: BTreeMap<String, NamedRows> = serde_json::from_str(data).into_diagnostic()?;
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
    pub fn restore_backup(&self, in_file: &str) -> Result<()> {
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
        match self.restore_backup(in_file) {
            Ok(_) => json!({"ok": true}).to_string(),
            Err(err) => json!({"ok": false, "message": err.to_string()}).to_string(),
        }
    }
    /// Dispatcher method. See [crate::Db::import_from_backup].
    pub fn import_from_backup(&self, in_file: &str, relations: &[String]) -> Result<()> {
        match self {
            DbInstance::Mem(db) => db.import_from_backup(in_file, relations),
            #[cfg(feature = "storage-sqlite")]
            DbInstance::Sqlite(db) => db.import_from_backup(in_file, relations),
            #[cfg(feature = "storage-rocksdb")]
            DbInstance::RocksDb(db) => db.import_from_backup(in_file, relations),
            #[cfg(feature = "storage-sled")]
            DbInstance::Sled(db) => db.import_from_backup(in_file, relations),
            #[cfg(feature = "storage-tikv")]
            DbInstance::TiKv(db) => db.import_from_backup(in_file, relations),
        }
    }
    /// Import relations from an Sqlite backup, with JSON string return value
    ///
    /// Note that triggers are _not_ run for the relations, if any exists.
    /// If you need to activate triggers, use queries with parameters.
    pub fn import_from_backup_str(&self, payload: &str) -> String {
        match self.import_from_backup_str_inner(payload) {
            Ok(_) => json!({"ok": true}).to_string(),
            Err(err) => json!({"ok": false, "message": err.to_string()}).to_string(),
        }
    }
    fn import_from_backup_str_inner(&self, payload: &str) -> Result<()> {
        #[derive(serde_derive::Deserialize)]
        struct Payload {
            path: String,
            relations: Vec<String>,
        }
        let json_payload: Payload = serde_json::from_str(payload).into_diagnostic()?;

        self.import_from_backup(&json_payload.path, &json_payload.relations)
    }
}

/// Convert error raised by the database into friendly JSON format
pub fn format_error_as_json(mut err: Report, source: Option<&str>) -> JsonValue {
    if err.source_code().is_none() {
        if let Some(src) = source {
            err = err.with_source_code(src.to_string());
        }
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

lazy_static! {
    static ref TEXT_ERR_HANDLER: GraphicalReportHandler = miette::GraphicalReportHandler::new()
        .with_theme(GraphicalTheme {
            characters: ThemeCharacters::unicode(),
            styles: ThemeStyles::ansi()
        });
    static ref JSON_ERR_HANDLER: JSONReportHandler = miette::JSONReportHandler::new();
}
