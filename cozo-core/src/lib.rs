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
//! let db = new_cozo_mem().unwrap();
//! let script = "?[a] := a in [1, 2, 3]";
//! let result = db.run_script(script, &Default::default()).unwrap();
//! println!("{:?}", result);
//! ```
//! We created an in-memory database with [`new_cozo_mem`](crate::new_cozo_mem) above.
//! Persistent options include [`new_cozo_rocksdb`](crate::new_cozo_rocksdb),
//! [`new_cozo_sqlite`](crate::new_cozo_sqlite) and others.
#![doc = document_features::document_features!()]
#![warn(rust_2018_idioms, future_incompatible)]
#![warn(missing_docs)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub use miette::Error;

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

// pub use storage::re::{new_cozo_redb, ReStorage};

pub(crate) mod algo;
pub(crate) mod data;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod runtime;
pub(crate) mod storage;
pub(crate) mod utils;
