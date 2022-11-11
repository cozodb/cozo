/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MPL-2.0.
 */

//! This crate provides the core functionalities of [CozoDB](https://github.com/cozodb/cozo).
//! It may be used directly for embedding CozoDB in other applications.
//!
//! This doc describes the Rust API. For general information about how to use Cozo, see:
//!
//! * [Installation and first queries](https://github.com/cozodb/cozo#install)
//! * [Tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
//! * [Manual for CozoScript](https://cozodb.github.io/current/manual/)

#![warn(rust_2018_idioms, future_incompatible)]
#![warn(missing_docs)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub use miette::Error;

pub use runtime::db::Db;

pub(crate) mod algo;
pub(crate) mod data;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod runtime;
pub(crate) mod utils;
pub(crate) mod storage;
