/*
 * Copyright 2022, The Cozo Project Authors. Licensed under AGPL-3 or later.
 */

#![warn(rust_2018_idioms, future_incompatible)]
#![warn(missing_docs)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub use miette::Error;

pub use cozorocks::DbBuilder;
pub use runtime::db::Db;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

pub(crate) mod algo;
pub(crate) mod data;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod runtime;
pub(crate) mod utils;
