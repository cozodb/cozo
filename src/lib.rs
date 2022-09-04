#![warn(rust_2018_idioms, future_incompatible)]

pub use miette::Error;

pub use cozorocks::DbBuilder;
pub use data::encode::EncodedVec;
pub use runtime::db::Db;

// #[cfg(not(target_env = "msvc"))]
// use tikv_jemallocator::Jemalloc;

// #[cfg(not(target_env = "msvc"))]
// #[global_allocator]
// static GLOBAL: Jemalloc = Jemalloc;

pub(crate) mod data;
pub(crate) mod parse;
pub(crate) mod query;
pub(crate) mod runtime;
pub(crate) mod transact;
pub(crate) mod utils;
pub(crate) mod algo;
