/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![warn(rust_2018_idioms, future_incompatible)]
#![allow(clippy::type_complexity)]

pub use bridge::db::DbBuilder;
pub use bridge::db::RocksDb;
pub use bridge::ffi::RocksDbStatus;
pub use bridge::ffi::SnapshotBridge;
pub use bridge::ffi::StatusCode;
pub use bridge::ffi::StatusSeverity;
pub use bridge::ffi::StatusSubCode;
pub use bridge::iter::DbIter;
pub use bridge::iter::IterBuilder;
pub use bridge::tx::PinSlice;
pub use bridge::tx::Tx;
pub use bridge::tx::TxBuilder;

pub(crate) mod bridge;
