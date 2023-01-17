/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use crossbeam::channel::Sender;
use smartstring::{LazyCompact, SmartString};

use crate::{Db, NamedRows, Storage};

/// Represents the kind of operation that triggered the callback
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CallbackOp {
    /// Triggered by Put operations
    Put,
    /// Triggered by Rm operations
    Rm,
}

impl Display for CallbackOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CallbackOp::Put => f.write_str("Put"),
            CallbackOp::Rm => f.write_str("Rm"),
        }
    }
}

impl CallbackOp {
    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            CallbackOp::Put => "Put",
            CallbackOp::Rm => "Rm",
        }
    }
}

#[allow(dead_code)]
pub struct CallbackDeclaration {
    pub(crate) dependent: SmartString<LazyCompact>,
    pub(crate) sender: Sender<(CallbackOp, NamedRows, NamedRows)>,
}

pub(crate) type CallbackCollector =
    BTreeMap<SmartString<LazyCompact>, Vec<(CallbackOp, NamedRows, NamedRows)>>;

#[allow(dead_code)]
pub(crate) type EventCallbackRegistry = (
    BTreeMap<u32, CallbackDeclaration>,
    BTreeMap<SmartString<LazyCompact>, BTreeSet<u32>>,
);

impl<'s, S: Storage<'s>> Db<S> {
    pub(crate) fn current_callback_targets(&self) -> BTreeSet<SmartString<LazyCompact>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.event_callbacks
                .read()
                .unwrap()
                .1
                .keys()
                .cloned()
                .collect()
        }

        #[cfg(target_arch = "wasm32")]
        {
            Default::default()
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn send_callbacks(&'s self, collector: CallbackCollector) {
        let mut to_remove = vec![];

        for (table, vals) in collector {
            for (op, new, old) in vals {
                let (cbs, cb_dir) = &*self.event_callbacks.read().unwrap();
                if let Some(cb_ids) = cb_dir.get(&table) {
                    let mut it = cb_ids.iter();
                    if let Some(fst) = it.next() {
                        for cb_id in it {
                            if let Some(cb) = cbs.get(cb_id) {
                                if cb.sender.send((op, new.clone(), old.clone())).is_err() {
                                    to_remove.push(*cb_id)
                                }
                            }
                        }

                        if let Some(cb) = cbs.get(fst) {
                            if cb.sender.send((op, new, old)).is_err() {
                                to_remove.push(*fst)
                            }
                        }
                    }
                }
            }
        }

        if !to_remove.is_empty() {
            let (cbs, cb_dir) = &mut *self.event_callbacks.write().unwrap();
            for removing_id in &to_remove {
                if let Some(removed) = cbs.remove(removing_id) {
                    if let Some(set) = cb_dir.get_mut(&removed.dependent) {
                        set.remove(removing_id);
                    }
                }
            }
        }
    }
}
