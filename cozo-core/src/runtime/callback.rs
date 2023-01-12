/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet};

use smartstring::{LazyCompact, SmartString};

use crate::{Db, NamedRows, Storage};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CallbackOp {
    Put,
    Rm,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct CallbackDeclaration {
    pub(crate) dependent: SmartString<LazyCompact>,
    pub(crate) callback: Box<dyn Fn(CallbackOp, NamedRows, NamedRows) + Send + Sync>,
}

pub(crate) type CallbackCollector =
    BTreeMap<SmartString<LazyCompact>, Vec<(CallbackOp, NamedRows, NamedRows)>>;

#[cfg(not(target_arch = "wasm32"))]
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
        for (k, vals) in collector {
            for (op, new, old) in vals {
                self.callback_sender
                    .send((k.clone(), op, new, old))
                    .expect("sending to callback processor failed");
            }
        }
    }
}
