// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef COZOROCKS_OPTS_H
#define COZOROCKS_OPTS_H

#include "common.h"

inline void set_w_opts_sync(WriteOptions& opts, bool val) {
    opts.sync = val;
}

inline void set_w_opts_disable_wal(WriteOptions& opts, bool val) {
    opts.disableWAL = val;
}

inline void set_w_opts_no_slowdown(WriteOptions& opts, bool val) {
    opts.no_slowdown = val;
}

#endif //COZOROCKS_OPTS_H
