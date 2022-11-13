// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#include "status.h"
#include "cozorocks/src/bridge/mod.rs.h"

void write_status(const Status &rstatus, RocksDbStatus &status) {
    status.code = rstatus.code();
    status.subcode = rstatus.subcode();
    status.severity = rstatus.severity();
    if (!rstatus.ok() && !rstatus.IsNotFound()) {
        status.message = rust::String::lossy(rstatus.ToString());
    }
}

RocksDbStatus convert_status(const Status &status) {
    RocksDbStatus ret;
    write_status(status, ret);
    return ret;
}