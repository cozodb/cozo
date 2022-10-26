/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */

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