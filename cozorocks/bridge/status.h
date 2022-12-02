// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef COZOROCKS_STATUS_H
#define COZOROCKS_STATUS_H

#include "common.h"

void write_status(const Status &rstatus, RocksDbStatus &status);

RocksDbStatus convert_status(const Status &status);

#endif //COZOROCKS_STATUS_H
