// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef COZOROCKS_SLICE_H
#define COZOROCKS_SLICE_H

#include "common.h"

inline Slice convert_slice(RustBytes d) {
    return {reinterpret_cast<const char *>(d.data()), d.size()};
}

inline string convert_slice_to_string(RustBytes d) {
    return {reinterpret_cast<const char *>(d.data()), d.size()};
}

inline RustBytes convert_slice_back(const Slice &s) {
    return {reinterpret_cast<const std::uint8_t *>(s.data()), s.size()};
}

inline RustBytes convert_pinnable_slice_back(const PinnableSlice &s) {
    return {reinterpret_cast<const std::uint8_t *>(s.data()), s.size()};
}

#endif //COZOROCKS_SLICE_H
