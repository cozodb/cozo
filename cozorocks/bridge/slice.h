//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_SLICE_H
#define COZOROCKS_SLICE_H

#include "common.h"

inline Slice convert_slice(RustBytes d) {
    return Slice(reinterpret_cast<const char *>(d.data()), d.size());
}

inline RustBytes convert_slice_back(const Slice &s) {
    return rust::Slice(reinterpret_cast<const std::uint8_t *>(s.data()), s.size());
}

inline RustBytes convert_pinnable_slice_back(const PinnableSlice &s) {
    return rust::Slice(reinterpret_cast<const std::uint8_t *>(s.data()), s.size());
}

#endif //COZOROCKS_SLICE_H
