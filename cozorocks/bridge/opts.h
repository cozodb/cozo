//
// Created by Ziyang Hu on 2022/7/3.
//

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
