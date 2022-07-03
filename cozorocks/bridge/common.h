//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_ROCKS_BRIDGE_H
#define COZOROCKS_ROCKS_BRIDGE_H

#include "rust/cxx.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"

using namespace rocksdb;
using namespace std;

struct RdbStatus;
struct DbOpts;

typedef Status::Code StatusCode;
typedef Status::SubCode StatusSubCode;
typedef Status::Severity StatusSeverity;


#endif //COZOROCKS_ROCKS_BRIDGE_H
