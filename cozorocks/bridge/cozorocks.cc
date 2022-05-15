//
// Created by Ziyang Hu on 2022/4/13.
//

#include "cozorocks.h"
#include "cozorocks/src/bridge.rs.h"

void write_status_impl(BridgeStatus &status, StatusCode code, StatusSubCode subcode, StatusSeverity severity,
                       int bridge_code) {
    status.code = code;
    status.subcode = subcode;
    status.severity = severity;
    status.bridge_code = static_cast<StatusBridgeCode>(bridge_code);
}

BridgeStatus IteratorBridge::status() const {
    BridgeStatus s;
    write_status(inner->status(), s);
    return s;
}

//unique_ptr<vector<PinnableSlice>> TransactionBridge::multiget_txn(
////        const ColumnFamilyHandle &cf,
//        rust::Slice<const rust::Slice<const uint8_t>> keys,
//        rust::Slice<BridgeStatus> statuses) const {
//    auto cpp_keys = vector<Slice>();
//    cpp_keys.reserve(keys.size());
//    for (auto key: keys) {
//        auto cpp_key = convert_slice(key);
//        cpp_keys.push_back(cpp_key);
//    }
//    auto ret = make_unique<vector<PinnableSlice>>(keys.size());
//    auto cpp_statuses = vector<Status>(keys.size());
//    inner->MultiGet(*r_ops,
//                    raw_db->DefaultColumnFamily(),
////                    const_cast<ColumnFamilyHandle *>(&cf),
//                    keys.size(),
//                    cpp_keys.data(),
//                    ret->data(),
//                    cpp_statuses.data());
//    for (size_t i = 0; i < cpp_statuses.size(); ++i) {
//        write_status(std::move(cpp_statuses[i]), statuses[i]);
//    }
//    return ret;
//}
//
//unique_ptr<vector<PinnableSlice>> TransactionBridge::multiget_raw(
////        const ColumnFamilyHandle &cf,
//        rust::Slice<const rust::Slice<const uint8_t>> keys,
//        rust::Slice<BridgeStatus> statuses) const {
//    auto cpp_keys = vector<Slice>();
//    cpp_keys.reserve(keys.size());
//    for (auto key: keys) {
//        auto cpp_key = convert_slice(key);
//        cpp_keys.push_back(cpp_key);
//    }
//    auto ret = make_unique<vector<PinnableSlice>>(keys.size());
//    auto cpp_statuses = vector<Status>(keys.size());
//    raw_db->MultiGet(*r_ops,
//                     raw_db->DefaultColumnFamily(),
////                     const_cast<ColumnFamilyHandle *>(&cf),
//                     keys.size(),
//                     cpp_keys.data(),
//                     ret->data(),
//                     cpp_statuses.data());
//    for (size_t i = 0; i < cpp_statuses.size(); ++i) {
//        write_status(std::move(cpp_statuses[i]), statuses[i]);
//    }
//    return ret;
//}