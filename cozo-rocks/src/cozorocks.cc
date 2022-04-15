////
//// Created by Ziyang Hu on 2022/4/13.
////
//
#include "../include/cozorocks.h"
#include "cozo-rocks/src/lib.rs.h"

void write_status_impl(BridgeStatus &status, StatusCode code, StatusSubCode subcode, StatusSeverity severity, int bridge_code) {
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