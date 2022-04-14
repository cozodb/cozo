//
// Created by Ziyang Hu on 2022/4/13.
//

#include "../include/cozorocks.h"
#include "cozo-rocks/src/lib.rs.h"

void write_status_impl(Status &status, StatusCode code, StatusSubCode subcode, StatusSeverity severity) {
    status.code = code;
    status.subcode = subcode;
    status.severity = severity;
}

Status IteratorBridge::status() const {
    Status s;
    write_status(inner->status(), s);
    return s;
}