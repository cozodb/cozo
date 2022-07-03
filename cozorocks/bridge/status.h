//
// Created by Ziyang Hu on 2022/7/3.
//

#ifndef COZOROCKS_STATUS_H
#define COZOROCKS_STATUS_H

#include "common.h"

void write_status(const Status &rstatus, RdbStatus &status);

RdbStatus convert_status(const Status &status);

#endif //COZOROCKS_STATUS_H
