// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STAT_H
#define CALICODB_STAT_H

#include "utils.h"

namespace calicodb
{

struct Stat {
    enum Type {
        kCacheHits,
        kCacheMisses,
        kReadDB,
        kWriteDB,
        kSyncDB,
        kReadWal,
        kWriteWal,
        kSyncWal,
        kSMOCount,
        kTypeCount
    };

    uint64_t counters[kTypeCount] = {};
};

} // namespace calicodb

#endif // CALICODB_STAT_H
