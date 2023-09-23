// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STATS_H
#define CALICODB_STATS_H

#include <cstdint>

namespace calicodb
{

// Statistics information for a database
struct Stats {
    // Pager cache hit ratio.
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;

    // Number of bytes transferred from/to the database file.
    uint64_t read_db = 0;
    uint64_t write_db = 0;

    // Number of File::sync() calls on the database file.
    uint64_t sync_db = 0;

    // Number of bytes transferred from/to the WAL file.
    uint64_t read_wal = 0;
    uint64_t write_wal = 0;

    // Number of File::sync() calls on the WAL file.
    uint64_t sync_wal = 0;

    // Number of structure modification operations (SMOs) performed on all trees.
    uint64_t tree_smo = 0;
};

} // namespace calicodb

#endif // CALICODB_STATS_H
