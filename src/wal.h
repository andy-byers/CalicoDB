// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "utils.h"
#include <vector>

namespace calicodb
{

class Env;
class File;
class WalIndex;
class WalIterator;
struct CacheEntry;

struct WalStatistics {
    std::size_t bytes_read = 0;
    std::size_t bytes_written = 0;
};

// New pager page acquire routine:
//   1. Check if the page is cached
//      + If it is, return it
//      + If it is not, goto 2
//   2. Check if the page is in the dirty table
//      + If it is not, read it from the database file
//      + If it is, goto 3
//   3. Read the WAL index to determine the most-recent frame containing the page
//   4. Use the frame contents to reconstruct the page
//   5. Cache and return the page

class Wal
{
public:
    struct Parameters {
        std::string filename;
        std::size_t page_size = 0;
        Env *env = nullptr;
    };

    virtual ~Wal();

    // Open or create a WAL file called "filename".
    [[nodiscard]] static auto open(const Parameters &param, Wal *&out) -> Status;

    // Read the most-recent version of page "page_id" from the WAL.
    [[nodiscard]] virtual auto read(Id page_id, char *page) -> Status = 0;

    struct PageDescriptor {
        Id page_id;
        Slice data;
    };

    // Write new versions of the given pages to the WAL.
    [[nodiscard]] virtual auto write(const CacheEntry *dirty, std::size_t db_size) -> Status = 0;

    // Write the WAL contents back to the DB. Resets internal counters such
    // that the next write to the WAL will start at the beginning again.
    [[nodiscard]] virtual auto checkpoint(File &db_file) -> Status = 0;

    [[nodiscard]] virtual auto sync() -> Status = 0;

    [[nodiscard]] virtual auto commit() -> Status = 0;

    [[nodiscard]] virtual auto statistics() const -> WalStatistics = 0;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
