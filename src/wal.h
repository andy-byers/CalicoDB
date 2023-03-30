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

struct WalIndexHeader {
    U32 version;
    U32 unused;
    U32 change;
    U16 flags;
    U16 page_size;
    U32 max_frame;
    U32 page_count;
    U32 frame_checksum;
    U32 salt[2];
    U32 checksum[2];
};

static_assert(std::is_pod_v<WalIndexHeader>);
static_assert(sizeof(WalIndexHeader) == 44);

// The WAL index looks like this in memory:
//
//     <h><frames><hashes>
//     <frames   ><hashes>
//     <frames   ><hashes>
//     ...
//
// "<h>" represents the index header.

class WalIndex final
{
    friend class WalImpl;

    std::vector<char *> m_segments;
    WalIndexHeader *m_header = nullptr;
    std::size_t m_frame_number = 0;

    struct Segment {
        U32 *frames = nullptr;
        U16 *hashes = nullptr;
        std::size_t base = 0;
    };

    [[nodiscard]] auto create_or_open_segment(std::size_t table_number) -> Segment;

public:
    ~WalIndex();
    explicit WalIndex(WalIndexHeader &header);
    [[nodiscard]] auto frame_for_page(Id page_id, Id min_frame_id, Id &out) -> Status;
    [[nodiscard]] auto page_for_frame(Id frame_id) -> Id;
    [[nodiscard]] auto assign(Id page_id, Id frame_id) -> Status;
    [[nodiscard]] auto header() -> WalIndexHeader *;
    auto cleanup() -> void;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
