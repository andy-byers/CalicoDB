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

    // Write a new version of page "page_id" to the WAL.
    [[nodiscard]] virtual auto write(Id page_id, const Slice &page, std::size_t db_size) -> Status = 0;

    // Write the WAL contents back to the DB. Resets internal counters such
    // that the next write to the WAL will start at the beginning again.
    [[nodiscard]] virtual auto checkpoint(File &db_file) -> Status = 0;

    [[nodiscard]] virtual auto sync() -> Status = 0;

    [[nodiscard]] virtual auto commit() -> Status = 0;

    [[nodiscard]] virtual auto statistics() const -> WalStatistics = 0;
};

struct WalIndexHeader {
    std::uint32_t version = 0;
    std::uint32_t change = 0;
    bool is_init = false;
    std::uint32_t page_size = 0;
    std::uint32_t max_frame = 0;
    std::uint32_t page_count = 0;
    std::uint32_t frame_checksum = 0;
    std::uint32_t salt[2] = {};
    std::uint32_t checksum[2] = {};
};

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
    std::vector<char *> m_tables;
    WalIndexHeader *m_header = nullptr;
    std::size_t m_frame_number = 0;

    struct WalTable {
        std::uint32_t *frames = nullptr;
        std::uint16_t *hashes = nullptr;
        std::size_t base = 0;
    };

    [[nodiscard]] auto create_or_open_table(std::size_t table_number) -> WalTable;

public:
    ~WalIndex();
    explicit WalIndex(WalIndexHeader &header);
    [[nodiscard]] auto frame_for_page(Id page_id, Id min_frame_id, Id &out) -> Status;
    [[nodiscard]] auto page_for_frame(Id frame_id) -> Id;
    [[nodiscard]] auto assign(Id page_id, Id frame_id) -> Status;
    auto cleanup() -> void;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
