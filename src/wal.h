// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "calicodb/db.h"
#include "utils.h"
#include <functional>
#include <vector>

namespace calicodb
{

class Env;
class File;
struct PageRef;

struct HashIndexHdr {
    U32 version;
    U32 unused0;
    U32 change;
    U16 is_init;
    U16 unused1;
    U32 max_frame;
    U32 page_count;
    U32 frame_cksum[2];
    U32 salt[2];
    U32 cksum[2];
};

class HashIndex final
{
public:
    friend class HashIterator;

    using Key = U32;
    using Value = U32;

    explicit HashIndex(HashIndexHdr &header, File *file);
    [[nodiscard]] auto fetch(Value value) -> Key;
    [[nodiscard]] auto lookup(Key key, Value lower, Value &out) -> Status;
    [[nodiscard]] auto assign(Key key, Value value) -> Status;
    [[nodiscard]] auto header() -> volatile HashIndexHdr *;
    [[nodiscard]] auto groups() const -> const std::vector<volatile char *> &;
    auto cleanup() -> void;
    auto close() -> void;

private:
    friend class WalImpl;

    [[nodiscard]] auto map_group(std::size_t group_number, bool extend) -> Status;

    // Storage for hash table groups.
    std::vector<volatile char *> m_groups;

    // Address of the hash table header kept in memory. This version of the header corresponds
    // to the current transaction. The one stored in the first table group corresponds to the
    // most-recently-committed transaction.
    HashIndexHdr *m_hdr;

    File *m_file;
};

// Construct for iterating through the hash index.
class HashIterator final
{
public:
    using Key = HashIndex::Key;
    using Value = HashIndex::Value;

    struct Entry {
        Key key = 0;
        Value value = 0;
    };

    ~HashIterator();

    // Create an iterator over the contents of the provided hash index.
    explicit HashIterator(HashIndex &index);
    [[nodiscard]] auto init(U32 backfill = 0) -> Status;

    // Return the next hash entry.
    //
    // This method should return a key that is greater than the last key returned by this
    // method, along with the most-recently-set value.
    [[nodiscard]] auto read(Entry &out) -> bool;

private:
    struct State {
        struct Group {
            Key *keys;
            U16 *index;
            U32 size;
            U32 next;
            U32 base;
        } groups[1];
    };

    HashIndex *m_source = nullptr;
    State *m_state = nullptr;
    std::size_t m_num_groups = 0;
    Key m_prior = 0;
};

class Wal
{
public:
    struct Parameters {
        const char *wal_name;
        const char *db_name;
        Env *env;
        File *db_file;
        Logger *info_log;
        BusyHandler *busy;
        Options::SyncMode sync_mode;
        Options::LockMode lock_mode;
    };

    virtual ~Wal();

    // Open or create a WAL file called "filename".
    [[nodiscard]] static auto open(const Parameters &param, Wal *&out) -> Status;
    [[nodiscard]] virtual auto close() -> Status = 0;

    // Write as much of the WAL back to the DB as possible
    [[nodiscard]] virtual auto checkpoint(bool reset) -> Status = 0;

    // UNLOCKED -> READER
    [[nodiscard]] virtual auto start_reader(bool &changed) -> Status = 0;

    // Read the most-recent version of page "page_id" from the WAL.
    //
    // "page" must point to at least a full page of memory. It is set to nullptr if
    // page "page_id" does not exist in the WAL.
    [[nodiscard]] virtual auto read(Id page_id, char *&page) -> Status = 0;

    // READER -> WRITER
    [[nodiscard]] virtual auto start_writer() -> Status = 0;

    // Write new versions of the given pages to the WAL.
    [[nodiscard]] virtual auto write(PageRef *dirty, std::size_t db_size) -> Status = 0;

    using Undo = std::function<void(Id)>;
    virtual auto rollback(const Undo &undo) -> void = 0;

    // WRITER -> READER
    virtual auto finish_writer() -> void = 0;

    // READER -> UNLOCKED
    virtual auto finish_reader() -> void = 0;

    enum StatType {
        kStatReadWal,
        kStatWriteWal,
        kStatWriteDB,
        kStatTypeCount
    };
    using Stats = StatCounters<kStatTypeCount>;

    [[nodiscard]] virtual auto stats() const -> const Stats & = 0;
};

template <class Callback>
auto busy_wait(BusyHandler *handler, const Callback &callback) -> Status
{
    for (unsigned n = 0;; ++n) {
        auto s = callback();
        if (s.is_busy()) {
            if (handler && handler->exec(n)) {
                continue;
            }
        }
        return s;
    }
}

} // namespace calicodb

#endif // CALICODB_WAL_H
