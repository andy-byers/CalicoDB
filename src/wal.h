// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "calicodb/db.h"
#include "utils.h"

namespace calicodb
{

class Env;
class File;
struct PageRef;
struct Stat;

// WAL state transitions:
//      Start mode | Method          | End mode
//     ------------|-----------------|----------
//      Unlocked   | start_reader()  | Reader
//      Reader     | start_writer()  | Writer
//      Writer     | finish_writer() | Reader
//      Reader     | finish_reader() | Unlocked
class Wal
{
public:
    struct Parameters {
        const char *filename;
        Env *env;
        File *db_file;
        Logger *info_log;
        Stat *stat;
        BusyHandler *busy;
        Options::SyncMode sync_mode;
        Options::LockMode lock_mode;
    };

    virtual ~Wal();

    // Open or create a WAL file called `param.filename`
    static auto open(const Parameters &param, Wal *&out) -> Status;
    virtual auto close() -> Status = 0;

    // Write as much of the WAL back to the DB as possible
    // REQUIRES: WAL is in Unlocked mode
    virtual auto checkpoint(bool reset) -> Status = 0;

    // REQUIRES: WAL is in Unlocked mode
    virtual auto start_reader(bool &changed) -> Status = 0;

    // Read the most-recent version of page `page_id` from the WAL
    // `page_out` must point to at least a full page of memory. On success, reads page `page_id`
    // into `page_out` and returns an OK status. Otherwise, sets `page_out` to nullptr and
    // returns a non-OK status.
    // REQUIRES: WAL is in Reader mode
    virtual auto read(Id page_id, char *&page_out) -> Status = 0;

    // State transition from "reader locked" to "writer locked"
    // REQUIRES: WAL is in Reader mode
    virtual auto start_writer() -> Status = 0;

    // Write new versions of the given pages to the WAL
    // REQUIRES: WAL is in Writer mode
    virtual auto write(PageRef *dirty, size_t db_size) -> Status = 0;

    // REQUIRES: WAL is in Writer mode
    using Undo = void (*)(void *, Id);
    virtual auto rollback(const Undo &undo, void *object) -> void = 0;

    // REQUIRES: WAL is in Writer mode
    virtual auto finish_writer() -> void = 0;

    // Unlock the WAL
    // REQUIRES: WAL is in Reader mode
    virtual auto finish_reader() -> void = 0;

    [[nodiscard]] virtual auto last_frame_count() const -> size_t = 0;
    [[nodiscard]] virtual auto db_size() const -> uint32_t = 0;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
