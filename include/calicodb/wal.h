// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_H
#define CALICODB_WAL_H

#include "options.h"
#include "slice.h"
#include "status.h"

namespace calicodb
{

// Construct for recording database transactions apart from the main database file
// Details about the default behavior, and requirements for implementors are
// listed below.
// The default WAL implementation uses both the shared memory and file locking APIs
// (see calicodb/env.h) to speed up lookups for specific pages, and to coordinate
// concurrent access from multiple connections, respectively.
// Conceptually, the WAL is always in 1 of 4 states: Closed, Open, Reader, or Writer.
// State transitions are performed by the following methods:
//   Method       | Before   | After
//  --------------|----------|---------
//   open         | Closed   | Open
//   start_read   | Open     | Reader
//   start_write  | Reader   | Writer
//   finish_write | Writer   | Reader
//   finish_read  | Open     | Open
//   finish_read  | Reader   | Open
//   finish_read  | Writer   | Open
//   close        | Open     | Closed
//
// If a method returns with Status::ok(), then the WAL is expected to be in the "After"
// state shown above. Otherwise, it is kept in the "Before" state. If a method has no
// return, then it is expected to make the state transition without fail.
// NOTE: If concurrency is not required, the WAL implementation can be pretty simple.
//       See src/temp.cpp for an example that uses a hash table to keep track of pages.
class Wal
{
public:
    explicit Wal();
    virtual ~Wal();

    // REQUIRES: WAL is in "Closed" mode
    virtual auto open(const WalOptions &options, const char *filename) -> Status = 0;

    // REQUIRES: WAL is in "Open" mode
    virtual auto close(char *scratch, uint32_t page_size) -> Status = 0;

    // Return the number of frames in the WAL when this connection last committed
    // This is the value that is compared with the "auto_checkpoint" threshold. Returns 0 if
    // this connection hasn't committed, or if this method has already been called since the
    // last commit.
    [[nodiscard]] virtual auto callback() -> uint32_t = 0;

    // Return the number of pages in the database file
    // Returns 0 if the value is not yet known, i.e. the WAL is not running a transaction.
    [[nodiscard]] virtual auto db_size() const -> uint32_t = 0;

    // REQUIRES: WAL is in "Open" mode
    virtual auto start_read(bool &changed) -> Status = 0;

    // Unconditionally switch the WAL into "Open" mode
    virtual void finish_read() = 0;

    // REQUIRES: WAL is in "Reader" mode
    virtual auto start_write() -> Status = 0;

    // REQUIRES: WAL is in "Writer" mode
    virtual void finish_write() = 0;

    // Iterator over a set of pages that needs to be written to the WAL
    class Pages
    {
    public:
        explicit Pages();
        virtual ~Pages();

        struct Data {
            const char *data;
            uint16_t *flag;
            uint32_t page_id;
        };

        virtual auto value() const -> Data * = 0;
        virtual void next() = 0;
        virtual void reset() = 0;
    };

    // REQUIRES: WAL is in "Reader" mode
    virtual auto read(uint32_t page_id, uint32_t page_size, char *&page_out) -> Status = 0;

    // REQUIRES: WAL is in "Writer" mode
    virtual auto write(Pages &pages, uint32_t page_size, size_t db_size) -> Status = 0;

    using Rollback = void (*)(void *, uint32_t);

    // REQUIRES: WAL is in "Writer" mode
    virtual void rollback(const Rollback &hook, void *arg) = 0;

    // REQUIRES: WAL is in "Open" mode
    virtual auto checkpoint(CheckpointMode mode,
                            char *scratch,
                            uint32_t scratch_size,
                            BusyHandler *busy,
                            CheckpointInfo *info_out) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
