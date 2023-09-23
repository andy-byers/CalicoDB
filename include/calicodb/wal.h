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

class Wal
{
public:
    explicit Wal();
    virtual ~Wal();

    virtual auto open(const WalOptions &options) -> Status = 0;
    virtual auto close(char *scratch, uint32_t page_size) -> Status = 0;

    [[nodiscard]] virtual auto wal_size() const -> size_t = 0;
    [[nodiscard]] virtual auto db_size() const -> size_t = 0;

    virtual auto start_read(bool &changed) -> Status = 0;
    virtual auto finish_read() -> void = 0;
    virtual auto start_write() -> Status = 0;
    virtual auto finish_write() -> void = 0;

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
        virtual auto next() -> void = 0;
        virtual auto reset() -> void = 0;
    };

    virtual auto read(uint32_t page_id, uint32_t page_size, char *&page_out) -> Status = 0;
    virtual auto write(Pages &pages, uint32_t page_size, size_t db_size) -> Status = 0;

    using Rollback = void (*)(void *, uint32_t);
    virtual auto rollback(const Rollback &hook, void *arg) -> void = 0;
    virtual auto checkpoint(bool reset, char *scratch, uint32_t page_size) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_WAL_H
