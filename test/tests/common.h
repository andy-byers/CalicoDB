// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TEST_COMMON_H
#define CALICODB_TEST_COMMON_H

#include "calicodb/db.h"
#include "tools.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

static constexpr auto kExpectationMatcher = "^expectation";

#define STREAM_MESSAGE(expr) #expr                                    \
                                 << " == Status::ok()` but got \""    \
                                 << get_status_name(expect_ok_status) \
                                 << "\" status with message \""       \
                                 << expect_ok_status.to_string()      \
                                 << "\"\n";

#define EXPECT_OK(expr)                        \
    do {                                       \
        const auto &expect_ok_status = (expr); \
        EXPECT_TRUE(expect_ok_status.is_ok())  \
            << "expected `"                    \
            << STREAM_MESSAGE(expr);           \
    } while (0)

#define ASSERT_OK(expr)                        \
    do {                                       \
        const auto &expect_ok_status = (expr); \
        ASSERT_TRUE(expect_ok_status.is_ok())  \
            << "asserted `"                    \
            << STREAM_MESSAGE(expr);           \
    } while (0)

struct TransferBatch {
    explicit TransferBatch(std::size_t ntab, std::size_t nrec);
    // Transfer a batch of `num_records` records between `num_tables` tables
    [[nodiscard]] auto run(Txn &txn) -> Status;
    [[nodiscard]] auto check(Txn &txn, bool validate) -> Status;

    std::size_t num_tables;
    std::size_t num_records;
    std::size_t round = 0;
};

[[nodiscard]] static auto make_record(U32 id, U32 iteration = 0) -> std::pair<std::string, std::string>;
[[nodiscard]] static auto put_random(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto put_random(Table &table, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto put_sequential(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto put_sequential(Table &table, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto erase_all(Txn &txn, const std::string &tbname, bool drop_table) -> Status;
[[nodiscard]] static auto erase_all(Table &table) -> Status;
[[nodiscard]] static auto check_records(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto check_records(Table &table, std::size_t num_records, U32 iteration = 0) -> Status;
[[nodiscard]] static auto is_empty(Txn &txn, const std::string &tbname) -> bool;
[[nodiscard]] static auto is_empty(Table &table) -> bool;

template <class Callback>
[[nodiscard]] auto with_txn(DB &db, bool write, const std::string &tbname, const Callback &callback) -> Status
{
    Status s;
    Txn *txn;
    if ((s = db.new_txn(write, txn)).is_ok()) {
        s = callback(*txn);
        delete txn;
    }
    return s;
}

template <class Callback>
[[nodiscard]] auto with_table(Txn &txn, const std::string &tbname, const Callback &callback) -> Status
{
    Status s;
    Table *table;
    if ((s = txn.new_table(TableOptions(), tbname, table)).is_ok()) {
        s = callback(*table);
        delete table;
    }
    return s;
}

template <class Callback>
[[nodiscard]] auto with_table(DB &db, bool write, const std::string &tbname, const Callback &callback) -> Status
{
    return with_txn(db, write, [&tbname, &callback](auto &txn) {
        return with_table(txn, tbname, callback);
    });
}

} // namespace calicodb::test

#endif // CALICODB_TEST_COMMON_H