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
    [[nodiscard]] static auto make_record(std::size_t key) -> std::pair<std::string, std::string>;
    // Transfer a batch of `num_records` records between `num_tables` tables
    [[nodiscard]] auto run(Txn &txn) -> Status;
    [[nodiscard]] auto check(Txn &txn) -> Status;

    std::size_t num_tables;
    std::size_t num_records;
    std::size_t round = 0;
};

} // namespace calicodb::test

#endif // CALICODB_TEST_COMMON_H