// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

static constexpr const char *kDBName = "operationsDB";

using TransferBatchTestParam = std::tuple<
    std::size_t,
    std::size_t,
    std::size_t>;
class TransferBatchTests : public testing::TestWithParam<TransferBatchTestParam>
{
protected:
    explicit TransferBatchTests()
    {
        (void)DB::destroy(Options(), kDBName);
    }

    ~TransferBatchTests() override
    {
        delete m_txn;
        delete m_db;
        delete m_env;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(reopen(true));

        // Add some pages to the freelist.
        Table *table;
        tools::RandomGenerator random;
        const auto records = tools::fill_db(*m_txn, "makeroom", random, 1'000);
        ASSERT_OK(m_txn->new_table(TableOptions(), "makeroom", table));
        for (const auto &[key, value] : records) {
            ASSERT_OK(table->erase(key));
        }
        delete table;
        ASSERT_OK(m_txn->commit());

        ASSERT_OK(reopen(true));
    }

    [[nodiscard]] auto reopen(bool write) -> Status
    {
        delete m_txn;
        delete m_db;
        m_txn = nullptr;
        m_db = nullptr;

        CALICODB_TRY(DB::open(Options(), kDBName, m_db));
        return m_db->new_txn(write, m_txn);
    }

    auto end_txn_and_validate() -> void
    {
        // Transaction must be finished when a checkpoint is run.
        delete m_txn;
        m_txn = nullptr;
        ASSERT_OK(m_db->checkpoint(false));
        ASSERT_OK(reopen(false));
        ASSERT_OK(m_routine.check(*m_txn, true));
    }

    TransferBatch m_routine{
        std::get<0>(GetParam()),
        std::get<1>(GetParam()),
    };
    Env *m_env = nullptr;
    Txn *m_txn = nullptr;
    DB *m_db = nullptr;
};

TEST_P(TransferBatchTests, TransferBatches)
{
    const auto num_rounds = std::get<2>(GetParam());
    for (std::size_t i = 0; i < num_rounds; ++i) {
        ASSERT_OK(m_routine.run(*m_txn));
        m_txn->rollback();
        ASSERT_OK(m_routine.run(*m_txn));
        ASSERT_OK(m_txn->commit());
        m_routine.round += i + 1 < num_rounds;
    }
    end_txn_and_validate();
}

INSTANTIATE_TEST_CASE_P(
    TransferBatchTests,
    TransferBatchTests,
    testing::Combine(
        testing::Values(1, 8, 32),
        testing::Values(1, 100, 1'000),
        testing::Values(1, 2)),
    [](const auto &info) -> std::string {
        std::string label;
        const auto ntab = std::get<0>(info.param);
        append_number(label, ntab);
        label.append("Table");
        label.append(ntab > 1 ? "s_" : "_");
        const auto nrec = std::get<1>(info.param);
        append_number(label, nrec);
        label.append("Record");
        if (nrec > 1) {
            label += 's';
        }
        label.append("_x");
        const auto ntimes = std::get<2>(info.param);
        append_number(label, ntimes);
        return label;
    });

} // namespace calicodb::test
