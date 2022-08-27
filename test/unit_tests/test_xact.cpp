
#include <fstream>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/store.h"
#include "calico/transaction.h"
#include "core/core.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "pager/framer.h"
#include "store/disk.h"
#include "store/system.h"
#include "tools.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "utils/utils.h"
#include "wal/basic_wal.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"

namespace {

using namespace calico;
namespace fs = std::filesystem;

class XactTests: public TestOnDisk {
public:
    auto SetUp() -> void override
    {
        options.page_size = 0x400;
        options.frame_count = 64;
        options.log_level = spdlog::level::trace;
        options.store = store.get();

        ASSERT_TRUE(expose_message(core.open(ROOT, options)));
    }

    auto TearDown() -> void override
    {
        ASSERT_TRUE(expose_message(core.close()));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {123};
    Options options;
    Core core;
};

TEST_F(XactTests, NewDatabaseIsOk)
{
    ASSERT_TRUE(expose_message(core.status()));
}

template<class Action>
static auto with_xact(XactTests &test, const Action &action)
{
    auto xact = test.core.transaction();
    action();
    ASSERT_TRUE(expose_message(xact.commit()));
}

static auto insert_1000_records(XactTests &test)
{
    auto records = test.generator.generate(test.random, 1'000);
    for (const auto &r: records) {
        EXPECT_TRUE(expose_message(test.core.insert(stob(r.key), stob(r.value))));
    }
    return records;
}

auto erase_1000_records(XactTests &test)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(expose_message(test.core.erase(test.core.first())));
    }
}

TEST_F(XactTests, AbortFirstXact)
{
    auto xact = core.transaction();
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(core.info().record_count(), 0);

    // Normal operations after abort should work.
    insert_1000_records(*this);
    ASSERT_EQ(core.info().record_count(), 1'000);
}

TEST_F(XactTests, CommitIsACheckpoint)
{
    with_xact(*this, [this] {insert_1000_records(*this);});

    auto xact = core.transaction();
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(core.info().record_count(), 1'000);
}

TEST_F(XactTests, KeepsCommittedRecords)
{
    with_xact(*this, [this] {insert_1000_records(*this);});

    auto xact = core.transaction();
    erase_1000_records(*this);
    ASSERT_TRUE(expose_message(xact.abort()));
    ASSERT_EQ(core.info().record_count(), 1'000);

    // Normal operations after abort should work.
    with_xact(*this, [this] {erase_1000_records(*this);});
    ASSERT_EQ(core.info().record_count(), 0);
}

template<class Test, class Itr>
auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
{
    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(test.core.insert(stob(itr->key), stob(itr->value))));
    }

    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.next_int(5) == 0) {
            EXPECT_TRUE(expose_message(test.core.erase(stob(itr->key))));
        } else {
            committed.emplace_back(*itr);
        }
    }
    return committed;
}

TEST_F(XactTests, AbortRestoresPriorState)
{
    static constexpr Size NUM_RECORDS {500};
    const auto path = ROOT + std::string {DATA_FILENAME};
    const auto records = generator.generate(random, NUM_RECORDS);

    auto xact = core.transaction();
    auto committed = run_random_operations(*this, cbegin(records), cbegin(records) + NUM_RECORDS/2);
    ASSERT_TRUE(expose_message(xact.commit()));

    xact = core.transaction();
    run_random_operations(*this, cbegin(records) + NUM_RECORDS/2, cend(records));
    ASSERT_TRUE(expose_message(xact.abort()));

    // The database should contain exactly these records.
    ASSERT_EQ(core.info().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(core, key, value));
    }
}

template<class Test>
[[nodiscard]]
auto run_random_transactions(Test &test, Size n)
{
    static constexpr long XACT_SIZE {100};
    // Generate the records all at once, so we know that they are unique.
    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
    std::vector<Record> committed;

    for (Size i {}; i < n; ++i) {
        auto xact = test.core.transaction();
        const auto start = cbegin(all_records) + static_cast<long>(XACT_SIZE * i);
        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
        if (test.random.next_int(4UL) == 0) {
            EXPECT_TRUE(expose_message(xact.abort()));
        } else {
            EXPECT_TRUE(expose_message(xact.commit()));
            committed.insert(cend(committed), cbegin(temp), cend(temp));
        }
    }
    return committed;
}

TEST_F(XactTests, SanityCheck)
{
    for (const auto &[key, value]: run_random_transactions(*this, 20)) {
        ASSERT_TRUE(tools::contains(core, key, value));
    }
}

TEST_F(XactTests, PersistenceSanityCheck)
{
    ASSERT_TRUE(expose_message(core.close()));
    std::vector<Record> committed;

    for (Size i {}; i < 5; ++i) {
        ASSERT_TRUE(expose_message(core.open(ROOT, options)));
        const auto current = run_random_transactions(*this, 10);
        committed.insert(cend(committed), cbegin(current), cend(current));
        ASSERT_TRUE(expose_message(core.close()));
    }

    ASSERT_TRUE(expose_message(core.open(ROOT, options)));
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(core, key, value));
    }
}

TEST_F(XactTests, AtomicOperationSanityCheck)
{
    const auto all_records = generator.generate(random, 500);
    const auto committed = run_random_operations(*this, cbegin(all_records), cend(all_records));

    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(core, key, value));
    }
}

class FailureTests: public TestWithMock {
public:
    FailureTests() = default;

    ~FailureTests() override = default;

    auto SetUp() -> void override
    {
//        EXPECT_CALL(mock_store(), open_random_editor).Times(2);
//        EXPECT_CALL(mock_store(), open_random_reader).Times(1);
//        EXPECT_CALL(mock_store(), open_append_writer).Times(1);
        Options options;
        options.page_size = 0x200;
        options.frame_count = 16;
//        options.store = store.get();
        ASSERT_TRUE(expose_message(db.open(ROOT + std::string {"__"}, options)));

//        EXPECT_CALL(*data_mock(), sync).Times(1);
    }

    [[nodiscard]]
    auto data_mock() -> MockRandomEditor*
    {
        return mock_store().get_mock_random_editor(DATA_FILENAME);
    }

    [[nodiscard]]
    auto wal_writer_mock(SegmentId id) -> MockAppendWriter*
    {
        return mock_store().get_mock_append_writer(ROOT + id.to_name());
    }

    [[nodiscard]]
    auto wal_reader_mock(SegmentId id) -> MockRandomReader*
    {
        return mock_store().get_mock_random_reader(ROOT + id.to_name());
    }

    Database db;
};



TEST_F(FailureTests, A)
{
//    auto *mock = data_mock();
//    (void)mock;
}

} // <anonymous>