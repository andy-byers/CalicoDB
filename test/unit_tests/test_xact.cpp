#include <fstream>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/store.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "pager/framer.h"
#include "store/disk.h"
#include "store/system.h"
#include "tools.h"
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

class XactTests: public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_xact_tests/";

    XactTests()
    {
        std::error_code ignore;
        fs::remove_all(ROOT, ignore);
    }

    ~XactTests() override
    {
//        std::error_code ignore;
//        fs::remove_all(ROOT, ignore);
    }

    auto SetUp() -> void override
    {
        store = std::make_unique<DiskStorage>();
        store->create_directory(ROOT);

        WriteAheadLog *temp {};
        BasicWriteAheadLog::Parameters param {
            ROOT,
            store.get(),
            create_sink(ROOT, spdlog::level::trace),
            0x200,
        };
        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open(param, &temp)));
        wal.reset(temp);

        options.page_size = 0x200;
        options.frame_count = 16;
        options.log_level = spdlog::level::trace;
        options.store = store.get();
        options.wal = wal.get();

        ASSERT_TRUE(expose_message(db.open(ROOT, options)));
        ASSERT_TRUE(db.is_open());
    }

    auto TearDown() -> void override
    {
        if (db.is_open()) {
            ASSERT_TRUE(expose_message(db.close()));
        }
        ASSERT_FALSE(db.is_open());
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    std::unique_ptr<Storage> store;
    std::unique_ptr<WriteAheadLog> wal;
    Random random {123};
    Options options;
    Database db;
};

TEST_F(XactTests, NewDatabaseIsOk)
{
    ASSERT_TRUE(expose_message(db.status()));
}

TEST_F(XactTests, WalIsReadyAfterStartup)
{
    ASSERT_TRUE(wal->is_enabled());
    ASSERT_TRUE(wal->is_writing());
}

TEST_F(XactTests, CommittingEmptyXactIsOk)
{
    ASSERT_TRUE(expose_message(db.commit()));
}

TEST_F(XactTests, AbortingEmptyXactIsOk)
{
    ASSERT_TRUE(expose_message(db.abort()));
}

auto insert_1000_records(XactTests &test)
{
    auto records = test.generator.generate(test.random, 1'000);
    for (const auto &r: records) {
        EXPECT_TRUE(expose_message(test.db.insert(r)));
    }
    return records;
}

auto erase_1000_records(XactTests &test)
{
    for (Size i {}; i < 1'000; ++i) {
        ASSERT_TRUE(expose_message(test.db.erase(test.db.find_minimum())));
    }
}

TEST_F(XactTests, SequenceIdsAlwaysIncrease)
{
    insert_1000_records(*this);

    // The WAL design should allow stopping and starting at any time. We should be able to call redo_all() safely, and the WAL
    // will just reload the positions of each WAL record in the most-recent transaction. If we want to call undo_last() manually,
    // we should return a non-OK status from the callback at some point, otherwise the WAL will get rid of the segments belonging to
    // the most-recent transaction. Normally, we wouldn't want to call either of these methods directly. This is just for testing!
    SequenceId last_lsn;
    ASSERT_TRUE(expose_message(wal->stop_writer()));
    ASSERT_TRUE(expose_message(wal->redo_all([&last_lsn](const auto &descriptor) {
        EXPECT_LT(last_lsn, descriptor.page_lsn);
        return Status::ok();
    })));
    ASSERT_TRUE(expose_message(wal->start_writer()));
}

TEST_F(XactTests, AbortFirstXact)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 0);

    // Normal operations after abort should work.
    insert_1000_records(*this);
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests, CommitIsACheckpoint)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.commit()));
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests, AbortSecondXact)
{
    insert_1000_records(*this);
    ASSERT_TRUE(expose_message(db.commit()));
    erase_1000_records(*this);
    ASSERT_TRUE(expose_message(db.abort()));
    ASSERT_EQ(db.info().record_count(), 1'000);

    // Normal operations after abort should work.
    erase_1000_records(*this);
    ASSERT_EQ(db.info().record_count(), 0);
}

template<class Itr>
auto run_random_operations(XactTests &test, const Itr &begin, const Itr &end)
{
    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(test.db.insert(*itr)));
    }
    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.next_int(5) == 0) {
            EXPECT_TRUE(expose_message(test.db.erase(itr->key)));
        } else {
            committed.emplace_back(*itr);
        }
    }
    return committed;
}

[[nodiscard]]
auto read_whole_file(const std::string &path) -> std::string
{
    std::string data;
    std::ifstream ifs {path};
    EXPECT_TRUE(ifs.is_open());
    ifs >> data;
    return data;
}

TEST_F(XactTests, AbortRestoresPriorState)
{
    const auto path = ROOT + std::string{"/data"};
    const auto before = read_whole_file(path);
    const auto records = generator.generate(random, 500);
    for (const auto &r: run_random_operations(*this, cbegin(records), cend(records))) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
    ASSERT_TRUE(expose_message(db.abort()));
    const auto after = read_whole_file(path);

    // TODO: Pager component should truncate the data file given the new page_count value after the abort. The second argument passed to substr() below should be removed once this is implemented.
    ASSERT_EQ(before.substr(sizeof(FileHeader)), after.substr(sizeof(FileHeader), before.size() - sizeof(FileHeader)));
}

[[nodiscard]]
auto run_random_transactions(XactTests &test, Size n)
{
    static constexpr Size XACT_SIZE {9};

    // Generate the records all at once so that we know they are unique.
    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
    std::vector<Record> committed;

    for (Size i {}; i < n; ++i) {
        const auto start = cbegin(all_records) + long(XACT_SIZE*i);
        for (auto itr = start; itr != start + XACT_SIZE; ++itr) {
            EXPECT_TRUE(expose_message(test.db.insert(*itr)));
        }
        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
//        if (test.random.next_int(4UL) == 0) {
            EXPECT_TRUE(expose_message(test.db.abort()));
//        } else {
//            EXPECT_TRUE(expose_message(test.db.commit()));
//            committed.insert(cend(committed), cbegin(temp), cend(temp));
//        }
    }
    return committed;
}

TEST_F(XactTests, SanityCheck_1)
{
    for (const auto &r: run_random_transactions(*this, 1)) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
}

TEST_F(XactTests, SanityCheck_10)
{
    for (const auto &r: run_random_transactions(*this, 3)) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
}

TEST_F(XactTests, SanityCheck_100)
{
    for (const auto &r: run_random_transactions(*this, 100)) {
        ASSERT_TRUE(tools::contains(db, r.key));
    }
}

} // <anonymous>