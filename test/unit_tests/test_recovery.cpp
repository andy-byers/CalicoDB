/*
 * Recovery tests (harness is modified from LevelDB).
 */
#include "calico/calico.h"
#include "tools.h"
#include "unit_tests.h"

namespace Calico {

class RecoveryTestHarness : public InMemoryTest {
public:
    RecoveryTestHarness()
        : db_prefix {PREFIX}
    {
        db_options.page_size = MINIMUM_PAGE_SIZE;
        db_options.cache_size = MINIMUM_PAGE_SIZE * 16;
        db_options.storage = storage.get();
        open();
    }

    ~RecoveryTestHarness() override
    {
        close();
    }

    auto impl() const -> DatabaseImpl *
    {
        return reinterpret_cast<DatabaseImpl *>(db);
    }

    void close()
    {
        delete db;
        db = nullptr;
    }

    auto open_with_status(Options *options = nullptr) -> Status
    {
        close();
        Options opts = db_options;
        if (options != nullptr) {
            opts = *options;
        }
        if (opts.storage == nullptr) {
            opts.storage = storage.get();
        }
        tail.resize(wal_block_size(opts.page_size));
        return Database::open(db_prefix, opts, &db);
    }

    auto open(Options *options = nullptr) -> void
    {
        ASSERT_OK(open_with_status(options));
    }

    auto put(const std::string &k, const std::string &v) const -> Status
    {
        return db->put(k, v);
    }

    auto get(const std::string &k) const -> std::string
    {
        std::string result;
        Status s = db->get(k, result);
        if (s.is_not_found()) {
            result = "NOT_FOUND";
        } else if (!s.is_ok()) {
            result = s.to_string();
        }
        return result;
    }

    auto log_name(Id id) const -> std::string
    {
        return encode_segment_name(db_prefix + "wal-", id);
    }

    auto remove_log_files() -> size_t
    {
        // Linux allows unlinking open files, but Windows does not.
        // Closing the db allows for file deletion.
        close();
        std::vector<Id> logs = get_logs();
        for (const auto &log: logs) {
            EXPECT_OK(storage->remove_file(encode_segment_name(db_prefix + "wal-", log)));
        }
        return logs.size();
    }

    auto get_logs() -> std::vector<Id>
    {
        std::vector<std::string> filenames;
        EXPECT_OK(storage->get_children(db_prefix, filenames));
        std::vector<Id> result;
        for (const auto &name: filenames) {
            if (name.find("wal-") != std::string::npos) {
                result.push_back(decode_segment_name("wal-", name));
            }
        }
        return result;
    }

    auto num_logs() -> Size
    {
        return get_logs().size();
    }

    auto file_size(const std::string &fname) -> Size
    {
        Size result;
        EXPECT_OK(storage->file_size(fname, result));
        return result;
    }

    Tools::RandomGenerator random {1024 * 1024 * 4};
    Options db_options;
    std::string db_prefix;
    std::string tail;
    Database *db {};
};

class RecoveryTests
    : public RecoveryTestHarness,
      public testing::Test {
};

TEST_F(RecoveryTests, NormalShutdown)
{
    ASSERT_EQ(num_logs(), 1);

    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());
    close();

    ASSERT_EQ(num_logs(), 0);
}

TEST_F(RecoveryTests, OnlyCommittedUpdatesArePersisted)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());

    ASSERT_OK(put("c", "X"));
    ASSERT_OK(put("d", "4"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
    ASSERT_EQ(get("d"), "NOT_FOUND");
}

TEST_F(RecoveryTests, PacksMultipleTransactionsIntoSegment)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());

    ASSERT_EQ(num_logs(), 1);
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
}

TEST_F(RecoveryTests, RevertsNthTransaction)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("c", "3"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "NOT_FOUND");
}

TEST_F(RecoveryTests, SanityCheck)
{
    std::map<std::string, std::string> map;
    const Size N {100};

    for (Size i {}; i < N; ++i) {
        const auto k = random.Generate(db_options.page_size * 2);
        const auto v = random.Generate(db_options.page_size * 4);
        map[k.to_string()] = v.to_string();
    }

    for (Size commit {}; commit < map.size(); ++commit) {
        open();

        auto record = begin(map);
        for (Size index {}; record != end(map); ++index, ++record) {
            if (index == commit) {
                ASSERT_OK(db->commit());
            } else {
                ASSERT_OK(db->put(record->first, record->second));
            }
        }
        open();

        record = begin(map);
        for (Size index {}; record != end(map); ++index, ++record) {
            std::string value;
            if (index < commit) {
                ASSERT_OK(db->get(record->first, value));
                ASSERT_EQ(value, record->second);
            } else {
                ASSERT_TRUE(db->get(record->first, value).is_not_found());
            }
        }
        close();

        ASSERT_OK(Database::destroy(db_prefix, db_options));
    }
}

class RecoverySanityCheck
    : public RecoveryTestHarness,
      public testing::TestWithParam<std::tuple<std::string, Tools::Interceptor::Type, int>> {
public:
    RecoverySanityCheck()
        : interceptor_prefix {db_prefix + std::get<0>(GetParam())}
    {
        open();

        Tools::RandomGenerator random {1'024 * 1'024 * 8};
        const Size N {5'000};

        for (Size i {}; i < N; ++i) {
            const auto k = random.Generate(db_options.page_size * 2);
            const auto v = random.Generate(db_options.page_size * 4);
            map[k.to_string()] = v.to_string();
        }
    }

    ~RecoverySanityCheck() override = default;

    auto SetUp() -> void override
    {
        auto record = begin(map);
        for (Size index {}; record != end(map); ++index, ++record) {
            ASSERT_OK(db->put(record->first, record->second));
            if (record->first.front() % 10 == 1) {
                ASSERT_OK(db->commit());
            }
        }
        ASSERT_OK(db->commit());

        Counting_Interceptor(interceptor_prefix, interceptor_type, interceptor_count);
    }

    auto validate() -> void
    {
        Clear_Interceptors();
        open();

        for (const auto &[k, v]: map) {
            std::string value;
            ASSERT_OK(db->get(k, value));
            ASSERT_EQ(value, v);
        }
    }

    std::string interceptor_prefix;
    Tools::Interceptor::Type interceptor_type {std::get<1>(GetParam())};
    int interceptor_count {std::get<2>(GetParam())};
    std::map<std::string, std::string> map;
};

TEST_P(RecoverySanityCheck, FailureWhileRunning)
{
    for (const auto &[k, v]: map) {
        auto s = db->erase(k);
        if (!s.is_ok()) {
            assert_special_error(s);
            break;
        }
    }
    assert_special_error(db->status());

    validate();
}

// TODO: Find some way to determine if an error occurred during the destructor. It happens in each
//       instance except for when we attempt to fail due to a WAL write error, since the WAL is not
//       written during the close/recovery routine.
TEST_P(RecoverySanityCheck, FailureDuringClose)
{
    // The final transaction committed successfully, so the data we added should persist.
    delete db;
    db = nullptr;

    validate();
}

TEST_P(RecoverySanityCheck, FailureDuringCloseWithUncommittedUpdates)
{
    while (db->status().is_ok()) {
        (void)db->put(random.Generate(16), random.Generate(100));
    }

    delete db;
    db = nullptr;

    validate();
}

INSTANTIATE_TEST_SUITE_P(
    RecoverySanityCheck,
    RecoverySanityCheck,
    ::testing::Values(
        std::make_tuple("data", Tools::Interceptor::READ, 0),
        std::make_tuple("data", Tools::Interceptor::READ, 1),
        std::make_tuple("data", Tools::Interceptor::READ, 5),
        std::make_tuple("data", Tools::Interceptor::WRITE, 0),
        std::make_tuple("data", Tools::Interceptor::WRITE, 1),
        std::make_tuple("data", Tools::Interceptor::WRITE, 5),
        std::make_tuple("wal-", Tools::Interceptor::WRITE, 0),
        std::make_tuple("wal-", Tools::Interceptor::WRITE, 1),
        std::make_tuple("wal-", Tools::Interceptor::WRITE, 5),
//        std::make_tuple("wal-", Tools::Interceptor::SYNC, 0), TODO: May need separate testing
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 0),
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 1),
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 5)));

class OpenErrorTests: public RecoverySanityCheck {
public:
    ~OpenErrorTests() override = default;

    auto SetUp() -> void override
    {
        RecoverySanityCheck::SetUp();
        const auto saved_count = interceptor_count;
        interceptor_count = 0;

        delete db;
        db = nullptr;

        interceptor_count = saved_count;
    }
};

TEST_P(OpenErrorTests, FailureDuringOpen)
{
    assert_special_error(open_with_status());
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    OpenErrorTests,
    OpenErrorTests,
    ::testing::Values(
        std::make_tuple("data", Tools::Interceptor::READ, 0),
        std::make_tuple("data", Tools::Interceptor::READ, 1),
        std::make_tuple("data", Tools::Interceptor::READ, 5),
        std::make_tuple("data", Tools::Interceptor::WRITE, 0),
        std::make_tuple("data", Tools::Interceptor::WRITE, 1),
        std::make_tuple("data", Tools::Interceptor::WRITE, 5),
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 0),
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 1),
        std::make_tuple("wal-", Tools::Interceptor::OPEN, 5)));

} // namespace Calico
