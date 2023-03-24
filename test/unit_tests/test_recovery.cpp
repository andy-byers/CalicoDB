// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Recovery tests (harness is modified from LevelDB).

#include "calicodb/db.h"
#include "tools.h"
#include "unit_tests.h"
#include "wal_reader.h"

namespace calicodb
{

class WalPagerInteractionTests
    : public InMemoryTest,
      public testing::Test
{
public:
    static constexpr auto kFilename = "./test";
    static constexpr auto kWalPrefix = "./wal-";
    static constexpr auto kPageSize = kMinPageSize;
    static constexpr std::size_t kFrameCount = 16;

    WalPagerInteractionTests()
        : scratch(kPageSize, '\x00'),
          log_scratch(wal_scratch_size(kPageSize), '\x00')
    {
    }

    ~WalPagerInteractionTests() override
    {
        delete wal;
        delete pager;
    }

    auto SetUp() -> void override
    {
        tables.add(LogicalPageId::root());

        const WriteAheadLog::Parameters wal_param {
            kWalPrefix,
            env.get(),
            kPageSize};
        ASSERT_OK(WriteAheadLog::open(wal_param, &wal));

        const Pager::Parameters pager_param {
            kFilename,
            env.get(),
            wal,
            nullptr,
            &state,
            kFrameCount,
            kPageSize,
        };
        ASSERT_OK(Pager::open(pager_param, &pager));
        ASSERT_OK(wal->start_writing());
        state.is_running = true;

        tail_buffer.resize(wal_block_size(kPageSize));
        payload_buffer.resize(wal_scratch_size(kPageSize));
    }

    DBState state;
    std::string log_scratch;
    std::string scratch;
    std::string collect_scratch;
    std::string payload_buffer;
    std::string tail_buffer;
    Pager *pager;
    WriteAheadLog *wal;
    TableSet tables;
    tools::RandomGenerator random {1'024 * 1'024 * 8};
};

TEST_F(WalPagerInteractionTests, GeneratesAppropriateWALRecords)
{
    auto lsn_value = wal->current_lsn().value;
    Page page;

    // Image and delta records.
    ASSERT_OK(pager->allocate(page));
    ASSERT_EQ(wal->current_lsn().value, ++lsn_value);
    (void)page.mutate(page.size() - 1, 1);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, ++lsn_value);

    // Page was not "upgraded", so no WAL records should be written.
    ASSERT_OK(pager->acquire(Id::root(), page));
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, lsn_value);

    // Upgrading a page that already has an image should not cause another to be
    // written, but only if there are no deltas.
    ASSERT_OK(pager->acquire(Id::root(), page));
    pager->upgrade(page);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, lsn_value);

    // This page already exists and has an image in the WAL. Only a
    // delta record should be written.
    ASSERT_OK(pager->acquire(Id::root(), page));
    pager->upgrade(page);
    (void)page.mutate(page.size() - 1, 1);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, ++lsn_value);
}

TEST_F(WalPagerInteractionTests, AllocateTruncatedPages)
{
    for (std::size_t i = 0; i < 5; ++i) {
        Page page;
        ASSERT_OK(pager->allocate(page));
        pager->release(std::move(page));
    }

    // The recovery routine handles duplicate images. It will only apply the first one
    // for a given page in a given transaction.
    ASSERT_OK(pager->truncate(1));
    auto current_lsn_value = wal->current_lsn().value;

    Page page;
    ASSERT_OK(pager->allocate(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);
    (void)page.mutate(page.size() - 1, 1);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);

    // If the page isn't updated by the user, a delta is still written due to the
    // page LSN change.
    ASSERT_OK(pager->allocate(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);

    ASSERT_OK(pager->checkpoint());

    // Normal page.
    ASSERT_OK(pager->allocate(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);
    (void)page.mutate(page.size() - 1, 1);
    pager->release(std::move(page));
    ASSERT_EQ(wal->current_lsn().value, ++current_lsn_value);
}

template <class EnvType = tools::FaultInjectionEnv>
class RecoveryTestHarness
{
public:
    static constexpr auto kFilename = "./test";
    static constexpr auto kWalPrefix = "./wal-";

    RecoveryTestHarness()
        : db_prefix {kFilename}
    {
        env = std::make_unique<EnvType>();
        db_options.wal_prefix = kWalPrefix;
        db_options.page_size = kMinPageSize;
        db_options.cache_size = kMinPageSize * 16;
        db_options.env = env.get();

        open();
    }

    virtual ~RecoveryTestHarness()
    {
        delete db;
    }

    virtual auto close() -> void
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
        if (opts.env == nullptr) {
            opts.env = env.get();
        }
        return DB::open(opts, db_prefix, db);
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
        const auto s = db->get(k, &result);
        if (s.is_not_found()) {
            result = "NOT_FOUND";
        } else if (!s.is_ok()) {
            result = s.to_string();
        }
        return result;
    }

    [[nodiscard]] auto get_logs() const -> std::vector<Id>
    {
        std::vector<std::string> filenames;
        EXPECT_OK(env->get_children(".", filenames));
        std::vector<Id> result;
        for (const auto &name : filenames) {
            if (name.find("wal-") == 0) {
                result.push_back(decode_segment_name("wal-", name));
            }
        }
        std::sort(begin(result), end(result));
        return result;
    }

    [[nodiscard]] auto num_logs() const -> std::size_t
    {
        return get_logs().size();
    }

    [[nodiscard]] auto file_size(const std::string &fname) const -> std::size_t
    {
        std::size_t result;
        EXPECT_OK(env->file_size(fname, result));
        return result;
    }

    tools::RandomGenerator random {1024 * 1024 * 4};
    std::unique_ptr<EnvType> env;
    Options db_options;
    std::string db_prefix;
    DB *db {};
};

class RecoveryTests
    : public RecoveryTestHarness<>,
      public testing::Test
{
};

TEST_F(RecoveryTests, NormalShutdown)
{
    ASSERT_EQ(num_logs(), 1);

    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->checkpoint());
    close();

    ASSERT_EQ(num_logs(), 0);
}

TEST_F(RecoveryTests, OnlyCommittedUpdatesArePersisted)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->checkpoint());

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
    ASSERT_OK(db->checkpoint());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->checkpoint());
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->checkpoint());

    ASSERT_EQ(num_logs(), 1);
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
}

TEST_F(RecoveryTests, RevertsNthTransaction)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->checkpoint());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->checkpoint());
    ASSERT_OK(put("c", "3"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "NOT_FOUND");
}

TEST_F(RecoveryTests, VacuumRecovery)
{
    std::vector<Record> committed;
    for (std::size_t i = 0; i < 1'000; ++i) {
        committed.emplace_back(Record {
            random.Generate(100).to_string(),
            random.Generate(100).to_string(),
        });
        ASSERT_OK(db->put(
            committed.back().key,
            committed.back().value));
    }
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }
    ASSERT_OK(db->checkpoint());

    // Grow the database, then make freelist pages.
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }
    // Shrink the database.
    ASSERT_OK(db->vacuum());

    // Grow the database again. This time, it will look like we need to write image records
    // for the new pages, even though they are already in the WAL.
    for (std::size_t i = 0; i < 1'000; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), random.Generate(db_options.page_size)));
    }

    // Now reopen the database and roll the WAL.
    open();

    // If we wrote more than one full image for a given page, we may mess up the database.
    std::string result;
    for (const auto &[key, value] : committed) {
        ASSERT_OK(db->get(key, &result));
        ASSERT_EQ(result, value);
    }
    db_impl(db)->TEST_validate();
}

TEST_F(RecoveryTests, SanityCheck)
{
    std::map<std::string, std::string> map;
    const std::size_t N = 100;

    for (std::size_t i = 0; i < N; ++i) {
        const auto k = random.Generate(db_options.page_size * 2);
        const auto v = random.Generate(db_options.page_size * 4);
        map[k.to_string()] = v.to_string();
    }

    for (std::size_t commit = 0; commit < map.size(); ++commit) {
        open();

        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            if (index == commit) {
                ASSERT_OK(db->checkpoint());
            } else {
                ASSERT_OK(db->put(record->first, record->second));
            }
        }
        open();

        record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            std::string value;
            if (index < commit) {
                ASSERT_OK(db->get(record->first, &value));
                ASSERT_EQ(value, record->second);
            } else {
                ASSERT_TRUE(db->get(record->first, &value).is_not_found());
            }
        }
        close();

        ASSERT_OK(DB::destroy(db_options, db_prefix));
    }
}

class RecoverySanityCheck
    : public RecoveryTestHarness<>,
      public testing::TestWithParam<std::tuple<std::string, tools::Interceptor::Type, int>>
{
public:
    RecoverySanityCheck()
        : interceptor_prefix(std::get<0>(GetParam()))
    {
        open();

        tools::RandomGenerator random(1'024 * 1'024 * 8);
        const std::size_t N = 10'000;

        for (std::size_t i = 0; i < N; ++i) {
            const auto k = random.Generate(db_options.page_size * 2);
            const auto v = random.Generate(db_options.page_size * 4);
            map[k.to_string()] = v.to_string();
        }
    }

    ~RecoverySanityCheck() override = default;

    auto SetUp() -> void override
    {
        auto record = begin(map);
        for (std::size_t index = 0; record != end(map); ++index, ++record) {
            ASSERT_OK(db->put(record->first, record->second));
            if (record->first.front() % 10 == 1) {
                ASSERT_OK(db->checkpoint());
            }
        }
        ASSERT_OK(db->checkpoint());

        COUNTING_INTERCEPTOR(interceptor_prefix, interceptor_type, interceptor_count);
    }

    auto validate() -> void
    {
        CLEAR_INTERCEPTORS();
        open();

        for (const auto &[k, v] : map) {
            std::string value;
            ASSERT_OK(db->get(k, &value));
            ASSERT_EQ(value, v);
        }
    }

    std::string interceptor_prefix;
    tools::Interceptor::Type interceptor_type {std::get<1>(GetParam())};
    int interceptor_count {std::get<2>(GetParam())};
    std::map<std::string, std::string> map;
};

TEST_P(RecoverySanityCheck, FailureWhileRunning)
{
    for (const auto &[k, v] : map) {
        auto s = db->erase(k);
        if (!s.is_ok()) {
            assert_special_error(s);
            break;
        }
    }
    if (db->status().is_ok()) {
        (void)db->vacuum();
    }
    assert_special_error(db->status());

    validate();
}

// TODO: Find some way to determine if an error occurred during the destructor. It happens in each
//       instance except for when we attempt to fail due to a WAL write error, since the WAL is not
//       written during the erase/recovery routine.
TEST_P(RecoverySanityCheck, FailureDuringClose)
{
    // The final transaction committed successfully, so the data we added should persist.
    close();

    validate();
}

TEST_P(RecoverySanityCheck, FailureDuringCloseWithUncommittedUpdates)
{
    while (db->status().is_ok()) {
        (void)db->put(random.Generate(16), random.Generate(100));
    }

    close();
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    RecoverySanityCheck,
    RecoverySanityCheck,
    ::testing::Values(
        std::make_tuple("./test", tools::Interceptor::kRead, 0),
        std::make_tuple("./test", tools::Interceptor::kRead, 1),
        std::make_tuple("./test", tools::Interceptor::kRead, 5),
        std::make_tuple("./test", tools::Interceptor::kWrite, 0),
        std::make_tuple("./test", tools::Interceptor::kWrite, 1),
        std::make_tuple("./test", tools::Interceptor::kWrite, 5),
        std::make_tuple("./wal-", tools::Interceptor::kWrite, 0),
        std::make_tuple("./wal-", tools::Interceptor::kWrite, 1),
        std::make_tuple("./wal-", tools::Interceptor::kWrite, 5),
        std::make_tuple("./wal-", tools::Interceptor::kOpen, 0),
        std::make_tuple("./wal-", tools::Interceptor::kOpen, 1)));

class OpenErrorTests : public RecoverySanityCheck
{
public:
    ~OpenErrorTests() override = default;

    auto SetUp() -> void override
    {
        RecoverySanityCheck::SetUp();
        const auto saved_count = interceptor_count;
        interceptor_count = 0;

        // Should fail on the first syscall given by "std::get<1>(GetParam())".
        close();

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
        std::make_tuple("./test", tools::Interceptor::kRead, 0),
        std::make_tuple("./test", tools::Interceptor::kRead, 1),
        std::make_tuple("./test", tools::Interceptor::kRead, 2),
        std::make_tuple("./test", tools::Interceptor::kWrite, 0),
        std::make_tuple("./test", tools::Interceptor::kWrite, 1),
        std::make_tuple("./wal-", tools::Interceptor::kOpen, 0),
        std::make_tuple("./wal-", tools::Interceptor::kOpen, 1)));

class DataLossEnv : public EnvWrapper
{
    std::string m_database_contents;
    std::size_t m_wal_sync_size {};

public:
    explicit DataLossEnv()
        : EnvWrapper {*new tools::FakeEnv}
    {
    }

    ~DataLossEnv() override
    {
        delete target();
    }

    [[nodiscard]] auto new_editor(const std::string &filename, Editor *&out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &filename, Logger *&out) -> Status override;

    auto register_database_contents(std::string database_contents) -> void
    {
        m_database_contents = std::move(database_contents);
    }

    auto register_wal_sync_size(std::size_t wal_sync_size) -> void
    {
        m_wal_sync_size = wal_sync_size;
    }

    [[nodiscard]] auto database_contents() const -> std::string
    {
        return m_database_contents;
    }

    [[nodiscard]] auto wal_sync_size() const -> std::size_t
    {
        return m_wal_sync_size;
    }
};

class DataLossEditor : public Editor
{
    std::string m_filename;
    DataLossEnv *m_env {};
    Editor *m_file {};

public:
    explicit DataLossEditor(std::string filename, Editor &file, DataLossEnv &env)
        : m_filename {std::move(filename)},
          m_env {&env},
          m_file {&file}
    {
    }

    ~DataLossEditor() override
    {
        delete m_file;
    }

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
    {
        return m_file->read(offset, size, scratch, out);
    }

    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override
    {
        return m_file->write(offset, in);
    }

    [[nodiscard]] auto sync() -> Status override
    {
        CALICODB_TRY(m_file->sync());

        std::size_t file_size;
        EXPECT_OK(m_env->file_size(m_filename, file_size));

        Slice slice;
        std::string contents(file_size, '\0');
        EXPECT_OK(m_file->read(0, file_size, contents.data(), &slice));
        EXPECT_EQ(slice.size(), file_size);

        m_env->register_database_contents(std::move(contents));
        return Status::ok();
    }
};

class DataLossLogger : public Logger
{
    std::string m_filename;
    DataLossEnv *m_env {};
    Logger *m_file {};

public:
    explicit DataLossLogger(std::string filename, Logger &file, DataLossEnv &env)
        : m_filename {std::move(filename)},
          m_env {&env},
          m_file {&file}
    {
    }

    ~DataLossLogger() override
    {
        delete m_file;
    }

    [[nodiscard]] auto write(const Slice &in) -> Status override
    {
        return m_file->write(in);
    }

    [[nodiscard]] auto sync() -> Status override
    {
        CALICODB_TRY(m_file->sync());

        std::size_t file_size;
        EXPECT_OK(m_env->file_size(m_filename, file_size));

        m_env->register_wal_sync_size(file_size);
        return Status::ok();
    }
};

auto DataLossEnv::new_editor(const std::string &filename, Editor *&out) -> Status
{
    EXPECT_OK(target()->new_editor(filename, out));
    out = new DataLossEditor {filename, *out, *this};
    return Status::ok();
}

auto DataLossEnv::new_logger(const std::string &filename, Logger *&out) -> Status
{
    EXPECT_OK(target()->new_logger(filename, out));
    out = new DataLossLogger {filename, *out, *this};
    return Status::ok();
}

class DataLossTests
    : public RecoveryTestHarness<DataLossEnv>,
      public testing::TestWithParam<std::size_t>
{
public:
    const std::size_t kCheckpointInterval = GetParam();

    ~DataLossTests() override = default;

    auto close() -> void override
    {
        // Hack to force an error to occur. The DB won't attempt to recover on close()
        // in this case. It will have to wait until open().
        const_cast<DBState &>(db_impl(db)->TEST_state()).status = special_error();

        RecoveryTestHarness::close();

        drop_unsynced_wal_data();
        set_db_contents();
    }

    auto drop_unsynced_wal_data() const -> void
    {
        // If fsync() failed, we would not have created any more WAL files. Just
        // truncate the last segment file.
        const auto logs = get_logs();
        if (!logs.empty()) {
            const auto segment_name = encode_segment_name(kWalPrefix, logs.back());
            ASSERT_OK(env->resize_file(segment_name, env->wal_sync_size()));
        }
    }

    auto set_db_contents() const -> void
    {
        if (!env->file_exists(kFilename)) {
            return;
        }

        EXPECT_OK(env->resize_file(kFilename, 0));

        Editor *file;
        EXPECT_OK(env->new_editor(kFilename, file));

        EXPECT_OK(file->write(0, env->database_contents()));
        delete file;
    }
};

TEST_P(DataLossTests, LossBeforeFirstCheckpoint)
{
    for (std::size_t i = 0; i < kCheckpointInterval; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), "value"));
    }
    open();

    ASSERT_EQ(db_impl(db)->TEST_state().record_count, 0);
}

TEST_P(DataLossTests, RecoversLastCheckpoint)
{
    for (std::size_t i = 0; i < kCheckpointInterval * 10; ++i) {
        if (i % kCheckpointInterval == 0) {
            ASSERT_OK(db->checkpoint());
        }
        ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
    }
    open();

    for (std::size_t i = 0; i < kCheckpointInterval * 9; ++i) {
        std::string value;
        ASSERT_OK(db->get(tools::integral_key(i), &value));
        ASSERT_EQ(value, tools::integral_key(i));
    }
    ASSERT_EQ(db_impl(db)->TEST_state().record_count, kCheckpointInterval * 9);
}

TEST_P(DataLossTests, LongTransaction)
{
    for (std::size_t i = 0; i < kCheckpointInterval * 10; ++i) {
        ASSERT_OK(db->put(tools::integral_key(i), tools::integral_key(i)));
    }
    ASSERT_OK(db->checkpoint());

    for (std::size_t i = 0; i < kCheckpointInterval * 10; ++i) {
        ASSERT_OK(db->erase(tools::integral_key(i)));
    }
    ASSERT_OK(db->vacuum());

    open();

    for (std::size_t i = 0; i < kCheckpointInterval * 10; ++i) {
        std::string value;
        ASSERT_OK(db->get(tools::integral_key(i), &value));
        ASSERT_EQ(value, tools::integral_key(i));
    }
    ASSERT_EQ(db_impl(db)->TEST_state().record_count, kCheckpointInterval * 10);
}

INSTANTIATE_TEST_SUITE_P(
    DataLossTests,
    DataLossTests,
    ::testing::Values(1, 10, 100, 1'000, 10'000));

} // namespace calicodb
