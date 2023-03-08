/*
 * Recovery tests (harness is modified from LevelDB).
 */
#include "calicodb/calicodb.h"
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
    static constexpr  std::size_t kFrameCount {16};

    WalPagerInteractionTests()
        : scratch(kPageSize, '\x00'),
          log_scratch(wal_scratch_size(kPageSize), '\x00')
    {
    }

    ~WalPagerInteractionTests() override
    {
        delete wal;
    }

    auto SetUp() -> void override
    {
        tables.add(LogicalPageId::root());
        tables.get(Id::root())->checkpoint_lsn.value = static_cast<std::uint64_t>(-1);

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
            &tables,
            &status,
            &is_running,
            kFrameCount,
            kPageSize,
        };
        ASSERT_OK(Pager::open(pager_param, &pager));
        ASSERT_OK(wal->start_writing());

        tail_buffer.resize(wal_block_size(kPageSize));
        payload_buffer.resize(wal_scratch_size(kPageSize));
    }

    auto read_segment(Id segment_id, std::vector<PayloadDescriptor> *out) -> Status
    {
        Reader *temp;
        EXPECT_OK(env->new_reader(encode_segment_name(kWalPrefix, segment_id), &temp));

        std::unique_ptr<Reader> file {temp};
        WalReader reader {*file, tail_buffer};

        for (; ; ) {
            Span payload {payload_buffer};
            auto s = reader.read(payload);

            if (s.is_ok()) {
                out->emplace_back(decode_payload(payload));
            } else if (s.is_not_found()) {
                break;
            } else {
                return s;
            }
        }
        return Status::ok();
    }

    std::string log_scratch;
    Status status;
    bool is_running {true};
    Lsn commit_lsn;
    std::string scratch;
    std::string collect_scratch;
    std::string payload_buffer;
    std::string tail_buffer;
    Pager *pager;
    WriteAheadLog *wal;
    TableSet tables;
    tools::RandomGenerator random {1'024 * 1'024 * 8};
};

TEST_F(WalPagerInteractionTests, PagerWritesTableIDs)
{
    Page page_1 {LogicalPageId {Id {1}, Id {1}}};
    ASSERT_OK(pager->allocate(page_1));
    std::memcpy(page_1.span(0, 13).data(), "Hello, world!", 13);
    pager->release(std::move(page_1));

    Page page_2 {LogicalPageId {Id {1}, Id {2}}};
    ASSERT_OK(pager->allocate(page_2));
    std::memcpy(page_2.span(0, 13).data(), "Hello, world!", 13);
    pager->release(std::move(page_2));

    ASSERT_OK(wal->flush());

    std::vector<PayloadDescriptor> payloads;
    ASSERT_OK(read_segment(Id {1}, &payloads));
    ASSERT_EQ(payloads.size(), 4);

    ASSERT_TRUE(std::holds_alternative<ImageDescriptor>(payloads[0]));
    ASSERT_EQ(std::get<ImageDescriptor>(payloads[0]).table_id, Id {1});
    ASSERT_TRUE(std::holds_alternative<DeltaDescriptor>(payloads[1]));
    ASSERT_EQ(std::get<ImageDescriptor>(payloads[1]).table_id, Id {1});

    ASSERT_TRUE(std::holds_alternative<ImageDescriptor>(payloads[2]));
    ASSERT_EQ(std::get<ImageDescriptor>(payloads[2]).table_id, Id {1});
    ASSERT_TRUE(std::holds_alternative<DeltaDescriptor>(payloads[3]));
    ASSERT_EQ(std::get<ImageDescriptor>(payloads[3]).table_id, Id {1});
}
//
//class RecoveryTestHarness
//{
//public:
//    static constexpr auto kFilename = "./test";
//
//    RecoveryTestHarness()
//        : db_prefix {kFilename}
//    {
//        env = std::make_unique<tools::FaultInjectionEnv>();
//        db_options.wal_prefix = "./wal-";
//        db_options.page_size = kMinPageSize;
//        db_options.cache_size = kMinPageSize * 16;
//        db_options.env = env.get();
//        put();
//    }
//
//    virtual ~RecoveryTestHarness()
//    {
//        erase();
//    }
//
//    auto impl() const -> DBImpl *
//    {
//        return reinterpret_cast<DBImpl *>(db);
//    }
//
//    void erase()
//    {
//        delete db;
//        db = nullptr;
//    }
//
//    auto open_with_status(Options *options = nullptr) -> Status
//    {
//        erase();
//        Options opts = db_options;
//        if (options != nullptr) {
//            opts = *options;
//        }
//        if (opts.env == nullptr) {
//            opts.env = env.get();
//        }
//        tail.resize(wal_block_size(opts.page_size));
//        return DB::put(opts, db_prefix, &db);
//    }
//
//    auto put(Options *options = nullptr) -> void
//    {
//        ASSERT_OK(open_with_status(options));
//    }
//
//    auto put(const std::string &k, const std::string &v) const -> Status
//    {
//        return db->put(k, v);
//    }
//
//    auto get(const std::string &k) const -> std::string
//    {
//        std::string result;
//        Status s = db->get(k, &result);
//        if (s.is_not_found()) {
//            result = "NOT_FOUND";
//        } else if (!s.is_ok()) {
//            result = s.to_string();
//        }
//        return result;
//    }
//
//    auto log_name(Id id) const -> std::string
//    {
//        return encode_segment_name("./wal-", id);
//    }
//
//    auto remove_log_files() -> size_t
//    {
//        // Linux allows unlinking put files, but Windows does not.
//        // Closing the db allows for file deletion.
//        erase();
//        std::vector<Id> logs = get_logs();
//        for (const auto &log : logs) {
//            EXPECT_OK(env->remove_file(encode_segment_name("./wal-", log)));
//        }
//        return logs.size();
//    }
//
//    auto get_logs() -> std::vector<Id>
//    {
//        std::vector<std::string> filenames;
//        EXPECT_OK(env->get_children(".", &filenames));
//        std::vector<Id> result;
//        for (const auto &name : filenames) {
//            if (name.find("wal-") == 0) {
//                result.push_back(decode_segment_name("wal-", name));
//            }
//        }
//        return result;
//    }
//
//    auto num_logs() -> std::size_t
//    {
//        return get_logs().size();
//    }
//
//    auto file_size(const std::string &fname) -> std::size_t
//    {
//        std::size_t result;
//        EXPECT_OK(env->file_size(fname, &result));
//        return result;
//    }
//
//    tools::RandomGenerator random {1024 * 1024 * 4};
//    std::unique_ptr<tools::FaultInjectionEnv> env;
//    Options db_options;
//    std::string db_prefix;
//    std::string tail;
//    DB *db {};
//};
//
//class RecoveryTests
//    : public RecoveryTestHarness,
//      public testing::Test
//{
//};
//
//TEST_F(RecoveryTests, NormalShutdown)
//{
//    ASSERT_EQ(num_logs(), 1);
//
//    ASSERT_OK(put("a", "1"));
//    ASSERT_OK(put("b", "2"));
//    ASSERT_OK(put("c", "3"));
//    ASSERT_OK(db->commit());
//    erase();
//
//    ASSERT_EQ(num_logs(), 0);
//}
//
//TEST_F(RecoveryTests, OnlyCommittedUpdatesArePersisted)
//{
//    ASSERT_OK(put("a", "1"));
//    ASSERT_OK(put("b", "2"));
//    ASSERT_OK(put("c", "3"));
//    ASSERT_OK(db->commit());
//
//    ASSERT_OK(put("c", "X"));
//    ASSERT_OK(put("d", "4"));
//    put();
//
//    ASSERT_EQ(get("a"), "1");
//    ASSERT_EQ(get("b"), "2");
//    ASSERT_EQ(get("c"), "3");
//    ASSERT_EQ(get("d"), "NOT_FOUND");
//}
//
//TEST_F(RecoveryTests, PacksMultipleTransactionsIntoSegment)
//{
//    ASSERT_OK(put("a", "1"));
//    ASSERT_OK(db->commit());
//    ASSERT_OK(put("b", "2"));
//    ASSERT_OK(db->commit());
//    ASSERT_OK(put("c", "3"));
//    ASSERT_OK(db->commit());
//
//    ASSERT_EQ(num_logs(), 1);
//    put();
//
//    ASSERT_EQ(get("a"), "1");
//    ASSERT_EQ(get("b"), "2");
//    ASSERT_EQ(get("c"), "3");
//}
//
//TEST_F(RecoveryTests, RevertsNthTransaction)
//{
//    ASSERT_OK(put("a", "1"));
//    ASSERT_OK(db->commit());
//    ASSERT_OK(put("b", "2"));
//    ASSERT_OK(db->commit());
//    ASSERT_OK(put("c", "3"));
//    put();
//
//    ASSERT_EQ(get("a"), "1");
//    ASSERT_EQ(get("b"), "2");
//    ASSERT_EQ(get("c"), "NOT_FOUND");
//}
//
//TEST_F(RecoveryTests, SanityCheck)
//{
//    std::map<std::string, std::string> map;
//    const std::size_t N {100};
//
//    for (std::size_t i {}; i < N; ++i) {
//        const auto k = random.Generate(db_options.page_size * 2);
//        const auto v = random.Generate(db_options.page_size * 4);
//        map[k.to_string()] = v.to_string();
//    }
//
//    for (std::size_t commit {}; commit < map.size(); ++commit) {
//        put();
//
//        auto record = begin(map);
//        for (std::size_t index {}; record != end(map); ++index, ++record) {
//            if (index == commit) {
//                ASSERT_OK(db->commit());
//            } else {
//                ASSERT_OK(db->put(record->first, record->second));
//            }
//        }
//        put();
//
//        record = begin(map);
//        for (std::size_t index {}; record != end(map); ++index, ++record) {
//            std::string value;
//            if (index < commit) {
//                ASSERT_OK(db->get(record->first, &value));
//                ASSERT_EQ(value, record->second);
//            } else {
//                ASSERT_TRUE(db->get(record->first, &value).is_not_found());
//            }
//        }
//        erase();
//
//        ASSERT_OK(DB::destroy(db_options, db_prefix));
//    }
//}
//
//class RecoverySanityCheck
//    : public RecoveryTestHarness,
//      public testing::TestWithParam<std::tuple<std::string, tools::Interceptor::Type, int>>
//{
//public:
//    RecoverySanityCheck()
//        : interceptor_prefix {std::get<0>(GetParam())}
//    {
//        put();
//
//        tools::RandomGenerator random {1'024 * 1'024 * 8};
//        const std::size_t N {5'000};
//
//        for (std::size_t i {}; i < N; ++i) {
//            const auto k = random.Generate(db_options.page_size * 2);
//            const auto v = random.Generate(db_options.page_size * 4);
//            map[k.to_string()] = v.to_string();
//        }
//    }
//
//    ~RecoverySanityCheck() override = default;
//
//    auto SetUp() -> void override
//    {
//        auto record = begin(map);
//        for (std::size_t index {}; record != end(map); ++index, ++record) {
//            ASSERT_OK(db->put(record->first, record->second));
//            if (record->first.front() % 10 == 1) {
//                ASSERT_OK(db->commit());
//            }
//        }
//        ASSERT_OK(db->commit());
//
//        COUNTING_INTERCEPTOR(interceptor_prefix, interceptor_type, interceptor_count);
//    }
//
//    auto validate() -> void
//    {
//        CLEAR_INTERCEPTORS();
//        put();
//
//        for (const auto &[k, v] : map) {
//            std::string value;
//            ASSERT_OK(db->get(k, &value));
//            ASSERT_EQ(value, v);
//        }
//    }
//
//    std::string interceptor_prefix;
//    tools::Interceptor::Type interceptor_type {std::get<1>(GetParam())};
//    int interceptor_count {std::get<2>(GetParam())};
//    std::map<std::string, std::string> map;
//};
//
//TEST_P(RecoverySanityCheck, FailureWhileRunning)
//{
//    for (const auto &[k, v] : map) {
//        auto s = db->erase(k);
//        if (!s.is_ok()) {
//            assert_special_error(s);
//            break;
//        }
//    }
//    if (db->status().is_ok()) {
//        (void)db->vacuum();
//    }
//    assert_special_error(db->status());
//
//    validate();
//}
//
//// TODO: Find some way to determine if an error occurred during the destructor. It happens in each
////       instance except for when we attempt to fail due to a WAL write error, since the WAL is not
////       written during the erase/recovery routine.
//TEST_P(RecoverySanityCheck, FailureDuringClose)
//{
//    // The final transaction committed successfully, so the data we added should persist.
//    delete db;
//    db = nullptr;
//
//    validate();
//}
//
//TEST_P(RecoverySanityCheck, FailureDuringCloseWithUncommittedUpdates)
//{
//    while (db->status().is_ok()) {
//        (void)db->put(random.Generate(16), random.Generate(100));
//    }
//
//    delete db;
//    db = nullptr;
//
//    validate();
//}
//
//INSTANTIATE_TEST_SUITE_P(
//    RecoverySanityCheck,
//    RecoverySanityCheck,
//    ::testing::Values(
//        std::make_tuple("./test", tools::Interceptor::kRead, 0),
//        std::make_tuple("./test", tools::Interceptor::kRead, 1),
//        std::make_tuple("./test", tools::Interceptor::kRead, 5),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 0),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 1),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 5),
//        std::make_tuple("./wal-", tools::Interceptor::kWrite, 0),
//        std::make_tuple("./wal-", tools::Interceptor::kWrite, 1),
//        std::make_tuple("./wal-", tools::Interceptor::kWrite, 5),
//        std::make_tuple("./wal-", tools::Interceptor::kOpen, 0),
//        std::make_tuple("./wal-", tools::Interceptor::kOpen, 1)));
//
//class OpenErrorTests : public RecoverySanityCheck
//{
//public:
//    ~OpenErrorTests() override = default;
//
//    auto SetUp() -> void override
//    {
//        RecoverySanityCheck::SetUp();
//        const auto saved_count = interceptor_count;
//        interceptor_count = 0;
//
//        delete db;
//        db = nullptr;
//
//        interceptor_count = saved_count;
//    }
//};
//
//TEST_P(OpenErrorTests, FailureDuringOpen)
//{
//    assert_special_error(open_with_status());
//    validate();
//}
//
//INSTANTIATE_TEST_SUITE_P(
//    OpenErrorTests,
//    OpenErrorTests,
//    ::testing::Values(
//        std::make_tuple("./test", tools::Interceptor::kRead, 0),
//        std::make_tuple("./test", tools::Interceptor::kRead, 1),
//        std::make_tuple("./test", tools::Interceptor::kRead, 5),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 0),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 1),
//        std::make_tuple("./test", tools::Interceptor::kWrite, 5),
//        std::make_tuple("./wal-", tools::Interceptor::kOpen, 0),
//        std::make_tuple("./wal-", tools::Interceptor::kOpen, 1),
//        std::make_tuple("./wal-", tools::Interceptor::kOpen, 5)));

} // namespace calicodb
