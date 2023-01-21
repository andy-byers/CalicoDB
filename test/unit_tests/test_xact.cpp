
#include "calico/options.h"
#include "calico/slice.h"
#include "calico/storage.h"
#include "core/core.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "tools.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "utils/header.h"
#include "wal/basic_wal.h"

namespace Calico {

namespace fs = std::filesystem;

namespace UnitTests {
    extern std::uint32_t random_seed;
} // namespace internal

namespace interceptors {
    extern OpenInterceptor open;
    extern ReadInterceptor read;
    extern WriteInterceptor write;
    extern SyncInterceptor sync;
} // namespace interceptors

class PageWrapper {
public:
    static constexpr Size VALUE_SIZE {32};

    explicit PageWrapper(Page page)
        : m_page {std::move(page)}
    {}

    [[nodiscard]]
    auto take() && -> Page
    {
        return std::move(m_page);
    }

    [[nodiscard]]
    auto get_lsn() const -> Id
    {
        return m_page.lsn();
    }

    [[nodiscard]]
    auto get_value() -> Slice
    {
        return m_page.view(m_page.size() - VALUE_SIZE);
    }

    auto set_value(Slice value) -> void
    {
        mem_copy(m_page.span(m_page.size() - VALUE_SIZE), value);
    }

private:
    Page m_page;
};

class XactTestHarness {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size PAGE_COUNT {64};
    static constexpr Size CACHE_SIZE {32};
    static constexpr Size WAL_LIMIT {16};

    auto set_up() -> void
    {
        store = std::make_unique<HeapStorage>();
        ASSERT_OK(store->create_directory("test"));
        scratch = std::make_unique<LogScratchManager>(wal_scratch_size(PAGE_SIZE), 32);

        auto wal_r = BasicWriteAheadLog::open({
            "test/",
            store.get(),
            &state,
            PAGE_SIZE,
            WAL_LIMIT,
            CACHE_SIZE,
        });
        EXPECT_TRUE(wal_r.has_value()) << wal_r.error().what().data();
        wal = std::move(*wal_r);

        auto pager_r = BasicPager::open({
            "test/",
            store.get(),
            scratch.get(),
            wal.get(),
            &state,
            CACHE_SIZE,
            PAGE_SIZE,
        });
        EXPECT_TRUE(pager_r.has_value()) << pager_r.error().what().data();
        pager = std::move(*pager_r);

        while (pager->page_count() < PAGE_COUNT) {
            ASSERT_OK(pager->release(pager->allocate().value()));
        }

        ASSERT_OK(wal->start_workers());
        state.has_xact.store(true);
    }

    auto tear_down() const -> void
    {
        interceptors::reset();

        // Force the WAL to finish writing, so we avoid use-after-free errors.
        wal->flush();
    }

    [[nodiscard]]
    auto get_wrapper(Id id, bool is_writable = false) const -> std::optional<PageWrapper>
    {
        auto page = pager->acquire(id, is_writable);
        if (!page.has_value()) {
            assert_error_42(page.error());
            return {};
        }
        return PageWrapper {std::move(*page)};
    }

    auto commit() -> Status
    {
        CALICO_TRY_S(save_state());

        const auto lsn = wal->current_lsn();
        const auto payload = encode_commit_payload(lsn, *scratch->get());

        wal->log(payload);
        wal->advance();
        allow_cleanup();

        state.commit_lsn.store(lsn);
        return status;
    }


    auto save_state() const -> Status
    {
        auto root = pager->acquire(Id::root(), true);
        if (!root.has_value()) return root.error();

        auto header = read_header(*root);
        pager->save_state(header);
        header.header_crc = compute_header_crc(header);
        write_header(*root, header);

        return pager->release(std::move(*root));
    }

    auto load_state() const -> Status
    {
        auto root = pager->acquire(Id::root(), false);
        if (!root.has_value()) return root.error();

        auto header = read_header(*root);
        EXPECT_EQ(header.header_crc, compute_header_crc(header));
        const auto before_count = pager->page_count();

        pager->load_state(header);

        auto s = pager->release(std::move(*root));
        if (s.is_ok() && pager->page_count() < before_count) {
            const auto after_size = pager->page_count() * pager->page_size();
            return store->resize_file("test/data", after_size);
        }
        return s;
    }

    auto set_value(Id id, const std::string &value) const -> void
    {
        auto wrapper = get_wrapper(id, true);
        ASSERT_TRUE(wrapper.has_value());
        wrapper->set_value(value);
    }

    auto try_set_value(Id id, const std::string &value) const -> bool
    {
        auto wrapper = get_wrapper(id, true);
        if (!wrapper.has_value()) return false;
        wrapper->set_value(value);
        return !state.has_error();
    }

    [[nodiscard]]
    auto get_value(Id id) const -> std::string
    {
        auto wrapper = get_wrapper(id, false);
        EXPECT_TRUE(wrapper.has_value());
        return wrapper.value().get_value().to_string();
    }

    [[nodiscard]]
    auto oldest_lsn() const -> Id
    {
        return std::min(state.commit_lsn.load(), pager->recovery_lsn()); // TODO: Changed this from flushed_lsn(), probably wrong now.
    }

    auto allow_cleanup() const -> void
    {
        wal->cleanup(oldest_lsn());
    }

    [[nodiscard]]
    auto generate_value() -> std::string
    {
        return random.get<std::string>('a', 'z', PageWrapper::VALUE_SIZE);
    }

    Options opt {PAGE_SIZE, PAGE_SIZE * PAGE_COUNT, PAGE_SIZE * PAGE_COUNT, {}, 1024, 32, LogLevel::TRACE, LogTarget::STDERR_COLOR};
    System state {"test", opt};
    Random random {UnitTests::random_seed};
    Status status {ok()};
    std::unique_ptr<HeapStorage> store;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<LogScratchManager> scratch;
};

class NormalXactTests
    : public testing::Test,
      public XactTestHarness
{
public:
    auto SetUp() -> void override
    {
        set_up();
    }

    auto TearDown() -> void override
    {
        tear_down();
    }
};

TEST_F(NormalXactTests, ReadAndWriteValue)
{
    const auto value = generate_value();
    set_value(Id {1}, value);
    ASSERT_EQ(get_value(Id {1}), value);
}

template<class Test>
static auto overwrite_value(Test &test, Id id)
{
    std::string value;
    test.set_value(id, test.generate_value());
    test.set_value(id, value = test.generate_value());
    ASSERT_EQ(test.get_value(id), value);
}

TEST_F(NormalXactTests, OverwriteValue)
{
    overwrite_value(*this, Id {1});
}

TEST_F(NormalXactTests, OverwriteValuesOnMultiplePages)
{
    overwrite_value(*this, Id {1});
    overwrite_value(*this, Id {2});
    overwrite_value(*this, Id {3});
}

template<class Test>
static auto undo_xact(Test &test)
{
    test.wal->advance();
    test.state.has_xact = false;
    Recovery recovery {*test.pager, *test.wal, test.state};
    CALICO_TRY_S(recovery.start_abort());
    // Don't need to load any state for these tests.
    CALICO_TRY_S(recovery.finish_abort());
    test.state.has_xact = true;
    return ok();
}

static auto assert_blank_value(Slice value)
{
    ASSERT_TRUE(value == std::string(PageWrapper::VALUE_SIZE, '\x00'));
}

TEST_F(NormalXactTests, UndoFirstValue)
{
    set_value(Id {1}, generate_value());
    ASSERT_OK(undo_xact(*this));
    assert_blank_value(get_value(Id {1}));
}

TEST_F(NormalXactTests, UndoFirstXact)
{
    set_value(Id {1}, generate_value());
    set_value(Id {2}, generate_value());
    set_value(Id {2}, generate_value());
    ASSERT_OK(undo_xact(*this));
    assert_blank_value(get_value(Id {1}));
    assert_blank_value(get_value(Id {2}));
}

template<class Test>
static auto add_values(Test &test, Size n, bool allow_failure = false) -> std::vector<std::string>
{
    std::vector<std::string> values(n);
    std::generate(begin(values), end(values), [&test] {
        return test.generate_value();
    });

    Size index {};
    for (const auto &value: values) {
        if (allow_failure) {
            if (!test.try_set_value(Id::from_index(index), value))
                return {};
        } else {
            test.set_value(Id::from_index(index), value);
        }
        test.allow_cleanup();
        index = (index+1) % Test::PAGE_COUNT;
    }
    return values;
}

template<class Test>
static auto assert_values_match(Test &test, const std::vector<std::string> &values)
{
    Size index {};
    for (const auto &value: values) {
        const auto id = Id::from_index(index);
        if (test.get_value(id) != value) {
            ADD_FAILURE() << "error: mismatch on page " << id.value << " (" << Test::PAGE_COUNT << " pages total)";
            std::exit(EXIT_FAILURE);
        }
        index = (index+1) % Test::PAGE_COUNT;
    }
}

TEST_F(NormalXactTests, EmptyCommit)
{
    commit();
}

TEST_F(NormalXactTests, EmptyAbort)
{
    ASSERT_OK(undo_xact(*this));
}

TEST_F(NormalXactTests, AbortEmptyTransaction)
{
    const auto committed = add_values(*this, 3);
    commit();

    ASSERT_OK(undo_xact(*this));
    assert_values_match(*this, committed);
}

TEST_F(NormalXactTests, UndoSecondTransaction)
{
    const auto committed = add_values(*this, 3);
    commit();
    add_values(*this, 3);

    ASSERT_OK(undo_xact(*this));
    assert_values_match(*this, committed);
}

TEST_F(NormalXactTests, SpamCommit)
{
    std::vector<std::string> committed;
    for (Size i {}; i < 50; ++i) {
        committed = add_values(*this, PAGE_COUNT);
        commit();
    }
    add_values(*this, PAGE_COUNT);
    ASSERT_OK(undo_xact(*this));
    assert_values_match(*this, committed);
}

TEST_F(NormalXactTests, SpamAbort)
{
    const auto committed = add_values(*this, PAGE_COUNT);
    commit();

    for (Size i {}; i < 50; ++i) {
        add_values(*this, PAGE_COUNT);
        ASSERT_OK(undo_xact(*this));
        assert_values_match(*this, committed);
    }
}

TEST_F(NormalXactTests, AbortAfterMultipleOverwrites)
{
    const auto committed = add_values(*this, PAGE_COUNT);
    commit();

    add_values(*this, PAGE_COUNT);
    add_values(*this, PAGE_COUNT);
    add_values(*this, PAGE_COUNT);

    ASSERT_OK(undo_xact(*this));
    assert_values_match(*this, committed);
}

class RollForwardTests: public NormalXactTests {
public:
    auto get_lsn_range() -> std::pair<Id, Id>
    {
        std::vector<Id> lsns;
        EXPECT_OK(wal->roll_forward(Id::root(), [&lsns](WalPayloadOut payload) {
            lsns.emplace_back(payload.lsn());
            return ok();
        }));
        EXPECT_FALSE(lsns.empty());
        return {lsns.front(), lsns.back()};
    }
};

TEST_F(RollForwardTests, ObsoleteSegmentsAreRemoved)
{
    add_values(*this, PAGE_COUNT);
    commit();
    ASSERT_OK(pager->flush({}));
    allow_cleanup();
    wal->flush();

    const auto [first, last] = get_lsn_range();
    ASSERT_GT(first.value, 1);
    ASSERT_LE(first, pager->recovery_lsn());
    ASSERT_EQ(last, state.commit_lsn.load());
}

TEST_F(RollForwardTests, KeepsNeededSegments)
{
    for (Size i {}; i < 100; ++i) {
        add_values(*this, PAGE_COUNT);
        commit();
        allow_cleanup();
    }

    const auto [first, last] = get_lsn_range();
    ASSERT_LE(first, pager->recovery_lsn());
    ASSERT_EQ(last, state.commit_lsn.load());
}

TEST_F(RollForwardTests, SanityCheck)
{
    const auto committed = add_values(*this, PAGE_COUNT);
    commit();

    // We should keep all WAL segments generated in this loop, since we are not committing. We need to
    // be able to undo any of these changes.
    for (Size i {}; i < 100; ++i) {
        add_values(*this, PAGE_COUNT);
        allow_cleanup();
    }

    const auto [first, last] = get_lsn_range();
    ASSERT_LE(first, state.commit_lsn.load());
    ASSERT_EQ(Id {last.value + 1}, wal->current_lsn());

    ASSERT_OK(undo_xact(*this));
    assert_values_match(*this, committed);
}

class FailedXactTests
    : public testing::TestWithParam<Size>,
      public XactTestHarness
{
public:
    auto SetUp() -> void override
    {
        set_up();

        for (Size i {}; i < GetParam(); ++i) {
            committed = add_values(*this, PAGE_COUNT);
            commit();
        }
    }

    auto TearDown() -> void override
    {
        tear_down();
    }

    auto modify_until_failure()
    {
        for (; ; ) {
            if (add_values(*this, PAGE_COUNT, true).empty())
                break;
        }
    }

    [[nodiscard]]
    auto get_status()
    {
        return state.has_error() ? state.original_error().status : ok();
    }

    std::vector<std::string> committed;
};

TEST_P(FailedXactTests, DataWriteFailureIsPropagated)
{
    interceptors::set_write(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/data",
        {1, 1, 1, 0, 1},
    });
    modify_until_failure();
    assert_error_42(get_status());
}

TEST_P(FailedXactTests, WalWriteFailureIsPropagated)
{
    interceptors::set_write(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/wal",
        {1, 1, 1, 0, 1},
    });
    modify_until_failure();
    assert_error_42(get_status());
}

TEST_P(FailedXactTests, WalOpenFailureIsPropagated)
{
    interceptors::set_open(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/wal",
        {1, 1, 1, 0, 1},
    });
    modify_until_failure();
    assert_error_42(get_status());
}

INSTANTIATE_TEST_SUITE_P(
    WalOpenFailureIsPropagated,
    FailedXactTests,
    ::testing::Values(0, 1, 10, 50));

class TransactionTests : public TestOnDisk {
public:
    auto SetUp() -> void override
    {
        options.page_size = 0x400;
        options.page_cache_size = 64 * options.page_size;
        options.wal_buffer_size = 64 * options.page_size;
        options.log_level = LogLevel::OFF;
        options.storage = store.get();

        ASSERT_OK(db.open(ROOT, options));
    }

    auto TearDown() -> void override
    {
        interceptors::reset();
        ASSERT_OK(db.close());
    }

    [[nodiscard]]
    auto get_db() -> Core&
    {
        return db;
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {UnitTests::random_seed};
    Options options;
    Core db;
};

TEST_F(TransactionTests, NewDatabaseIsOk)
{
    ASSERT_OK(db.status());
}

template<class Action>
static auto with_xact(TransactionTests &test, const Action &action)
{
    auto xact = test.db.transaction();
    action();
    ASSERT_OK(xact.commit());
}

template<class Test>
static auto insert_records(Test &test, Size n = 1'000)
{
    auto records = test.generator.generate(test.random, n);
    for (const auto &r: records) {
        EXPECT_TRUE(expose_message(test.db.insert(r.key, r.value)));
    }
    return records;
}

auto erase_records(TransactionTests &test, Size n = 1'000)
{
    for (Size i {}; i < n; ++i) {
        ASSERT_OK(test.db.erase(test.db.first()));
    }
}

template<class Test>
static auto test_abort_first_xact(Test &test, Size num_records)
{
    auto xact = test.db.transaction();
    insert_records(test, num_records);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(test.db.statistics().record_count(), 0);

    // Normal operations after abort should work.
    with_xact(test, [&test] {insert_records(test);});
}

TEST_F(TransactionTests, CannotUseTransactionObjectAfterSuccessfulCommit)
{
    auto xact = db.transaction();
    insert_records(*this, 10);
    ASSERT_OK(xact.commit());
    ASSERT_TRUE(xact.abort().is_logic_error());
    ASSERT_TRUE(xact.commit().is_logic_error());
}

TEST_F(TransactionTests, CannotUseTransactionObjectAfterSuccessfulAbort)
{
    auto xact = db.transaction();
    insert_records(*this, 10);
    ASSERT_OK(xact.abort());
    ASSERT_TRUE(xact.abort().is_logic_error());
    ASSERT_TRUE(xact.commit().is_logic_error());
}

TEST_F(TransactionTests, AbortFirstXactWithSingleRecord)
{
    test_abort_first_xact(*this, 1);
}

TEST_F(TransactionTests, AbortFirstXactWithMultipleRecords)
{
    test_abort_first_xact(*this, 8);
}

TEST_F(TransactionTests, CommitIsACheckpoint)
{
    with_xact(*this, [this] {insert_records(*this);});

    auto xact = db.transaction();
    ASSERT_OK(xact.abort());
    ASSERT_EQ(db.statistics().record_count(), 1'000);
}

TEST_F(TransactionTests, KeepsCommittedRecords)
{
    with_xact(*this, [this] {insert_records(*this);});

    auto xact = db.transaction();
    erase_records(*this);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(db.statistics().record_count(), 1'000);

    // Normal operations after abort should work.
    with_xact(*this, [this] {erase_records(*this);});
    ASSERT_EQ(db.statistics().record_count(), 0);
}

template<class Test, class Itr>
static auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
{
    auto &db = test.get_db();

    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(db.insert(itr->key, itr->value)));
    }

    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.get(5) == 0) {
            EXPECT_TRUE(expose_message(db.erase(itr->key)));
        } else {
            committed.emplace_back(*itr);
        }
    }
    return committed;
}

template<class Test>
static auto test_abort_second_xact(Test &test, Size first_xact_size, Size second_xact_size)
{
    const auto path = Test::ROOT + std::string {DATA_FILENAME};
    const auto records = test.generator.generate(test.random, first_xact_size + second_xact_size);

    auto xact1 = test.db.transaction();
    auto committed = run_random_operations(test, cbegin(records), cbegin(records) + static_cast<long>(first_xact_size));
    ASSERT_OK(xact1.commit());

    auto xact2 = test.db.transaction();
    run_random_operations(test, cbegin(records) + static_cast<long>(first_xact_size), cend(records));
    ASSERT_OK(xact2.abort());

    // The database should contain exactly these records.
    ASSERT_EQ(test.db.statistics().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(test.db, key, value));
    }
}

TEST_F(TransactionTests, AbortSecondXact_1_1)
{
    test_abort_second_xact(*this, 1, 1);
}

TEST_F(TransactionTests, AbortSecondXact_1000_1)
{
    test_abort_second_xact(*this, 1'000, 1);
}

TEST_F(TransactionTests, AbortSecondXact_1_1000)
{
    test_abort_second_xact(*this, 1, 1'000);
}

TEST_F(TransactionTests, AbortSecondXact_1000_1000)
{
    test_abort_second_xact(*this, 1'000, 1'000);
}

template<class Test>
[[nodiscard]]
auto run_random_transactions(Test &test, Size n)
{
    static constexpr long XACT_SIZE {100};
    // Generate the records all at once, so we know that they are unique.
    auto all_records = test.generator.generate(test.random, n * XACT_SIZE);
    std::vector<Record> committed;
    auto &db = test.get_db();

    for (Size i {}; i < n; ++i) {
        auto xact = db.transaction();
        const auto start = cbegin(all_records) + static_cast<long>(XACT_SIZE * i);
        const auto temp = run_random_operations(test, start, start + XACT_SIZE);
        if (test.random.get(4) == 0) {
            EXPECT_TRUE(expose_message(xact.abort()));
        } else {
            EXPECT_TRUE(expose_message(xact.commit()));
            committed.insert(cend(committed), cbegin(temp), cend(temp));
        }
    }
    return committed;
}

TEST_F(TransactionTests, SanityCheck)
{
    for (const auto &[key, value]: run_random_transactions(*this, 20)) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(TransactionTests, AbortSanityCheck)
{
    static constexpr long NUM_RECORDS {5'000};
    auto records = generator.generate(random, NUM_RECORDS);
    const auto committed = run_random_transactions(*this, 10);

    for (long i {}, j {}; i + j < NUM_RECORDS; j += 10, i += j) {
        auto xact = db.transaction();
        const auto start = cbegin(records) + i;
        const auto temp = run_random_operations(*this, start, start + j);
        ASSERT_OK(xact.abort());
    }
    ASSERT_EQ(db.statistics().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(TransactionTests, PersistenceSanityCheck)
{
    ASSERT_OK(db.close());
    std::vector<Record> committed;

    for (Size i {}; i < 5; ++i) {
        ASSERT_OK(db.open(ROOT, options));
        const auto current = run_random_transactions(*this, 10);
        committed.insert(cend(committed), cbegin(current), cend(current));
        ASSERT_OK(db.close());
    }

    ASSERT_OK(db.open(ROOT, options));
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(TransactionTests, AtomicOperationSanityCheck)
{
    const auto all_records = generator.generate(random, 500);
    const auto committed = run_random_operations(*this, cbegin(all_records), cend(all_records));

    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

class FailureTests: public TestOnHeap {
public:
    FailureTests() = default;

    ~FailureTests() override
    {
        interceptors::reset();
    }

    auto SetUp() -> void override
    {
        Options options;
        options.page_size = 0x200;
        options.page_cache_size = 64 * options.page_size;
        options.wal_buffer_size = 64 * options.page_size;
        options.storage = store.get();
        options.log_level = LogLevel::OFF;
        ASSERT_OK(db.open(ROOT, options));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {UnitTests::random_seed};
    Database db;
};

auto add_sequential_records(Database &db, Size n)
{
    for (Size i {}; i < n; ++i) {
        const auto key = make_key(i);
        ASSERT_OK(db.insert(key, key));
    }
}

auto modify_until_failure(FailureTests &test, Size limit = 10'000) -> Status
{
    RecordGenerator::Parameters param;
    param.mean_key_size = 16;
    param.mean_value_size = 100;
    param.is_unique = true;
    param.spread = 0;
    RecordGenerator generator {param};

    const auto info = test.db.statistics();
    auto s = ok();

    for (Size i {}; i < limit; ++i) {
        for (const auto &[key, value]: generator.generate(test.random, 100)) {

            // insert()/erase() exercise data file reading/writing, and WAL file writing.
            if (test.random.get(4) == 0 && info.record_count()) {
                s = test.db.erase(test.db.first());
            } else {
                s = test.db.insert(key, value);
            }
            if (!s.is_ok()) return s;
        }
    }
    return ok();
}

template<class Test>
static auto run_propagate_test(Test &test)
{
    // Modify the database until a system call fails.
    auto xact = test.db.transaction();
    const auto s = modify_until_failure(test);
    assert_error_42(s);

    // The database status should reflect the error returned by write().
    assert_error_42(test.db.status());
    (void)xact.abort();
}

TEST_F(FailureTests, DataReadErrorIsPropagatedDuringModify)
{
    interceptors::set_read(FailOnce<5> {"test/data"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, DataWriteErrorIsPropagatedDuringModify)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, WalWriteErrorIsPropagatedDuringModify)
{
    interceptors::set_write(FailOnce<5> {"test/wal-"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, WalOpenErrorIsPropagatedDuringModify)
{
    interceptors::set_open(FailOnce<1> {"test/wal-"});
    run_propagate_test(*this);
}

TEST_F(FailureTests, WalReadErrorIsPropagatedDuringAbort)
{
    auto xact = db.transaction();
    insert_records(*this);

    interceptors::set_read(FailOnce<0> {"test/wal-"});

    assert_error_42(xact.abort());
    assert_error_42(db.status());
}

TEST_F(FailureTests, DataReadErrorIsNotPropagatedDuringQuery)
{
    add_sequential_records(db, 500);

    interceptors::set_read(FailOnce<5> {"test/data"});

    // Iterate until a read() call fails.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // The error in the cursor should reflect the read() error.
    assert_error_42(c.status());

    // The database status should still be OK. Errors during reads cannot corrupt or even modify the database state.
    ASSERT_OK(db.status());
}

TEST_F(FailureTests, DataWriteFailureDuringQuery)
{
    auto xact = db.transaction();

    // This tests database behavior when we encounter an error while flushing a dirty page to make room for a page read
    // during a query. In this case, we don't have a transaction we can try to abort, so we must exit the program. Next
    // time the database is opened, it will roll forward and apply any missing updates.
    add_sequential_records(db, 5'000);

//    ASSERT_OK(xact.commit());

    interceptors::set_write(FailOnce<0> {"test/data"});

    auto c = db.first();
    for (; c.is_valid(); ++c);

    assert_error_42(db.status());
}

TEST_F(FailureTests, CannotPerformOperationsAfterFatalError)
{
    interceptors::set_write(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/data",
        {1, 1, 1, 0, 1},
    });

    modify_until_failure(*this);
    assert_error_42(db.status());
    assert_error_42(db.first().status());
    assert_error_42(db.last().status());
    assert_error_42(db.find("key").status());
    assert_error_42(db.insert("key", "value"));
    assert_error_42(db.erase("key"));

    // If db.status() is not OK, creating a transaction object is not allowed. db.close() should
    // return the fatal error.
    assert_error_42(db.close());
}

class RecoveryTestHarness {
public:
    RecoveryTestHarness()
        : store {std::make_unique<HeapStorage>()},
          db {std::make_unique<Core>()}
    {}

    virtual auto setup(Size xact_count, Size uncommitted_count) -> void
    {
        options.storage = store.get();
        options.page_size = 0x200;
        options.page_cache_size = 64 * options.page_size;
        options.wal_buffer_size = 64 * options.page_size;
        options.log_level = LogLevel::OFF;

        ASSERT_OK(db->open("test", options));
        committed = run_random_transactions(*this, xact_count);
        const auto database_state = tools::read_file(*store, "test/data");

        interceptors::set_write(SystemCallOutcomes<RepeatFinalOutcome> {
            "test/data",
            {1, 0},
        });
        auto xact = db->transaction();
        uncommitted = generator.generate(random, uncommitted_count);
        for (const auto &[key, value]: uncommitted) {
            auto s = db->insert(key, value);
            if (!s.is_ok()) break;
        }
        // If the database encountered an error, these calls won't do anything.
        (void)xact.abort();
        (void)db->close();

        // Clone the database while there are still pages waiting to be written to the data file. We'll have
        // to use the WAL to recover.
        auto cloned = store->clone();
        tools::write_file(*cloned, "test/data", database_state);

        store.reset(dynamic_cast<HeapStorage*>(cloned));
        options.storage = store.get();
        db.reset();

        db = std::make_unique<Core>();
        interceptors::set_write([](auto, auto, auto) {return ok();});
    }

    virtual auto validate() -> void
    {
        for (const auto &[key, value]: committed)
            tools::expect_contains(*db, key, value);

        for (const auto &[key, value]: uncommitted) {
            ASSERT_FALSE(tools::contains(*db, key, value));
        }
        db->tree->TEST_validate_links();
        db->tree->TEST_validate_nodes();
        db->tree->TEST_validate_order();
    }

    [[nodiscard]]
    virtual auto get_db() -> Core&
    {
        return *db;
    }

    Random random {42};
    RecordGenerator generator {{16, 100, 10, false, true}};
    std::vector<Record> committed, uncommitted;
    std::unique_ptr<HeapStorage> store;
    Options options;
    std::unique_ptr<Core> db;
};

class RecoveryTests
    : public testing::TestWithParam<std::pair<Size, Size>>,
      public RecoveryTestHarness
{
public:
    auto SetUp() -> void override
    {
        const auto [xact_count, uncommitted_count] = GetParam();
        setup(xact_count, uncommitted_count);
    }
};

TEST_P(RecoveryTests, Recovers)
{
    ASSERT_OK(db->open("test", options));
    validate();
}

INSTANTIATE_TEST_SUITE_P(
    Recovers,
    RecoveryTests,
    ::testing::Values(
//        std::make_pair(  0, 100), // TODO: Sometimes, this test doesn't fail when it should.
        std::make_pair(  1, 100),
        std::make_pair( 10, 100)));

class RecoveryFailureTestRunner {
public:
    explicit RecoveryFailureTestRunner(std::string filter_prefix)
        : prefix {std::move(filter_prefix)}
    {}

    template<class Test>
    auto run(Test &test) -> void
    {
        Size num_tries {};
        for (; ; num_tries++) {
            if (auto s = test.db->open("test", test.options); s.is_ok()) {
                break;
            } else {
                assert_error_42(s);
            }
            test.db.reset();
            test.db = std::make_unique<Core>();
        }
        test.validate();
        ASSERT_GT(num_tries, 0) << "recovery should have failed at least once";
    }

    auto should_syscall_succeed(const std::string &path, ...) -> Status
    {
        if (Slice {path}.starts_with(prefix) && counter++ >= target) {
            target += step;
            counter = 0;
            return system_error("42");
        }
        return Status::ok();
    }

    std::string prefix;
    Size counter {};
    Size target {1};
    Size step {1};
};

class RecoveryDataWriteFailureTests: public RecoveryTests {

};

TEST_P(RecoveryDataWriteFailureTests, ErrorIsPropagated)
{
    interceptors::set_write(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/data",
        {1, 0},
    });
    assert_error_42(db->open("test", options));
}

TEST_P(RecoveryDataWriteFailureTests, RecoveryIsReentrant)
{
    RecoveryFailureTestRunner runner {"test/data"};
    interceptors::set_write([&runner](const auto &path, ...) {
        return runner.should_syscall_succeed(path);
    });
    runner.run(*this);
}

INSTANTIATE_TEST_SUITE_P(
    Recovers,
    RecoveryDataWriteFailureTests,
    ::testing::Values(
//        std::make_pair(  0, 100),
        std::make_pair(  1, 100),
        std::make_pair( 10, 100)));

class RecoveryWalReadFailureTests: public RecoveryTests {

};

TEST_P(RecoveryWalReadFailureTests, ErrorIsPropagated)
{
    interceptors::set_read(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/wal",
        {1, 1, 1, 0, 1},
    });
    assert_error_42(db->open("test", options));
}

TEST_P(RecoveryWalReadFailureTests, RecoveryIsReentrant)
{
    RecoveryFailureTestRunner runner {"test/wal"};
    interceptors::set_read([&runner](const auto &path, ...) {
        return runner.should_syscall_succeed(path);
    });
    runner.run(*this);
}

INSTANTIATE_TEST_SUITE_P(
    Recovers,
    RecoveryWalReadFailureTests,
    ::testing::Values(
//        std::make_pair(  0, 100),
        std::make_pair(  1, 100),
        std::make_pair( 10, 100)));

class RecoveryWalOpenFailureTests: public RecoveryTests {

};

TEST_P(RecoveryWalOpenFailureTests, ErrorIsPropagated)
{
    interceptors::set_open(SystemCallOutcomes<RepeatFinalOutcome> {
        "test/wal",
        {1, 0, 1},
    });
    assert_error_42(db->open("test", options));
}

TEST_P(RecoveryWalOpenFailureTests, RecoveryIsReentrant)
{
    RecoveryFailureTestRunner runner {"test/wal"};
    interceptors::set_open([&runner](const auto &path, ...) {
        return runner.should_syscall_succeed(path);
    });
    runner.run(*this);
}

INSTANTIATE_TEST_SUITE_P(
    Recovers,
    RecoveryWalOpenFailureTests,
    ::testing::Values(
//        std::make_pair(  0, 100),
        std::make_pair(  1, 100),
        std::make_pair( 10, 100)));

} // namespace Calico

