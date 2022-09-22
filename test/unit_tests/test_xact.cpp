
#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/storage.h"
#include "core/core.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "tools.h"
#include "tree/tree.h"
#include "unit_tests.h"
#include "wal/basic_wal.h"

namespace calico {

namespace fs = std::filesystem;

namespace internal {
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
    auto get_lsn() const -> SequenceId
    {
        return m_page.lsn();
    }

    [[nodiscard]]
    auto get_value() -> BytesView
    {
        return m_page.view(m_page.size() - VALUE_SIZE);
    }

    auto set_value(BytesView value) -> void
    {
        mem_copy(m_page.bytes(m_page.size() - VALUE_SIZE), value);
    }

private:
    Page m_page;
};


class XactTestHarness {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size PAGE_COUNT {64};
    static constexpr Size FRAME_COUNT {32};
    static constexpr Size WAL_LIMIT {16};

    auto set_up() -> void
    {
        store = std::make_unique<HeapStorage>();
        ASSERT_OK(store->create_directory("test"));
        scratch = std::make_unique<LogScratchManager>(wal_scratch_size(PAGE_SIZE));

        WriteAheadLog *temp {};
        ASSERT_OK(BasicWriteAheadLog::open({
            "test/",
            store.get(),
            scratch.get(),
            create_sink(),
            PAGE_SIZE,
            WAL_LIMIT,
        }, &temp));
        wal.reset(temp);

        auto r = BasicPager::open({
            "test/",
            *store,
            scratch.get(),
            &images,
            *wal,
            status,
            has_xact,
            create_sink(),
            FRAME_COUNT,
            PAGE_SIZE,
        });
        ASSERT_TRUE(r.has_value());
        pager = std::move(*r);

        while (pager->page_count() < PAGE_COUNT) {
            ASSERT_OK(pager->release(pager->allocate().value()));
        }

        ASSERT_OK(wal->start_workers());
    }

    auto tear_down() -> void
    {
        if (wal->is_working())
            (void)wal->stop_workers();
        interceptors::reset();
    }

    [[nodiscard]]
    auto get_wrapper(PageId id, bool is_writable = false) -> std::optional<PageWrapper>
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
        commit_lsn.value = wal->current_lsn().value - 1;
        CALICO_TRY(wal->advance());
        images.clear();
        return status;
    }

    auto set_value(PageId id, const std::string &value) -> void
    {
        auto wrapper = get_wrapper(id, true);
        ASSERT_TRUE(wrapper.has_value());
        wrapper->set_value(value);
    }

    auto try_set_value(PageId id, const std::string &value) -> bool
    {
        auto wrapper = get_wrapper(id, true);
        if (!wrapper.has_value()) return false;
        wrapper->set_value(value);
        return true;
    }

    [[nodiscard]]
    auto get_value(PageId id) -> std::string
    {
        auto wrapper = get_wrapper(id, true);
        EXPECT_TRUE(wrapper.has_value());
        return wrapper.value().get_value().to_string();
    }

    [[nodiscard]]
    auto try_get_value(PageId id) -> std::string
    {
        auto wrapper = get_wrapper(id, true);
        if (!wrapper.has_value()) return {};
        return wrapper->get_value().to_string();
    }

    [[nodiscard]]
    auto generate_value() -> std::string
    {
        return random.get<std::string>('a', 'z', PageWrapper::VALUE_SIZE);
    }

    Random random {internal::random_seed};
    Status status {Status::ok()};
    SequenceId commit_lsn;
    FileHeader state {};
    bool has_xact {};
    std::unique_ptr<HeapStorage> store;
    std::unique_ptr<Pager> pager;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<LogScratchManager> scratch;
    std::unordered_set<PageId, PageId::Hash> images;
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
    set_value(PageId {1}, value);
    ASSERT_EQ(get_value(PageId {1}), value);
}

template<class Test>
static auto overwrite_value(Test &test, PageId id)
{
    std::string value;
    test.set_value(id, test.generate_value());
    test.set_value(id, value = test.generate_value());
    ASSERT_EQ(test.get_value(id), value);
}

TEST_F(NormalXactTests, OverwriteValue)
{
    overwrite_value(*this, PageId {1});
}

TEST_F(NormalXactTests, OverwriteValuesOnMultiplePages)
{
    overwrite_value(*this, PageId {1});
    overwrite_value(*this, PageId {2});
    overwrite_value(*this, PageId {3});
}

template<class Test>
static auto undo_xact(Test &test, SequenceId commit_id)
{
    Recovery recovery {*test.pager, *test.wal};
    (void)test.wal->stop_workers();
    CALICO_TRY(recovery.start_abort(commit_id));
    // Don't need to load any state for these tests.
    return recovery.finish_abort(commit_id);
}

static auto assert_blank_value(BytesView value)
{
    ASSERT_TRUE(value == std::string(PageWrapper::VALUE_SIZE, '\x00'));
}

TEST_F(NormalXactTests, UndoFirstValue)
{
    set_value(PageId {1}, generate_value());
    ASSERT_OK(undo_xact(*this, SequenceId::null()));
    assert_blank_value(get_value(PageId {1}));
}

TEST_F(NormalXactTests, UndoFirstXact)
{
    set_value(PageId {1}, generate_value());
    set_value(PageId {2}, generate_value());
    set_value(PageId {2}, generate_value());
    ASSERT_OK(undo_xact(*this, SequenceId::null()));
    assert_blank_value(get_value(PageId {1}));
    assert_blank_value(get_value(PageId {2}));
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
            if (!test.try_set_value(PageId::from_index(index), value))
                return {};
        } else {
            test.set_value(PageId::from_index(index), value);
        }
        index = ++index % Test::PAGE_COUNT;
    }
    return values;
}

template<class Test>
static auto assert_values_match(Test &test, const std::vector<std::string> &values)
{
    Size index {};
    for (const auto &value: values) {
        ASSERT_EQ(test.get_value(PageId::from_index(index)), value);
        index = ++index % Test::PAGE_COUNT;
    }
}

TEST_F(NormalXactTests, EmptyCommit)
{
    commit();
}

TEST_F(NormalXactTests, EmptyAbort)
{
    ASSERT_OK(undo_xact(*this, SequenceId::null()));
}

TEST_F(NormalXactTests, EmptyAbortAfterCommit)
{
    const auto committed = add_values(*this, 3);
    commit();

    ASSERT_OK(undo_xact(*this, commit_lsn));
    assert_values_match(*this, committed);
}

TEST_F(NormalXactTests, UndoSecondXact)
{
    const auto committed = add_values(*this, 3);
    commit();
    add_values(*this, 3);

    ASSERT_OK(undo_xact(*this, commit_lsn));
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
    ASSERT_OK(undo_xact(*this, commit_lsn));
    assert_values_match(*this, committed);
}

TEST_F(NormalXactTests, SpamAbort)
{
    const auto committed = add_values(*this, PAGE_COUNT);
    commit();

    for (Size i {}; i < 50; ++i) {
        add_values(*this, PAGE_COUNT);
        ASSERT_OK(undo_xact(*this, commit_lsn));
    }
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
        return pager->status().is_ok() ? wal->worker_status() : pager->status();
    }

    std::vector<std::string> committed;
};

TEST_P(FailedXactTests, DataWriteFailureIsPropagated)
{
    interceptors::set_write(FailOnce<10> {"test/data"});
    modify_until_failure();
    assert_error_42(pager->status());
}

INSTANTIATE_TEST_SUITE_P(
    DataWriteFailureIsPropagated,
    FailedXactTests,
    ::testing::Values(0, 1, 10, 50));

TEST_P(FailedXactTests, WalWriteFailureIsPropagated)
{
    interceptors::set_write(FailOnce<10> {"test/wal-"});
    modify_until_failure();
    assert_error_42(pager->status());
}

INSTANTIATE_TEST_SUITE_P(
    WalWriteFailureIsPropagated,
    FailedXactTests,
    ::testing::Values(0, 1, 10, 50));

TEST_P(FailedXactTests, WalOpenFailureIsPropagated)
{
    interceptors::set_open(FailOnce<3> {"test/wal-"});
    modify_until_failure();
    assert_error_42(pager->status());
}

INSTANTIATE_TEST_SUITE_P(
    WalOpenFailureIsPropagated,
    FailedXactTests,
    ::testing::Values(0, 1, 10, 50));

class XactTests_ : public TestOnDisk {
public:
    auto SetUp() -> void override
    {
        options.page_size = 0x400;
        options.frame_count = 32;
        options.log_level = spdlog::level::trace;
        options.store = store.get();

        ASSERT_OK(db.open(ROOT, options));
    }

    auto TearDown() -> void override
    {
        interceptors::reset();
        ASSERT_OK(db.close());
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {internal::random_seed};
    Options options;
    Core db;
};

TEST_F(XactTests_, NewDatabaseIsOk)
{
    ASSERT_OK(db.status());
}

template<class Action>
static auto with_xact(XactTests_ &test, const Action &action)
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
        EXPECT_TRUE(expose_message(test.db.insert(stob(r.key), stob(r.value))));
    }
    return records;
}

auto erase_records(XactTests_ &test, Size n = 1'000)
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
    ASSERT_EQ(test.db.info().record_count(), 0);

    // Normal operations after abort should work.
    with_xact(test, [&test] {insert_records(test);});
}

TEST_F(XactTests_, CannotUseTransactionObjectAfterSuccessfulCommit)
{
    auto xact = db.transaction();
    insert_records(*this, 10);
    ASSERT_OK(xact.commit());
    ASSERT_TRUE(xact.abort().is_logic_error());
    ASSERT_TRUE(xact.commit().is_logic_error());
}

TEST_F(XactTests_, CannotUseTransactionObjectAfterSuccessfulAbort)
{
    auto xact = db.transaction();
    insert_records(*this, 10);
    ASSERT_OK(xact.abort());
    ASSERT_TRUE(xact.abort().is_logic_error());
    ASSERT_TRUE(xact.commit().is_logic_error());
}

TEST_F(XactTests_, TransactionObjectIsMovable)
{
    auto xact = db.transaction();
    auto xact2 = std::move(xact);
    xact = std::move(xact2);

    insert_records(*this, 10);
    ASSERT_OK(xact.commit());
}

TEST_F(XactTests_, AbortFirstXactWithSingleRecord)
{
    test_abort_first_xact(*this, 1);
}

TEST_F(XactTests_, AbortFirstXactWithMultipleRecords)
{
    test_abort_first_xact(*this, 8);
}

TEST_F(XactTests_, CommitIsACheckpoint)
{
    with_xact(*this, [this] {insert_records(*this);});

    auto xact = db.transaction();
    ASSERT_OK(xact.abort());
    ASSERT_EQ(db.info().record_count(), 1'000);
}

TEST_F(XactTests_, KeepsCommittedRecords)
{
    with_xact(*this, [this] {insert_records(*this);});

    auto xact = db.transaction();
    erase_records(*this);
    ASSERT_OK(xact.abort());
    ASSERT_EQ(db.info().record_count(), 1'000);

    // Normal operations after abort should work.
    with_xact(*this, [this] {erase_records(*this);});
    ASSERT_EQ(db.info().record_count(), 0);
}

template<class Test, class Itr>
static auto run_random_operations(Test &test, const Itr &begin, const Itr &end)
{
    for (auto itr = begin; itr != end; ++itr) {
        EXPECT_TRUE(expose_message(test.db.insert(stob(itr->key), stob(itr->value))));
    }

    std::vector<Record> committed;
    for (auto itr = begin; itr != end; ++itr) {
        if (test.random.get(5) == 0) {
            EXPECT_TRUE(expose_message(test.db.erase(stob(itr->key))));
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

    auto xact = test.db.transaction();
    auto committed = run_random_operations(test, cbegin(records), cbegin(records) + static_cast<long>(first_xact_size));
    ASSERT_OK(xact.commit());

    xact = test.db.transaction();
    run_random_operations(test, cbegin(records) + static_cast<long>(first_xact_size), cend(records));
    ASSERT_OK(xact.abort());

    // The database should contain exactly these records.
    ASSERT_EQ(test.db.info().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(test.db, key, value));
    }
}

TEST_F(XactTests_, AbortSecondXact_1_1)
{
    test_abort_second_xact(*this, 1, 1);
}

TEST_F(XactTests_, AbortSecondXact_1000_1)
{
    test_abort_second_xact(*this, 1'000, 1);
}

TEST_F(XactTests_, AbortSecondXact_1_1000)
{
    test_abort_second_xact(*this, 1, 1'000);
}

TEST_F(XactTests_, AbortSecondXact_1000_1000)
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

    for (Size i {}; i < n; ++i) {
        auto xact = test.db.transaction();
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

TEST_F(XactTests_, SanityCheck)
{
    for (const auto &[key, value]: run_random_transactions(*this, 20)) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(XactTests_, AbortSanityCheck)
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
    ASSERT_EQ(db.info().record_count(), committed.size());
    for (const auto &[key, value]: committed) {
        ASSERT_TRUE(tools::contains(db, key, value));
    }
}

TEST_F(XactTests_, PersistenceSanityCheck)
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

TEST_F(XactTests_, AtomicOperationSanityCheck)
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
        options.frame_count = 16;
        options.store = store.get();
        options.log_level = spdlog::level::err; // TODO
        ASSERT_OK(db.open(ROOT, options));
    }

    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {internal::random_seed};
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

    const auto info = test.db.info();
    auto s = Status::ok();

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
    return Status::ok();
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

    // TODO: Kinda sketchy to set this after we've written...
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
    // This tests database behavior when we encounter an error while flushing a dirty page to make room for a page read
    // during a query. In this case, we don't have a transaction we can try to abort, so we must exit the program. Next
    // time the database is opened, it will roll forward and apply any missing updates.
    add_sequential_records(db, 500);

    interceptors::set_write(FailOnce<5> {"test/data"});

    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    assert_error_42(c.status());
    assert_error_42(db.status());
}

TEST_F(FailureTests, DatabaseNeverWritesAfterPagesAreFlushedDuringQuery)
{
    add_sequential_records(db, 500);

    // This will cause all dirty pages to eventually be evicted to make room.
    auto c = db.first();
    for (; c.is_valid(); ++c) {}

    // Writes to any file will fail.
    interceptors::set_write(FailOnce<0> {"test/"});

    // We should be able to iterate through all pages without any writes occurring.
    c = db.first();
    for (; c.is_valid(); ++c) {}

    auto s = c.status();
    ASSERT_TRUE(s.is_not_found()) << s.what();

    s = db.status();
    ASSERT_TRUE(s.is_ok()) << s.what();
}

template<class Test>
static auto run_abort_restores_state_test(Test &test) -> void
{
    auto xact = test.db.transaction();
    auto s = modify_until_failure(test);
    assert_error_42(s);

    s = test.db.status();
    assert_error_42(s);

    ASSERT_OK(xact.abort());
    ASSERT_OK(test.db.status());
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError)
{
    interceptors::set_read(FailOnce<5> {"test/data"});
    run_abort_restores_state_test(*this);
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    run_abort_restores_state_test(*this);
}

TEST_F(FailureTests, AbortRestoresStateAfterWalWriteError)
{
    interceptors::set_write(FailOnce<5> {"test/wal-"});
    run_abort_restores_state_test(*this);
}

template<class Test>
static auto run_abort_is_reentrant_test(Test &test, int &counter, int &counter_max) -> void
{
    auto xact = test.db.transaction();
    auto s = modify_until_failure(test);
    assert_error_42(s);
    Size fail_count {};
    counter_max = 0;

    for (; ; ) {
        counter = 0;
        counter_max++;
        if (xact.abort().is_ok())
            break;
        s = test.db.status();
        assert_error_42(s);
        fail_count++;
    }
    ASSERT_GT(fail_count, 5);
    ASSERT_OK(test.db.status());

    interceptors::reset();
}

TEST_F(FailureTests, AbortIsReentrantForDataReadErrors)
{
    int counter {};
    int counter_max {10};
    interceptors::set_read([&counter, &counter_max](const std::string &path, Bytes&, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == counter_max ? Status::system_error("42") : Status::ok();
    });

    run_abort_is_reentrant_test(*this, counter, counter_max);
}

TEST_F(FailureTests, AbortIsReentrantForDataWriteErrors)
{
    int counter {};
    int counter_max {10};
    interceptors::set_write([&counter, &counter_max](const std::string &path, BytesView, Size) {
        if (path != "test/data")
            return Status::ok();
        return counter++ == counter_max ? Status::system_error("42") : Status::ok();
    });

    run_abort_is_reentrant_test(*this, counter, counter_max);
}

TEST_F(FailureTests, AbortRestoresStateAfterDataReadError_Atomic)
{
    interceptors::set_read(FailOnce<2> {"test/data"});
    assert_error_42(modify_until_failure(*this));
    ASSERT_OK(db.status());
}

TEST_F(FailureTests, AbortRestoresStateAfterDataWriteError_Atomic)
{
    interceptors::set_write(FailOnce<5> {"test/data"});
    assert_error_42(modify_until_failure(*this));
    ASSERT_OK(db.status());
}

enum class RecoveryTestFailureType {
    DATA_WRITE,
    DATA_SYNC,
    WAL_OPEN,
    WAL_READ,
    WAL_WRITE,
    WAL_SYNC,
};

[[nodiscard]]
static constexpr auto recovery_test_failure_type_name(RecoveryTestFailureType type) -> const char*
{
    switch (type) {
        case RecoveryTestFailureType::DATA_WRITE:
            return "DATA_WRITE";
        case RecoveryTestFailureType::DATA_SYNC:
            return "DATA_SYNC";
        case RecoveryTestFailureType::WAL_OPEN:
            return "WAL_OPEN";
        case RecoveryTestFailureType::WAL_READ:
            return "WAL_READ";
        case RecoveryTestFailureType::WAL_WRITE:
            return "WAL_WRITE";
        case RecoveryTestFailureType::WAL_SYNC:
            return "WAL_SYNC";
        default:
            return "";
    }
}

template<class Failure>
class RecoveryTestHarness: public testing::TestWithParam<RecoveryTestFailureType> {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";
    
    RecoveryTestHarness()
        : store {std::make_unique<HeapStorage>()}
    {
        options.page_size = 0x200;
        options.frame_count = 16;
        options.store = store.get();
    }

    ~RecoveryTestHarness() override = default;

    auto SetUp() -> void override
    {
        ASSERT_OK(open_database());

        static constexpr Size GROUP_SIZE {1'000};
        uncommitted = generator.generate(random, GROUP_SIZE * 2);
        committed.insert(cend(committed), cbegin(uncommitted) + GROUP_SIZE, cend(uncommitted));
        uncommitted.resize(GROUP_SIZE);

        // Transaction needs to go out of scope before the database is closed, hence the block.
        {
            static constexpr Size NUM_XACTS {10};
            static constexpr auto XACT_SIZE = GROUP_SIZE / NUM_XACTS;
            static_assert(GROUP_SIZE % XACT_SIZE == 0);

            // Commit NUM_XACTS transactions.
            auto begin = cbegin(committed);
            while (begin != cend(committed)) {
                auto xact = db.transaction();
                for (auto itr = begin; itr != begin + XACT_SIZE; ++itr) {
                    ASSERT_OK(db.insert(itr->key, itr->value));
                }
                ASSERT_OK(xact.commit());
                begin += XACT_SIZE;
            }

            switch (GetParam()) {
                case RecoveryTestFailureType::DATA_WRITE:
                    interceptors::set_write(Failure {"test/data"});
                    break;
                case RecoveryTestFailureType::DATA_SYNC:
                    interceptors::set_sync(Failure {"test/data"});
                    break;
                case RecoveryTestFailureType::WAL_OPEN:
                    interceptors::set_open(Failure {"test/wal-"});
                    break;
                case RecoveryTestFailureType::WAL_READ:
                    interceptors::set_read(Failure {"test/wal-"});
                    break;
                case RecoveryTestFailureType::WAL_WRITE:
                    interceptors::set_write(Failure {"test/wal-"});
                    break;
                case RecoveryTestFailureType::WAL_SYNC:
                    interceptors::set_sync(Failure {"test/wal-"});
                    break;
                default:
                    ADD_FAILURE() << "unrecognized test type \"" << int(GetParam()) << "\"";
            }

#define BREAK_IF_ERROR if (!s.is_ok()) {assert_error_42(s); break;}

            // Run transactions involving the uncommitted set until failure.
            for (; ; ) {
                auto xact = db.transaction();
                auto s = db.status();
                for (const auto &[key, value]: uncommitted) {
                    s = db.insert(key, value);
                    BREAK_IF_ERROR
                }
                BREAK_IF_ERROR
                for (const auto &[key, value]: uncommitted) {
                    s = db.erase(key);
                    BREAK_IF_ERROR
                }
                if (random.get(4)) {
                    s = db.commit();
                } else {
                    s = db.abort();
                }
                BREAK_IF_ERROR

#undef BREAK_IF_ERROR
            }
            assert_error_42(db.status());
        }
        assert_error_42(db.close());
        interceptors::reset();
    }

    auto open_database() -> Status
    {
        options.store = store.get();
        return db.open(ROOT, options);
    }

    auto validate() -> void
    {
        db.tree().TEST_validate_nodes();
        db.tree().TEST_validate_links();
        db.tree().TEST_validate_order();

        for (const auto &[key, value]: committed) {
            EXPECT_TRUE(tools::contains(db, key, value)) << "database should contain " << key;
        }
        for (const auto &[key, value]: uncommitted) {
            EXPECT_FALSE(db.find_exact(key).is_valid()) << "database should not contain " << key;
        }
    }

    std::unique_ptr<Storage> store;
    RecordGenerator generator {{16, 100, 10, false, true}};
    Random random {internal::random_seed};
    std::vector<Record> committed;
    std::vector<Record> uncommitted;
    Options options;
    Core db;
};

template<class Failure, Size Step>
class RecoveryReentrancyTestHarness: public RecoveryTestHarness<Failure> {
public:
    using Base = RecoveryTestHarness<Failure>;
    using Base::GetParam;

    explicit RecoveryReentrancyTestHarness(RecoveryTestFailureType type)
        : second_failure_type {type}
    {}

    auto SetUp() -> void override
    {
        Base::SetUp();

        auto callback = [this](const std::string &path, ...) -> Status
        {
            if (!stob(path).starts_with(prefix))
                return Status::ok();
            return counter++ == target ? Status::system_error("42") : Status::ok();
        };

        switch (second_failure_type) {
            case RecoveryTestFailureType::DATA_WRITE:
                prefix = "test/data";
                interceptors::set_write(callback);
                break;
            case RecoveryTestFailureType::DATA_SYNC:
                prefix = "test/data";
                interceptors::set_sync(callback);
                break;
            case RecoveryTestFailureType::WAL_OPEN:
                prefix = "test/wal-";
                interceptors::set_open(callback);
                break;
            case RecoveryTestFailureType::WAL_READ:
                prefix = "test/wal-";
                interceptors::set_read(callback);
                break;
            case RecoveryTestFailureType::WAL_WRITE:
                prefix = "test/wal-";
                interceptors::set_write(callback);
                break;
            case RecoveryTestFailureType::WAL_SYNC:
                prefix = "test/wal-";
                interceptors::set_sync(callback);
                break;
            default:
                ADD_FAILURE() << "unrecognized test type \"" << int(second_failure_type) << "\"";
        }
    }

    auto run_test() -> void
    {
        Size num_tries {};
        for (; ; num_tries++) {
            auto s = Base::open_database();
            if (s.is_ok()) {
                break;
            } else {
                assert_error_42(s);
                counter = 0;

                // Allow Step more calls of the target system call to succeed on the next round.
                target += Step;
            }
        }
        Base::validate();
        ASSERT_GT(num_tries, 0) << "recovery should have failed at least once";
    }

    RecoveryTestFailureType second_failure_type;
    std::string prefix;
    Size counter {};
    Size target {};
};

class RecoveryTests_FailImmediately: public RecoveryTestHarness<FailAfter<0>> {};

INSTANTIATE_TEST_SUITE_P(
    RecoveryTests_FailImmediately,
    RecoveryTests_FailImmediately,
    ::testing::Values(
        RecoveryTestFailureType::DATA_WRITE,
        RecoveryTestFailureType::DATA_SYNC,
        RecoveryTestFailureType::WAL_OPEN,
        RecoveryTestFailureType::WAL_READ
        // TODO: We can't use this test for WAL_WRITE, since we don't write during abort(). We have no way to make the
        //       abort procedure fail with the current setup.
        ),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryTests_FailImmediately, BasicRecovery)
{
    open_database();
    validate();
}

// Only can test system calls that are called at least 5 times before and during abort(). If we don't produce 5 calls during abort(),
// the procedure will succeed and the database will not need recovery.
class RecoveryTests_FailAfterDelay_5: public RecoveryTestHarness<FailAfter<5>> {};

INSTANTIATE_TEST_SUITE_P(
    RecoveryTests_FailAfterDelay_5,
    RecoveryTests_FailAfterDelay_5,
    ::testing::Values(
        RecoveryTestFailureType::DATA_WRITE,
        RecoveryTestFailureType::WAL_OPEN,
        RecoveryTestFailureType::WAL_READ),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryTests_FailAfterDelay_5, BasicRecovery)
{
    open_database();
    validate();
}

class RecoveryTests_FailAfterDelay_500: public RecoveryTestHarness<FailAfter<500>> {};

INSTANTIATE_TEST_SUITE_P(
    RecoveryTests_FailAfterDelay_500,
    RecoveryTests_FailAfterDelay_500,
    ::testing::Values(
        RecoveryTestFailureType::DATA_WRITE,
        RecoveryTestFailureType::WAL_OPEN,
        RecoveryTestFailureType::WAL_READ),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryTests_FailAfterDelay_500, BasicRecovery)
{
    open_database();
    validate();
}

class RecoveryReentrancyTests_FailImmediately_100: public RecoveryReentrancyTestHarness<FailAfter<0>, 100> {
public:
    RecoveryReentrancyTests_FailImmediately_100()
        : RecoveryReentrancyTestHarness<FailAfter<0>, 100> {RecoveryTestFailureType::DATA_WRITE}
    {}
};

INSTANTIATE_TEST_SUITE_P(
    RecoveryReentrancyTests_FailImmediately_100,
    RecoveryReentrancyTests_FailImmediately_100,
    ::testing::Values(
        RecoveryTestFailureType::DATA_WRITE,
        RecoveryTestFailureType::WAL_OPEN),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryReentrancyTests_FailImmediately_100, RecoveryIsReentrant)
{
    run_test();
    validate();
}

class RecoveryReentrancyTests_FailImmediately_10000: public RecoveryReentrancyTestHarness<FailAfter<0>, 10000> {
public:
    RecoveryReentrancyTests_FailImmediately_10000()
        : RecoveryReentrancyTestHarness<FailAfter<0>, 10000> {RecoveryTestFailureType::DATA_WRITE}
    {}
};

INSTANTIATE_TEST_SUITE_P(
    RecoveryReentrancyTests_FailImmediately_10000,
    RecoveryReentrancyTests_FailImmediately_10000,
    ::testing::Values(
        RecoveryTestFailureType::WAL_READ),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryReentrancyTests_FailImmediately_10000, RecoveryIsReentrant)
{
    run_test();
    validate();
}

class RecoveryReentrancyTests_FailAfterDelay_100: public RecoveryReentrancyTestHarness<FailAfter<100>, 100> {
public:
    RecoveryReentrancyTests_FailAfterDelay_100()
        : RecoveryReentrancyTestHarness<FailAfter<100>, 100> {RecoveryTestFailureType::DATA_WRITE}
    {}
};

INSTANTIATE_TEST_SUITE_P(
    RecoveryReentrancyTests_FailAfterDelay_100,
    RecoveryReentrancyTests_FailAfterDelay_100,
    ::testing::Values(
        RecoveryTestFailureType::DATA_WRITE,
        RecoveryTestFailureType::WAL_OPEN),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryReentrancyTests_FailAfterDelay_100, RecoveryIsReentrant)
{
    run_test();
    validate();
}

class RecoveryReentrancyTests_FailAfterDelay_10000: public RecoveryReentrancyTestHarness<FailAfter<100>, 10000> {
public:
    RecoveryReentrancyTests_FailAfterDelay_10000()
        : RecoveryReentrancyTestHarness<FailAfter<100>, 10000> {RecoveryTestFailureType::DATA_WRITE}
    {}
};

INSTANTIATE_TEST_SUITE_P(
    RecoveryReentrancyTests_FailAfterDelay_10000,
    RecoveryReentrancyTests_FailAfterDelay_10000,
    ::testing::Values(
        RecoveryTestFailureType::WAL_READ),
    [](const auto &info) {
        return recovery_test_failure_type_name(info.param);
    });

TEST_P(RecoveryReentrancyTests_FailAfterDelay_10000, RecoveryIsReentrant)
{
    run_test();
    validate();
}

} // namespace calico