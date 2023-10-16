// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "db_impl.h"
#include "model.h"
#include "stest/bounded_scenario.h"
#include "stest/random_scenario.h"
#include "stest/rule_scenario.h"
#include "stest/sequence_scenario.h"
#include "test.h"
#include "tx_impl.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

using namespace stest;

struct DatabaseState {
    static constexpr uint32_t kMaxKeyLen = 1'024;
    static constexpr uint32_t kMaxValueLen = TEST_PAGE_SIZE;
    RandomGenerator rng;

    [[nodiscard]] auto random_chunk(size_t max_length) const -> Slice
    {
        return rng.Generate(rng.Next(max_length));
    }

    std::string filename = "/tmp/calicodb_stest_db";
    Options db_opt;

    static constexpr uint32_t kOKMask = 1 << Status::kOK;
    uint32_t error_mask = kOKMask;

    // A std::map<std::string, std::map<std::string, std::string>> representing
    // the expected contents of the database.
    ModelStore model_store;

    DB *db = nullptr;
    Tx *tx = nullptr;

    Status s;

    enum {
        kNone,
        kReadable,
        kWritable,
    } state = kNone;

    static constexpr const char *kBucketNames[] = {
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
    };
    static constexpr size_t kMaxBuckets = ARRAY_SIZE(kBucketNames);

    struct BucketState final {
        TestBucket bucket;
        TestCursor cursor;
    } buckets[kMaxBuckets];

    struct BucketSelection {
        BucketState *state_addr;
        size_t bucket_id;

        explicit operator bool() const
        {
            return state_addr != nullptr;
        }
    };

    std::function<BucketSelection(bool)> select_next_bucket_callback = [this](bool find_existing) {
        for (size_t i = 0; i < kMaxBuckets; ++i) {
            if (!!buckets[i].bucket == find_existing) {
                return BucketSelection{&buckets[i], i};
            }
        }
        return BucketSelection{};
    };

    std::function<BucketSelection(bool)> select_random_bucket_callback = [this](bool find_existing) {
        bool seen[kMaxBuckets] = {};
        for (size_t num_seen = 0; num_seen < kMaxBuckets;) {
            const auto i = rng.Next(kMaxBuckets - 1);
            if (seen[i]) {
                continue;
            }
            if (!!buckets[i].bucket == find_existing) {
                return BucketSelection{&buckets[i], i};
            }
            seen[i] = true;
            ++num_seen;
        }
        return BucketSelection{};
    };

    std::function<BucketSelection(bool)> bucket_selector = select_next_bucket_callback;

    auto select_bucket() -> BucketSelection
    {
        check_status(kOKMask);
        return select_random_bucket_callback(true);
    }

    auto open_bucket() -> void
    {
        check_status(kOKMask);
        auto b = bucket_selector(false);
        Bucket *bucket;
        s = tx->main().open_bucket(kBucketNames[b.bucket_id], bucket);
        if (s.is_ok()) {
            b.state_addr->bucket.reset(bucket);
            b.state_addr->cursor.reset(bucket->new_cursor());
        } else if (s.is_invalid_argument()) {
            s = Status::ok();
        }
    }

    auto create_bucket() -> void
    {
        check_status(kOKMask);
        const auto b = bucket_selector(false);
        Bucket *bucket;
        s = tx->main().create_bucket_if_missing(kBucketNames[b.bucket_id], &bucket);
        if (s.is_ok()) {
            b.state_addr->bucket.reset(bucket);
            b.state_addr->cursor.reset(bucket->new_cursor());
        }
    }

    auto close_bucket() -> void
    {
        check_status(kOKMask);
        if (const auto b = bucket_selector(true)) {
            b.state_addr->cursor.reset();
            b.state_addr->bucket.reset();
        }
    }

    auto drop_bucket() -> void
    {
        check_status(kOKMask);
        if (const auto b = bucket_selector(true)) {
            s = tx->main().drop_bucket(kBucketNames[b.bucket_id]);
            // It shouldn't matter that the bucket is dropped before the cursors positioned
            // on it are delete'd.
            buckets[b.bucket_id].bucket.reset();
            buckets[b.bucket_id].cursor.reset();
        }
    }

    auto write_records(BucketSelection &selected_bucket) -> void
    {
        check_status(kOKMask);
        if (!selected_bucket) {
            return;
        }
        auto &b = *selected_bucket.state_addr->bucket;
        for (size_t i = 0, n = rng.Next(1'234); i < n && s.is_ok(); ++i) {
            const auto key = random_chunk(kMaxKeyLen);
            const auto value = random_chunk(kMaxValueLen);
            s = b.put(key, value);
        }
    }

    auto modify_records(BucketSelection &selected_bucket) -> void
    {
        check_status(kOKMask);
        if (selected_bucket) {
            auto &b = *selected_bucket.state_addr->bucket;
            auto &c = *selected_bucket.state_addr->cursor;
            for (size_t i = 0, n = rng.Next(1'234); i < n && s.is_ok(); ++i) {
                if (try_attach_and_move_cursor(c, i)) {
                    s = b.put(c, random_chunk(kMaxValueLen));
                }
            }
        }
    }

    auto try_attach_cursor(Cursor &c, size_t iteration = 0) const -> bool
    {
        for (int i = 0; i < 3 && !c.is_valid(); ++i) {
            c.seek(random_chunk(kMaxKeyLen));
        }
        if (!c.is_valid()) {
            if (iteration & 1) {
                c.seek_first();
            } else {
                c.seek_last();
            }
        }
        return c.is_valid();
    }

    auto try_attach_and_move_cursor(Cursor &c, size_t iteration = 0) const -> bool
    {
        if (try_attach_cursor(c, iteration)) {
            if (iteration & 1) {
                c.previous();
            } else {
                c.next();
            }
        }
        return c.is_valid();
    }

    auto read_records(const BucketSelection &selected_bucket) -> void
    {
        check_status(kOKMask);
        if (!selected_bucket) {
            return;
        }
        auto &c = *selected_bucket.state_addr->cursor;
        for (size_t i = 0, n = rng.Next(1'234); i < n && s.is_ok(); ++i) {
            if (!try_attach_cursor(c, i)) {
                break;
            }
            c.seek(random_chunk(kMaxKeyLen));
        }
    }

    auto erase_records(BucketSelection &selected_bucket) -> void
    {
        check_status(kOKMask);
        if (!selected_bucket) {
            return;
        }
        auto &b = *selected_bucket.state_addr->bucket;
        auto &c = *selected_bucket.state_addr->cursor;
        for (size_t i = 0, n = rng.Next(123); i < n && s.is_ok(); ++i) {
            if (!try_attach_cursor(c, i)) {
                break;
            }
            c.seek(rng.Generate(kMaxKeyLen));
            if (c.is_valid()) {
                s = b.erase(c);
            }
        }
    }

    auto check_status(uint32_t mask) const -> void
    {
        ASSERT_EQ(mask, mask & (1 << s.code())) << s.message();
        if (tx) {
            ASSERT_EQ(s, tx->status()) << tx->status().message();
        }
    }

    [[nodiscard]] auto has_readable_tx() const -> bool
    {
        return db && tx && tx->status().is_ok() && state >= kReadable;
    }

    [[nodiscard]] auto has_writable_tx() const -> bool
    {
        return has_readable_tx() && state >= kWritable;
    }

    auto open_db() -> void
    {
        s = ModelDB::open(db_opt, filename.c_str(), model_store, db);
    }

    auto close_db() -> void
    {
        check_status(error_mask);
        ASSERT_EQ(state, kNone);
        s = Status::ok();

        delete db;
        db = nullptr;
    }

    auto start_readonly_tx() -> void
    {
        CALICODB_EXPECT_NE(db, nullptr);
        CALICODB_EXPECT_EQ(tx, nullptr);
        ASSERT_EQ(state, kNone);
        s = db->new_reader(tx);
        if (s.is_ok()) {
            state = kReadable;
        }
    }

    auto start_read_write_tx() -> void
    {
        CALICODB_EXPECT_NE(db, nullptr);
        CALICODB_EXPECT_EQ(tx, nullptr);
        ASSERT_EQ(state, kNone);
        s = db->new_writer(tx);
        if (s.is_ok()) {
            state = kWritable;
        }
    }

    auto finish_tx() -> void
    {
        check_status(error_mask);
        ASSERT_NE(state, kNone);
        close_all_buckets();

        state = kNone;
        s = Status::ok();

        delete tx;
        tx = nullptr;
    }

    auto close_all_buckets() -> void
    {
        for (auto &b : buckets) {
            b.bucket.reset();
            b.cursor.reset();
        }
    }
};

class OpenDBRule : public Rule<DatabaseState>
{
public:
    explicit OpenDBRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~OpenDBRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.db == nullptr;
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.open_db();
    }
};

class CloseDBRule : public Rule<DatabaseState>
{
public:
    explicit CloseDBRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~CloseDBRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.db != nullptr;
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.close_db();
    }
};

class StartReadonlyTransactionRule : public Rule<DatabaseState>
{
public:
    explicit StartReadonlyTransactionRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~StartReadonlyTransactionRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.db && !state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.start_readonly_tx();
    }
};

class StartReadWriteTransactionRule : public Rule<DatabaseState>
{
public:
    explicit StartReadWriteTransactionRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~StartReadWriteTransactionRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.db && !state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.start_read_write_tx();
    }
};

class FinishTransactionRule : public Rule<DatabaseState>
{
public:
    explicit FinishTransactionRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~FinishTransactionRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.finish_tx();
    }
};

class CreateBucketRule : public Rule<DatabaseState>
{
public:
    explicit CreateBucketRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~CreateBucketRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_writable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.create_bucket();
    }
};

class OpenBucketRule : public Rule<DatabaseState>
{
public:
    explicit OpenBucketRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~OpenBucketRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.open_bucket();
    }
};

class CloseBucketRule : public Rule<DatabaseState>
{
public:
    explicit CloseBucketRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~CloseBucketRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.close_bucket();
    }
};

class DropBucketRule : public Rule<DatabaseState>
{
public:
    explicit DropBucketRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~DropBucketRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_writable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.drop_bucket();
    }
};

class ReadRecordsRule : public Rule<DatabaseState>
{
public:
    explicit ReadRecordsRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~ReadRecordsRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        if (auto b = state.select_bucket()) {
            state.read_records(b);
        }
    }
};

enum class ModType {
    kWriteRecords,
    kModifyRecords,
    kEraseRecords,
};

template <ModType Type>
class ModifyRecordsRule : public Rule<DatabaseState>
{
public:
    explicit ModifyRecordsRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~ModifyRecordsRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_writable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        if (auto b = state.select_bucket()) {
            if constexpr (Type == ModType::kWriteRecords) {
                state.write_records(b);
            } else if constexpr (Type == ModType::kModifyRecords) {
                state.modify_records(b);
            } else if constexpr (Type == ModType::kEraseRecords) {
                state.erase_records(b);
            }
        }
    }
};

class VacuumDBRule : public Rule<DatabaseState>
{
public:
    explicit VacuumDBRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~VacuumDBRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_writable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.check_status(DatabaseState::kOKMask);
        state.s = state.tx->vacuum();
    }
};

class ValidateDBRule : public Rule<DatabaseState>
{
public:
    explicit ValidateDBRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~ValidateDBRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.has_readable_tx();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.check_status(DatabaseState::kOKMask);
        reinterpret_cast<const ModelDB *>(state.db)->check_consistency();
    }
};

struct Routines {
    OpenDBRule open_db_rule;
    CloseDBRule close_db_rule;
    ValidateDBRule validate_db_rule;
    StartReadonlyTransactionRule start_readonly_tx_rule;
    StartReadWriteTransactionRule start_read_write_tx_rule;
    FinishTransactionRule finish_tx_rule;
    VacuumDBRule vacuum_db_rule;
    DropBucketRule drop_bucket_rule;
    CreateBucketRule create_bucket_rule;
    OpenBucketRule open_bucket_rule;
    CloseBucketRule close_bucket_rule;
    ReadRecordsRule read_records_rule;
    ModifyRecordsRule<ModType::kWriteRecords> write_records_rule;
    ModifyRecordsRule<ModType::kModifyRecords> modify_records_rule;
    ModifyRecordsRule<ModType::kEraseRecords> erase_records_rule;

    RuleScenario<DatabaseState> open_db;
    RuleScenario<DatabaseState> close_db;
    RuleScenario<DatabaseState> validate_db;
    RuleScenario<DatabaseState> read_records;
    RuleScenario<DatabaseState> start_readonly_tx;
    RuleScenario<DatabaseState> start_read_write_tx;
    RuleScenario<DatabaseState> finish_tx;

    Rule<DatabaseState> *all_readonly_ops[3] = {
        &open_bucket_rule,
        &close_bucket_rule,
        &read_records_rule,
    };
    Rule<DatabaseState> *all_read_write_ops[9] = {
        &create_bucket_rule,
        &open_bucket_rule,
        &close_bucket_rule,
        &drop_bucket_rule,
        &read_records_rule,
        &write_records_rule,
        &modify_records_rule,
        &erase_records_rule,
        &vacuum_db_rule,
    };
    Rule<DatabaseState> *read_ops[1] = {
        &read_records_rule,
    };
    Rule<DatabaseState> *read_and_write_ops[4] = {
        &read_records_rule,
        &write_records_rule,
        &modify_records_rule,
        &erase_records_rule,
    };
    Rule<DatabaseState> *erase_and_vacuum_ops[2] = {
        &erase_records_rule,
        &vacuum_db_rule,
    };
    Rule<DatabaseState> *bucket_access_ops[4] = {
        &create_bucket_rule,
        &open_bucket_rule,
        &close_bucket_rule,
        &drop_bucket_rule,
    };

    RandomScenario<DatabaseState> all_readonly_ops_random;
    RandomScenario<DatabaseState> all_read_write_ops_random;
    RandomScenario<DatabaseState> random_reads;
    RandomScenario<DatabaseState> random_reads_and_writes;
    RandomScenario<DatabaseState> random_erases_and_vacuums;
    RandomScenario<DatabaseState> random_bucket_accesses;

    BoundedScenario<DatabaseState> all_readonly_ops_few;
    BoundedScenario<DatabaseState> all_read_write_ops_few;
    BoundedScenario<DatabaseState> few_reads;
    BoundedScenario<DatabaseState> few_reads_and_writes;
    BoundedScenario<DatabaseState> few_erases_and_vacuums;
    BoundedScenario<DatabaseState> few_bucket_accesses;

    BoundedScenario<DatabaseState> all_readonly_ops_many;
    BoundedScenario<DatabaseState> all_read_write_ops_many;
    BoundedScenario<DatabaseState> many_reads;
    BoundedScenario<DatabaseState> many_reads_and_writes;
    BoundedScenario<DatabaseState> many_erases_and_vacuums;
    BoundedScenario<DatabaseState> many_bucket_accesses;

    static constexpr size_t kFewIterations = 10;
    static constexpr size_t kManyIterations = 1'000;

    explicit Routines()
        : open_db_rule("OpenDB"),
          close_db_rule("CloseDB"),
          validate_db_rule("ValidateDBRule"),
          start_readonly_tx_rule("StartReadonlyTx"),
          start_read_write_tx_rule("StartReadWriteTx"),
          finish_tx_rule("FinishTx"),
          vacuum_db_rule("VacuumDB"),
          drop_bucket_rule("DropBucket"),
          create_bucket_rule("CreateBucket"),
          open_bucket_rule("OpenBucket"),
          close_bucket_rule("CloseBucket"),
          read_records_rule("ReadRecords"),
          write_records_rule("WriteRecords"),
          modify_records_rule("ModifyRecords"),
          erase_records_rule("EraseRecords"),

          open_db(open_db_rule),
          close_db(close_db_rule),
          validate_db(validate_db_rule),
          read_records(read_records_rule),
          start_readonly_tx(start_readonly_tx_rule),
          start_read_write_tx(start_read_write_tx_rule),
          finish_tx(finish_tx_rule),

          all_readonly_ops_random("RandomReadonlyOps", all_readonly_ops, ARRAY_SIZE(all_readonly_ops)),
          all_read_write_ops_random("RandomReadWriteOps", all_read_write_ops, ARRAY_SIZE(all_read_write_ops)),
          random_reads("RandomReads", read_ops, ARRAY_SIZE(read_ops)),
          random_reads_and_writes("RandomReadAndModifyOps", read_and_write_ops, ARRAY_SIZE(read_and_write_ops)),
          random_erases_and_vacuums("RandomEraseAndVacuumOps", erase_and_vacuum_ops, ARRAY_SIZE(erase_and_vacuum_ops)),
          random_bucket_accesses("RandomBucketAccessOps", bucket_access_ops, ARRAY_SIZE(bucket_access_ops)),

          all_readonly_ops_few("FewReadonlyOps", all_readonly_ops_random, kFewIterations),
          all_read_write_ops_few("FewReadWriteOps", all_read_write_ops_random, kFewIterations),
          few_reads("FewReads", random_reads, kFewIterations),
          few_reads_and_writes("FewReadAndModifyOps", random_reads_and_writes, kFewIterations),
          few_erases_and_vacuums("FewEraseAndVacuumOps", random_erases_and_vacuums, kFewIterations),
          few_bucket_accesses("FewBucketAccessOps", random_bucket_accesses, kFewIterations),

          all_readonly_ops_many("ManyReadonlyOps", all_readonly_ops_random, kManyIterations),
          all_read_write_ops_many("ManyReadWriteOps", all_read_write_ops_random, kManyIterations),
          many_reads("ManyReads", random_reads, kManyIterations),
          many_reads_and_writes("ManyReadAndModifyOps", random_reads_and_writes, kManyIterations),
          many_erases_and_vacuums("ManyEraseAndVacuumOps", random_erases_and_vacuums, kManyIterations),
          many_bucket_accesses("ManyBucketAccessOps", random_bucket_accesses, kManyIterations)
    {
    }
} g_routines;

class STestDB : public testing::Test
{
protected:
    DatabaseState m_state;

    explicit STestDB()
    {
        remove_calicodb_files(m_state.filename);
    }

    auto TearDown() -> void override
    {
        ASSERT_EQ(m_state.db, nullptr);
    }
};

TEST_F(STestDB, SanityCheck)
{
    Scenario<DatabaseState> *sequences[][2] = {
        {
            &g_routines.start_read_write_tx,
            &g_routines.all_read_write_ops_many,
        },
        {
            &g_routines.start_read_write_tx,
            &g_routines.many_reads_and_writes,
        },
        {
            &g_routines.start_readonly_tx,
            &g_routines.all_readonly_ops_many,
        },
    };

    g_routines.open_db.run(m_state);

    static constexpr const char *kSequenceNames[] = {"1", "2", "3"};
    for (size_t i = 0; i < ARRAY_SIZE(sequences); ++i) {
        SequenceScenario<DatabaseState> scenario(
            kSequenceNames[i],
            sequences[i],
            ARRAY_SIZE(sequences[i]));

        scenario.run(m_state);
        ASSERT_OK(m_state.s);

        g_routines.validate_db.run(m_state);
        g_routines.finish_tx.run(m_state);
    }

    g_routines.close_db.run(m_state);
}

} // namespace calicodb::test
