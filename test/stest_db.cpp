// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"
#include "test.h"
#include "tx_impl.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

using namespace stest;

struct DatabaseState {
    std::string filename;
    Options db_opt;
    BucketOptions b_opt;
    
    static constexpr uint32_t kOKMask = 1 << Status::kOK;
    uint32_t error_mask = kOKMask;

    // A std::map<std::string, std::map<std::string, std::string>> representing
    // the expected contents of the database.
    KVStore model_store;

    DB *db = nullptr;
    Tx *tx = nullptr;

    Status s;
    
    static constexpr const char *kBucketNames[] = {
        "A", "B", "C", "D", 
        "E", "F", "G", "H", 
        "I", "J", "K", "L",
        "M", "N", "O", "P",
    };
    static constexpr size_t kMaxBuckets = ARRAY_SIZE(kBucketNames);

    struct BucketState final {
        static constexpr size_t kCursorsPerBucket = 3;
        Cursor *cs[kCursorsPerBucket] = {};
        Bucket b;

        ~BucketState()
        {
            close_cursors();
        }

        auto open_cursors(Tx &t) -> void
        {
            for (auto *&c : cs) {
                c = t.new_cursor(b);
            }
        }

        auto close_cursors() -> void
        {
            for (const auto *c : cs) {
                delete c;
                c = nullptr;
            }
        }
    } buckets[kMaxBuckets];
    
    auto open_bucket() -> void
    {
        check_status(kOKMask);
        for (size_t i = 0; i < kMaxBuckets; ++i) {
            if (buckets[i].b.state == nullptr) {
                s = tx->open_bucket(kBucketNames[i], buckets[i].b);
                if (s.is_ok()) {
                    buckets[i].open_cursors(*tx);
                }
                break;
            }
        }
    }

    auto create_bucket() -> void
    {
        check_status(kOKMask);
        for (size_t i = 0; i < kMaxBuckets; ++i) {
            if (buckets[i].b.state == nullptr) {
                s = tx->create_bucket(b_opt, kBucketNames[i], &buckets[i].b);
                if (s.is_ok()) {
                    buckets[i].open_cursors(*tx);
                }
                break;
            }
        }
    }

    auto drop_bucket() -> void
    {
        check_status(kOKMask);
        for (size_t i = 0; i < kMaxBuckets; ++i) {
            const auto j = kMaxBuckets - i - 1;
            auto b = buckets[j];
            if (b.b.state) {
                b.close_cursors();
                s = tx->drop_bucket(kBucketNames[j]);
                b.b.state = nullptr;
                break;
            }
        }
    }
    
    auto check_status(uint32_t mask) const -> void
    {
        ASSERT_FALSE(mask & (1 << s.code())) << s.to_string();
        ASSERT_FALSE(mask & (1 << tx->status().code())) << tx->status().to_string();
    }

    [[nodiscard]] auto has_readable_tx() const -> bool
    {
        return db && tx && tx->status().is_ok();
    }

    [[nodiscard]] auto has_writable_tx() const -> bool
    {
        return db && tx && tx->status().is_ok();
    }
    
    auto open_db() -> void
    {
        s = ModelDB::open(db_opt, filename, model_store, db);
    }
    
    auto close_db() -> void
    {
        check_status(error_mask);
        s = Status::ok();
        
        delete db;
        db = nullptr;
    }
    
    auto start_readonly_tx() -> void
    {
        CALICODB_EXPECT_NE(db, nullptr);
        CALICODB_EXPECT_EQ(tx, nullptr);
        s = db->new_tx(tx);
    }
    
    auto start_read_write_tx() -> void
    {
        CALICODB_EXPECT_NE(db, nullptr);
        CALICODB_EXPECT_EQ(tx, nullptr);
        s = db->new_tx(WriteTag(), tx);
    }
    
    auto finish_tx() -> void
    {
        for (auto &[cs, b] : buckets) {
            for (auto *c : cs) {
                delete c;
                c = nullptr;
            }
            // This is only necessary as an indication that this bucket is closed.
            // The actual bucket resources are cleaned up automatically when the
            // Tx object is destroyed.
            b.state = nullptr;
        }

        check_status(error_mask);
        s = Status::ok();
        
        delete tx;
        tx = nullptr;
        tx = nullptr;
    }
};

class OpenDatabaseRule : public Rule<DatabaseState> {
public:
    explicit OpenDatabaseRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~OpenDatabaseRule() override = default;

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

class CloseDatabaseRule : public Rule<DatabaseState> {
public:
    explicit CloseDatabaseRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~CloseDatabaseRule() override = default;

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

class StartReadonlyTransactionRule : public Rule<DatabaseState> {
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

class StartReadWriteTransactionRule : public Rule<DatabaseState> {
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

class FinishTransactionRule : public Rule<DatabaseState> {
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

class OpenBucketRule : public Rule<DatabaseState> {
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

class CreateBucketRule : public Rule<DatabaseState> {
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

class DropBucketRule : public Rule<DatabaseState> {
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

class VacuumRule : public Rule<DatabaseState> {
public:
    explicit VacuumRule(const char *name)
        : Rule<DatabaseState>(name)
    {
    }

    ~VacuumRule() override = default;

    auto precondition(const DatabaseState &state) -> bool override
    {
        return state.db && state.tx && state.tx->status().is_ok();
    }

protected:
    auto action(DatabaseState &state) -> void override
    {
        state.check_status(DatabaseState::kOKMask);
        state.s = state.tx->vacuum();
    }
};

struct DatabaseRules {
    OpenDatabaseRule open_db;
    CloseDatabaseRule close_db;
    StartReadonlyTransactionRule start_readonly_tx;
    StartReadWriteTransactionRule start_read_write_tx;

    explicit DatabaseRules()
        : open_db("OpenDB"),
          close_db("CloseDB"),
          start_readonly_tx("StartReadonlyTx"),
          start_read_write_tx("StartReadWriteTx")
    {
    }
};

TEST(a, b)
{
    DatabaseState state;
    OpenDatabaseRule open_db_rule("OpenDB");
    CloseDatabaseRule close_db_rule("CloseDB");
    StartReadWriteTransactionRule read_write_tx_rule("StartReadWriteTx");
    StartReadonlyTransactionRule readonly_tx_rule("StartReadonlyTx");
}

} // namespace calicodb::test
