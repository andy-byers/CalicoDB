// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TOOLS_H
#define CALICODB_TOOLS_H

#include "calicodb/db.h"
#include "calicodb/env.h"
#include "db_impl.h"
#include "env_posix.h"
#include "logging.h"
#include "txn_impl.h"
#include <climits>
#include <cstdarg>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>

#define CHECK_TRUE(cond)                             \
    do {                                             \
        if (!(cond)) {                               \
            std::fputs(#cond " is false\n", stderr); \
            std::abort();                            \
        }                                            \
    } while (0)

#define CHECK_FALSE(cond) \
    CHECK_TRUE(!(cond))

#define CHECK_OK(expr)                                                         \
    do {                                                                       \
        if (auto assert_s = (expr); !assert_s.is_ok()) {                       \
            std::fprintf(                                                      \
                stderr,                                                        \
                "expected \"" #expr " == Status::ok()\" but got \"%s\": %s\n", \
                get_status_name(assert_s),                                     \
                assert_s.to_string().c_str());                                 \
            std::abort();                                                      \
        }                                                                      \
    } while (0)

#define CHECK_EQ(lhs, rhs)                        \
    do {                                          \
        if ((lhs) != (rhs)) {                     \
            std::fputs(#lhs " != " #rhs, stderr); \
            std::abort();                         \
        }                                         \
    } while (0)

namespace calicodb::tools
{

class TestDir final
{
public:
    static constexpr auto kDirname = "testdir";

    explicit TestDir(const std::string &location)
        : m_prefix(join_paths(location, kDirname))
    {
        reset();
    }

    ~TestDir()
    {
        std::filesystem::remove_all(m_prefix);
    }

    auto reset() const -> void
    {
        std::filesystem::remove_all(m_prefix);
        std::filesystem::create_directory(m_prefix);
    }

    [[nodiscard]] auto as_child(const std::string &filename) const -> std::string
    {
        return join_paths(m_prefix, filename);
    }

private:
    std::string m_prefix;
};

class WalStub : public Wal
{
public:
    ~WalStub() override = default;

    [[nodiscard]] auto read(Id, char *&) -> Status override
    {
        return Status::not_found("");
    }

    [[nodiscard]] auto write(const PageRef *, std::size_t) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto checkpoint(bool, std::size_t *) -> Status override
    {
        return Status::ok();
    }

    auto rollback() -> void override
    {
    }

    [[nodiscard]] auto close(std::size_t &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto statistics() const -> WalStatistics override
    {
        return {};
    }
};

class FakeWal : public Wal
{
    std::map<Id, std::string> m_committed;
    std::map<Id, std::string> m_pending;
    std::size_t m_db_size = 0;
    File *m_db_file;
    Parameters m_param;

public:
    explicit FakeWal(const Parameters &param);
    ~FakeWal() override = default;

    [[nodiscard]] auto read(Id page_id, char *&out) -> Status override;
    [[nodiscard]] auto write(const PageRef *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto checkpoint(bool, std::size_t *) -> Status override;
    [[nodiscard]] auto statistics() const -> WalStatistics override;
    [[nodiscard]] auto sync() -> Status override { return Status::ok(); }
    [[nodiscard]] auto close(std::size_t &) -> Status override;
    [[nodiscard]] auto start_reader(bool &) -> Status override { return Status::ok(); }
    [[nodiscard]] auto start_writer() -> Status override { return Status::ok(); }
    auto finish_reader() -> void override {}
    auto finish_writer() -> void override {}
    auto rollback() -> void override;
};

template <std::size_t Length = 12>
static auto integral_key(std::size_t key) -> std::string
{
    auto key_string = std::to_string(key);
    if (key_string.size() == Length) {
        return key_string;
    } else if (key_string.size() > Length) {
        return key_string.substr(0, Length);
    } else {
        return std::string(Length - key_string.size(), '0') + key_string;
    }
}

inline auto validate_txn(const Txn &txn)
{
    reinterpret_cast<const TxnImpl &>(txn).TEST_validate();
}

// Modified from LevelDB.
class RandomGenerator
{
private:
    using Engine = std::default_random_engine;

    std::string m_data;
    mutable std::size_t m_pos = 0;
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(std::size_t size = 256 /* KiB */ * 1'024);
    auto Generate(std::size_t len) const -> Slice;

    // Not in LevelDB.
    auto Next(U64 t_max) const -> U64
    {
        std::uniform_int_distribution<U64> dist(0, t_max);
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(U64 t_min, U64 t_max) const -> U64
    {
        std::uniform_int_distribution<U64> dist(t_min, t_max);
        return dist(m_rng);
    }
};

struct DatabaseCounts {
    std::size_t records = 0;
    std::size_t pages = 0;
    std::size_t updates = 0;
};

[[nodiscard]] inline auto parse_db_counts(std::string prop) -> DatabaseCounts
{
    DatabaseCounts counts;

    CHECK_EQ(prop.find("records:"), 0);
    prop = prop.substr(8);
    auto pos = prop.find(',');
    CHECK_TRUE(pos != std::string::npos);
    counts.records = std::stoi(prop.substr(0, pos));
    prop = prop.substr(pos);

    CHECK_EQ(prop.find(",pages:"), 0);
    prop = prop.substr(7);
    pos = prop.find(',');
    CHECK_TRUE(pos != std::string::npos);
    counts.pages = std::stoi(prop.substr(0, pos));
    prop = prop.substr(pos);

    CHECK_EQ(prop.find(",updates:"), 0);
    prop = prop.substr(9);
    pos = prop.find(',');
    CHECK_EQ(pos, std::string::npos);
    counts.updates = std::stoi(prop);

    return counts;
}

auto print_references(Pager &pager) -> void;
auto print_wals(Env &env, std::size_t page_size, const std::string &prefix) -> void;
auto hexdump_page(const Page &page) -> void;

auto read_file_to_string(Env &env, const std::string &filename) -> std::string;
auto write_string_to_file(Env &env, const std::string &filename, const std::string &buffer, long offset = -1) -> void;
auto assign_file_contents(Env &env, const std::string &filename, const std::string &contents) -> void;
auto fill_db(DB &db, const std::string &tablename, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(Txn &txn, const std::string &tablename, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto expect_db_contains(DB &db, const std::string &tablename, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(Txn &txn, const std::string &tablename, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(const Table &table, const std::map<std::string, std::string> &map) -> void;

template <class Callback>
class CustomTxnHandler : public TxnHandler
{
    Callback m_callback;

public:
    CustomTxnHandler(Callback callback)
        : m_callback(std::move(callback))
    {
    }

    ~CustomTxnHandler() override = default;

    [[nodiscard]] auto exec(Txn &txn) -> Status override
    {
        return m_callback(txn);
    }
};

template <std::size_t Length = 16>
class NumericKey
{
    std::string m_value;

public:
    explicit NumericKey()
        : m_value("0")
    {
    }

    explicit NumericKey(U64 number)
        : m_value(integral_key<Length>(number))
    {
    }

    explicit NumericKey(std::string string)
        : m_value(std::move(string))
    {
        // Make sure the string is a valid number.
        (void)number();
    }

    auto operator==(const NumericKey &rhs) const -> bool
    {
        return m_value == rhs.m_value;
    }

    auto operator!=(const NumericKey &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    [[nodiscard]] auto number() const -> U64
    {
        U64 value;
        Slice slice(m_value);
        consume_decimal_number(slice, &value);
        return value;
    }

    [[nodiscard]] auto string() const -> const std::string &
    {
        return m_value;
    }

    auto operator++() -> NumericKey &
    {
        m_value = integral_key<Length>(number() + 1);
        return *this;
    }

    auto operator--() -> NumericKey &
    {
        CALICODB_EXPECT_GT(number(), 0);
        m_value = integral_key<Length>(number() - 1);
        return *this;
    }

    auto operator++(int) -> NumericKey
    {
        auto prev = *this;
        ++*this;
        return prev;
    }

    auto operator--(int) -> NumericKey
    {
        auto prev = *this;
        --*this;
        return prev;
    }
};

} // namespace calicodb::tools

#endif // CALICODB_TOOLS_H