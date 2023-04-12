// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TOOLS_H
#define CALICODB_TOOLS_H

#include "calicodb/db.h"
#include "calicodb/env.h"
#include "db_impl.h"
#include <climits>
#include <cstdarg>
#include <cstdio>
#include <functional>
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

class FakeEnv : public Env
{
public:
    [[nodiscard]] virtual auto clone() const -> Env *;
    [[nodiscard]] virtual auto get_file_contents(const std::string &filename) const -> std::string;
    virtual auto put_file_contents(const std::string &filename, std::string contents) -> void;

    ~FakeEnv() override = default;
    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

protected:
    friend class FakeFile;
    friend class FakeLogFile;
    friend class TestEnv;

    struct FileState {
        std::string buffer;
        bool created = false;
    };

    [[nodiscard]] auto open_or_create_file(const std::string &filename) const -> FileState &;
    [[nodiscard]] auto read_file_at(const FileState &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status;
    [[nodiscard]] auto write_file_at(FileState &mem, std::size_t offset, const Slice &in) -> Status;

    mutable std::unordered_map<std::string, FileState> m_state;
};

class FakeFile : public File
{
public:
    FakeFile(std::string filename, FakeEnv &env, FakeEnv::FileState &mem)
        : m_state(&mem),
          m_env(&env),
          m_filename(std::move(filename))
    {
    }

    ~FakeFile() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

    [[nodiscard]] auto env() -> FakeEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto env() const -> const FakeEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto filename() -> const std::string &
    {
        return m_filename;
    }

protected:
    FakeEnv::FileState *m_state = nullptr;
    FakeEnv *m_env = nullptr;
    std::string m_filename;
};

class FakeLogFile : public LogFile
{
public:
    ~FakeLogFile() override = default;
    auto write(const Slice &) -> void override {}
};

struct Interceptor {
    enum Type {
        kRead,
        kWrite,
        kOpen,
        kSync,
        kUnlink,
        kResize,
        kTypeCount
    };

    using Callback = std::function<Status()>;

    explicit Interceptor(Type t, Callback c)
        : callback(std::move(c)),
          type(t)
    {
    }

    [[nodiscard]] auto operator()() const -> Status
    {
        return callback();
    }

    Callback callback;
    Type type;
};

class TestEnv : public EnvWrapper
{
public:
    explicit TestEnv();
    explicit TestEnv(Env &env);
    ~TestEnv() override;

    // NOTE: clone() always clones files into a FakeEnv, and only works properly if
    //       the wrapped Env was empty when passed to the constructor.
    [[nodiscard]] virtual auto clone() -> Env *;

    // The TestFile wrapper reads the whole file and saves it in memory after a
    // successful call to sync(). When a TestFile is closed, .
    virtual auto drop_after_last_sync(const std::string &filename) -> void;
    virtual auto drop_after_last_sync() -> void;

    virtual auto add_interceptor(const std::string &filename, Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;
    virtual auto clear_interceptors(const std::string &filename) -> void;

    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t file_size) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

private:
    friend class TestFile;

    struct FileState {
        std::vector<Interceptor> interceptors;
        std::string saved_state;
        bool unlinked = false;
    };

    mutable std::unordered_map<std::string, FileState> m_state;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status;
    auto save_file_contents(const std::string &filename) -> void;
    auto overwrite_file(const std::string &filename, const std::string &contents) -> void;
};

class TestFile : public File
{
    friend class TestEnv;

    std::string m_filename;
    TestEnv *m_env = nullptr;
    File *m_file = nullptr;

    explicit TestFile(std::string filename, File &file, TestEnv &env);

public:
    ~TestFile() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class TestLogFile : public LogFile
{
public:
    ~TestLogFile() override = default;
    auto write(const Slice &) -> void override {}
};

class StderrLog : public LogFile
{
public:
    ~StderrLog() override = default;

    auto write(const Slice &in) -> void override
    {
        std::fputs(in.to_string().c_str(), stderr);
        if (in.data()[in.size() - 1] != '\n') {
            std::fputc('\n', stderr);
        }
        std::fflush(stderr);
    }
};

class WalStub : public Wal
{
public:
    ~WalStub() override = default;

    [[nodiscard]] auto read(Id, char *&) -> Status override
    {
        return Status::not_found("");
    }

    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto needs_checkpoint() const -> bool override
    {
        return false;
    }

    [[nodiscard]] auto checkpoint(File &, std::size_t *) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto abort() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto close() -> Status override
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
    Parameters m_param;

public:
    explicit FakeWal(const Parameters &param);
    ~FakeWal() override = default;

    [[nodiscard]] auto read(Id page_id, char *&out) -> Status override;
    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto needs_checkpoint() const -> bool override;
    [[nodiscard]] auto checkpoint(File &db_file, std::size_t *) -> Status override;
    [[nodiscard]] auto abort() -> Status override;
    [[nodiscard]] auto close() -> Status override;
    [[nodiscard]] auto sync() -> Status override { return Status::ok(); }
    [[nodiscard]] auto statistics() const -> WalStatistics override;
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

inline auto validate_db(const DB &db)
{
    reinterpret_cast<const DBImpl &>(db).TEST_validate();
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

auto read_file_to_string(Env &env, const std::string &filename) -> std::string;
auto write_string_to_file(Env &env, const std::string &filename, std::string buffer, long offset = -1) -> void;
auto fill_db(DB &db, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(DB &db, Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto expect_db_contains(const DB &db, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(const DB &db, const Table &table, const std::map<std::string, std::string> &map) -> void;

} // namespace calicodb::tools

#endif // CALICODB_TOOLS_H