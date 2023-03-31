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

#define CHECK_OK(expr)                                        \
    do {                                                      \
        if (auto assert_s = (expr); !assert_s.is_ok()) {      \
            std::fputs(assert_s.to_string().c_str(), stderr); \
            std::abort();                                     \
        }                                                     \
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
    struct Memory {
        std::string buffer;
        bool created = false;
    };

    [[nodiscard]] virtual auto memory() -> std::unordered_map<std::string, Memory> &
    {
        return m_memory;
    }

    [[nodiscard]] virtual auto memory() const -> const std::unordered_map<std::string, Memory> &
    {
        return m_memory;
    }

    [[nodiscard]] virtual auto clone() const -> Env *;

    ~FakeEnv() override = default;
    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto sync_directory(const std::string &dirname) -> Status override { return Status::ok(); }

protected:
    friend class FakeFile;
    friend class FakeLogFile;

    [[nodiscard]] auto get_memory(const std::string &filename) const -> Memory &;
    [[nodiscard]] auto read_file_at(const Memory &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status;
    [[nodiscard]] auto write_file_at(Memory &mem, std::size_t offset, const Slice &in) -> Status;

    mutable std::unordered_map<std::string, Memory> m_memory;
};

class FakeFile : public File
{
public:
    FakeFile(std::string filename, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_filename {std::move(filename)}
    {
    }

    ~FakeFile() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem = nullptr;
    FakeEnv *m_parent = nullptr;
    std::string m_filename;
};

class FakeLogFile : public LogFile
{
public:
    ~FakeLogFile() override = default;
    auto logv(const char *, ...) -> void override {}
};

struct Interceptor {
    enum Type {
        kRead,
        kWrite,
        kOpen,
        kSync,
        kUnlink,
        kResize,
        kRename,
        kTypeCount
    };

    using Callback = std::function<Status()>;

    explicit Interceptor(std::string prefix_, Type type_, Callback callback_)
        : prefix {std::move(prefix_)},
          callback {std::move(callback_)},
          type {type_}
    {
    }

    [[nodiscard]] auto operator()() const -> Status
    {
        return callback();
    }

    std::string prefix;
    Callback callback;
    Type type;
};

class FaultInjectionEnv : public FakeEnv
{
    std::vector<Interceptor> m_interceptors;

    friend class FaultInjectionFile;
    friend class FaultInjectionLogFile;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status;

public:
    [[nodiscard]] auto clone() const -> Env * override;
    virtual auto add_interceptor(Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;

    ~FaultInjectionEnv() override = default;
    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto new_log_file(const std::string &filename, LogFile *&out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto sync_directory(const std::string &dirname) -> Status override { return Status::ok(); }
};

class FaultInjectionFile : public FakeFile
{
public:
    explicit FaultInjectionFile(File &file)
        : FakeFile {reinterpret_cast<FakeFile &>(file)}
    {
    }

    ~FaultInjectionFile() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class FaultInjectionLogFile : public FakeLogFile
{
public:
    ~FaultInjectionLogFile() override = default;
    auto logv(const char *, ...) -> void override {}
};

class StderrLog : public LogFile
{
public:
    ~StderrLog() override = default;

    auto logv(const char *fmt, ...) -> void override
    {
        std::va_list args;
        va_start(args, fmt);
        std::vfprintf(stderr, fmt, args);
        std::fputs("\n", stderr);
        va_end(args);
        std::fflush(stderr);
    }
};

class WalStub : public Wal
{
public:
    ~WalStub() override = default;

    [[nodiscard]] auto read(Id, char *) -> Status override
    {
        return Status::not_found("");
    }

    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto checkpoint(File &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto commit() -> Status override
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
    struct PageVersion {
        Id page_id;
        std::size_t db_size;
        std::string data;
    };
    std::vector<std::vector<PageVersion>> m_versions;
    Parameters m_param;

public:
    explicit FakeWal(const Parameters &param);
    ~FakeWal() override = default;

    [[nodiscard]] auto read(Id page_id, char *out) -> Status override;
    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto checkpoint(File &db_file) -> Status override;
    [[nodiscard]] auto commit() -> Status override;
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

inline auto expect_non_error(const Status &s)
{
    if (!s.is_ok() && !s.is_not_found()) {
        std::fprintf(stderr, "error: %s\n", s.to_string().data());
        std::abort();
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
    explicit RandomGenerator(std::size_t size = 16 /* KiB */ * 1'024);
    auto Generate(std::size_t len) const -> Slice;

    // Not in LevelDB.
    auto Next(U64 t_max) const -> U64
    {
        std::uniform_int_distribution<U64> dist {0, t_max};
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(U64 t_min, U64 t_max) const -> U64
    {
        std::uniform_int_distribution<U64> dist {t_min, t_max};
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
auto fill_db(DB &db, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto fill_db(DB &db, Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size = 100) -> std::map<std::string, std::string>;
auto expect_db_contains(const DB &db, const std::map<std::string, std::string> &map) -> void;
auto expect_db_contains(const DB &db, const Table &table, const std::map<std::string, std::string> &map) -> void;

} // namespace calicodb::tools

#endif // CALICODB_TOOLS_H