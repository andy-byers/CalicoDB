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

protected:
    friend class FakeFile;
    friend class FakeLogFile;
    friend class FaultInjectionEnv;

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

    [[nodiscard]] auto parent() -> FakeEnv &
    {
        return *m_parent;
    }

    [[nodiscard]] auto parent() const -> const FakeEnv &
    {
        return *m_parent;
    }

    [[nodiscard]] auto filename() -> const std::string &
    {
        return m_filename;
    }

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

class DataLossEnv : public EnvWrapper
{
    friend class DataLossFile;

    std::unordered_map<std::string, std::string> m_save_states;

    auto save_file_contents(const std::string &clean_filename) -> void;
    auto overwrite_file(const std::string &clean_filename, const std::string &contents) -> void;

public:
    explicit DataLossEnv()
        : EnvWrapper(*new tools::FakeEnv)
    {
    }

    explicit DataLossEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~DataLossEnv() override
    {
        delete target();
    }

    [[nodiscard]] auto new_file(const std::string &filename, File *&out) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    virtual auto drop_after_last_sync() -> void;
    virtual auto drop_after_last_sync(const std::string &filename) -> void;
};

class DataLossFile : public File
{
    std::string m_filename;
    DataLossEnv *m_env = nullptr;
    File *m_file = nullptr;

public:
    explicit DataLossFile(std::string filename, File &file, DataLossEnv &env)
        : m_filename(std::move(filename)),
          m_env(&env),
          m_file(&file)
    {
    }

    ~DataLossFile() override
    {
        delete m_file;
    }

    [[nodiscard]] auto parent() -> DataLossEnv *
    {
        return m_env;
    }

    [[nodiscard]] auto parent() const -> const DataLossEnv *
    {
        return m_env;
    }

    [[nodiscard]] auto filename() const -> const std::string &
    {
        return m_filename;
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
        m_env->save_file_contents(m_filename);
        return Status::ok();
    }
};

class FaultInjectionEnv : public DataLossEnv
{
    std::unordered_map<std::string, std::vector<Interceptor>> m_interceptors;

    friend class FaultInjectionFile;
    friend class FaultInjectionLogFile;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status;

public:
    [[nodiscard]] auto clone() const -> Env *;
    virtual auto add_interceptor(const std::string &filename, Interceptor interceptor) -> void;
    virtual auto clear_interceptors(const std::string &filename) -> void;
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
};

class FaultInjectionFile : public File
{
    friend class FaultInjectionEnv;

    DataLossFile *m_target = nullptr;

    [[nodiscard]] auto parent() const -> const FaultInjectionEnv &
    {
        return reinterpret_cast<const FaultInjectionEnv &>(*m_target->parent());
    }

    [[nodiscard]] auto parent() -> FaultInjectionEnv &
    {
        return reinterpret_cast<FaultInjectionEnv &>(*m_target->parent());
    }

    explicit FaultInjectionFile(DataLossFile *&file)
        : m_target(file)
    {
        // Takes ownership.
        file = nullptr;
    }

public:
    ~FaultInjectionFile() override;
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