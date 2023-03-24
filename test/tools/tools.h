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
        bool created {};
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
    [[nodiscard]] auto new_info_logger(const std::string &filename, InfoLogger *&out) -> Status override;
    [[nodiscard]] auto new_reader(const std::string &filename, Reader *&out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &filename, Editor *&out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &filename, Logger *&out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto sync_directory(const std::string &dirname) -> Status override { return Status::ok(); }

protected:
    friend class FakeEditor;
    friend class FakeReader;
    friend class FakeLogger;

    [[nodiscard]] auto get_memory(const std::string &filename) const -> Memory &;
    [[nodiscard]] auto read_file_at(const Memory &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status;
    [[nodiscard]] auto write_file_at(Memory &mem, std::size_t offset, const Slice &in) -> Status;

    mutable std::unordered_map<std::string, Memory> m_memory;
};

class FakeReader : public Reader
{
public:
    FakeReader(std::string filename, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_filename {std::move(filename)}
    {
    }

    ~FakeReader() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_filename;
};

class FakeEditor : public Editor
{
public:
    FakeEditor(std::string filename, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_filename {std::move(filename)}
    {
    }

    ~FakeEditor() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_filename;
};

class FakeLogger : public Logger
{
public:
    FakeLogger(std::string filename, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_filename {std::move(filename)}
    {
    }

    ~FakeLogger() override = default;
    [[nodiscard]] auto write(const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_filename;
};

class FakeInfoLogger : public InfoLogger
{
public:
    ~FakeInfoLogger() override = default;
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
    Callback callback {};
    Type type {};
};

class FaultInjectionEnv : public FakeEnv
{
    std::vector<Interceptor> m_interceptors;

    friend class FaultInjectionEditor;
    friend class FaultInjectionReader;
    friend class FaultInjectionLogger;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status;

public:
    [[nodiscard]] auto clone() const -> Env * override;
    virtual auto add_interceptor(Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;

    ~FaultInjectionEnv() override = default;
    [[nodiscard]] auto new_info_logger(const std::string &filename, InfoLogger *&out) -> Status override;
    [[nodiscard]] auto new_reader(const std::string &filename, Reader *&out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &filename, Editor *&out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &filename, Logger *&out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_filename, const std::string &new_filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto sync_directory(const std::string &dirname) -> Status override { return Status::ok(); }
};

class FaultInjectionReader : public FakeReader
{
public:
    explicit FaultInjectionReader(Reader &reader)
        : FakeReader {reinterpret_cast<FakeReader &>(reader)}
    {
    }

    ~FaultInjectionReader() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
};

class FaultInjectionEditor : public FakeEditor
{
public:
    explicit FaultInjectionEditor(Editor &editor)
        : FakeEditor {reinterpret_cast<FakeEditor &>(editor)}
    {
    }
    ~FaultInjectionEditor() override = default;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class FaultInjectionLogger : public FakeLogger
{
public:
    explicit FaultInjectionLogger(Logger &logger)
        : FakeLogger {reinterpret_cast<FakeLogger &>(logger)}
    {
    }
    ~FaultInjectionLogger() override = default;
    [[nodiscard]] auto write(const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class FaultInjectionInfoLogger : public InfoLogger
{
public:
    ~FaultInjectionInfoLogger() override = default;
    auto logv(const char *, ...) -> void override {}
};

class StderrLogger : public InfoLogger
{
public:
    ~StderrLogger() override = default;

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
    mutable std::size_t m_pos {};
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(std::size_t size = 16 /* KiB */ * 1'024);
    auto Generate(std::size_t len) const -> Slice;

    // Not in LevelDB.
    auto Next(std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist {0, t_max};
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(std::uint64_t t_min, std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist {t_min, t_max};
        return dist(m_rng);
    }
};

struct DatabaseCounts {
    std::size_t records {};
    std::size_t pages {};
    std::size_t updates {};
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