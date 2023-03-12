
#ifndef CALICODB_TOOLS_H
#define CALICODB_TOOLS_H

#include "calicodb/calicodb.h"
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
    [[nodiscard]] auto new_info_logger(const std::string &path, InfoLogger **out) -> Status override;
    [[nodiscard]] auto new_reader(const std::string &path, Reader **out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &path, Editor **out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &path, Logger **out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> *out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &path, std::size_t *out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;

protected:
    friend class FakeEditor;
    friend class FakeReader;
    friend class FakeLogger;

    [[nodiscard]] auto get_memory(const std::string &path) const -> Memory &;
    [[nodiscard]] auto read_file_at(const Memory &mem, char *data_out, std::size_t &size_out, std::size_t offset) -> Status;
    [[nodiscard]] auto write_file_at(Memory &mem, Slice in, std::size_t offset) -> Status;

    mutable std::unordered_map<std::string, Memory> m_memory;
};

class FakeReader : public Reader
{
public:
    FakeReader(std::string path, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {
    }

    ~FakeReader() override = default;
    [[nodiscard]] auto read(char *out, std::size_t *size, std::size_t offset) -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_path;
};

class FakeEditor : public Editor
{
public:
    FakeEditor(std::string path, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {
    }

    ~FakeEditor() override = default;
    [[nodiscard]] auto read(char *out, std::size_t *size, std::size_t offset) -> Status override;
    [[nodiscard]] auto write(Slice in, std::size_t offset) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_path;
};

class FakeLogger : public Logger
{
public:
    FakeLogger(std::string path, FakeEnv &parent, FakeEnv::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {
    }

    ~FakeLogger() override = default;
    [[nodiscard]] auto write(Slice in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

protected:
    friend class FakeEnv;

    FakeEnv::Memory *m_mem {};
    FakeEnv *m_parent {};
    std::string m_path;
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
        kFileSize,
        kRename,
        kExists,
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

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &path) -> Status;

public:
    [[nodiscard]] auto clone() const -> Env * override;
    virtual auto add_interceptor(Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;

    ~FaultInjectionEnv() override = default;
    [[nodiscard]] auto new_info_logger(const std::string &path, InfoLogger **out) -> Status override;
    [[nodiscard]] auto new_reader(const std::string &path, Reader **out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &path, Editor **out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &path, Logger **out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> *out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &path, std::size_t *out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;
};

class FaultInjectionReader : public FakeReader
{
public:
    explicit FaultInjectionReader(Reader &reader)
        : FakeReader {reinterpret_cast<FakeReader &>(reader)}
    {
    }

    ~FaultInjectionReader() override = default;
    [[nodiscard]] auto read(char *out, std::size_t *size, std::size_t offset) -> Status override;
};

class FaultInjectionEditor : public FakeEditor
{
public:
    explicit FaultInjectionEditor(Editor &editor)
        : FakeEditor {reinterpret_cast<FakeEditor &>(editor)}
    {
    }
    ~FaultInjectionEditor() override = default;
    [[nodiscard]] auto read(char *out, std::size_t *size, std::size_t offset) -> Status override;
    [[nodiscard]] auto write(Slice in, std::size_t offset) -> Status override;
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
    [[nodiscard]] auto write(Slice in) -> Status override;
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
    explicit RandomGenerator(std::size_t size = 4 /* KiB */ * 1'024);
    auto Generate(std::size_t len) const -> Slice;

    // Not in LevelDB.
    template <class T>
    auto Next(T t_max) const -> T
    {
        std::uniform_int_distribution<T> dist {std::numeric_limits<T>::min(), t_max};
        return dist(m_rng);
    }

    // Not in LevelDB.
    template <class T>
    auto Next(T t_min, T t_max) const -> T
    {
        std::uniform_int_distribution<T> dist {t_min, t_max};
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

struct DatabaseStats {
    double cache_hit_ratio {};
    std::size_t data_throughput {};
    std::size_t pager_throughput {};
    std::size_t wal_throughput {};
};

[[nodiscard]] inline auto parse_db_stats(std::string prop) -> DatabaseStats
{
    DatabaseStats stats;

    CHECK_EQ(prop.find("cache_hit_ratio:"), 0);
    prop = prop.substr(16);
    auto pos = prop.find(',');
    CHECK_TRUE(pos != std::string::npos);
    stats.cache_hit_ratio = std::stod(prop.substr(0, pos));
    prop = prop.substr(pos);

    CHECK_EQ(prop.find(",data_throughput:"), 0);
    prop = prop.substr(17);
    pos = prop.find(',');
    CHECK_TRUE(pos != std::string::npos);
    stats.data_throughput = std::stoi(prop.substr(0, pos));
    prop = prop.substr(pos);

    CHECK_EQ(prop.find(",pager_throughput:"), 0);
    prop = prop.substr(18);
    pos = prop.find(',');
    CHECK_TRUE(pos != std::string::npos);
    stats.pager_throughput = std::stoi(prop.substr(0, pos));
    prop = prop.substr(pos);

    CHECK_EQ(prop.find(",wal_throughput:"), 0);
    prop = prop.substr(16);
    pos = prop.find(',');
    CHECK_EQ(pos, std::string::npos);
    stats.wal_throughput = std::stoi(prop);

    return stats;
}

auto print_references(Pager &pager) -> void;
auto print_wals(Env &env, std::size_t page_size, const std::string &prefix) -> void;

} // namespace calicodb::Tools

#endif // CALICODB_TOOLS_H