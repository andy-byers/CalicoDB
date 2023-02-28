
#ifndef CALICO_TOOLS_H
#define CALICO_TOOLS_H

#include "database/database_impl.h"
#include <calico/calico.h>
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

#define CHECK_OK(expr)                                         \
    do {                                                       \
        if (auto assert_s = (expr); !assert_s.is_ok()) {       \
            std::fputs(assert_s.to_string().c_str(), stderr);  \
            std::abort();                                      \
        }                                                      \
    } while (0)

#define CHECK_EQ(lhs, rhs)                        \
    do {                                          \
        if ((lhs) != (rhs)) {                     \
            std::fputs(#lhs " != " #rhs, stderr); \
            std::abort();                         \
        }                                         \
    } while (0)

namespace Calico::Tools {

struct Interceptor {
    enum Type {
        READ,
        WRITE,
        OPEN,
        SYNC,
        UNLINK,
        SIZE,
        RENAME,
        EXISTS,
        TYPE_COUNT
    };

    using Callback = std::function<Status()>;

    explicit Interceptor(std::string prefix_, Type type_, Callback callback_)
        : prefix {std::move(prefix_)},
          callback {std::move(callback_)},
          type {type_}
    {}

    [[nodiscard]] auto operator()() const -> Status
    {
        return callback();
    }

    std::string prefix;
    Callback callback {};
    Type type {};
};

class DynamicMemory : public Storage {

    struct Memory {
        std::string buffer;
        bool created {};
    };

    std::vector<Interceptor> m_interceptors;
    mutable std::unordered_map<std::string, Memory> m_memory;

    friend class MemoryEditor;
    friend class MemoryReader;
    friend class SequentialMemoryReader;
    friend class MemoryLogger;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &path) -> Status;
    [[nodiscard]] auto get_memory(const std::string &path) const -> Memory &;
    [[nodiscard]] auto read_file_at(const Memory &mem, Byte *data_out, Size &size_out, Size offset) -> Status;
    [[nodiscard]] auto write_file_at(Memory &mem, Slice in, Size offset) -> Status;

public:
    [[nodiscard]] auto memory() -> std::unordered_map<std::string, Memory> &
    {
        return m_memory;
    }
    [[nodiscard]] auto memory() const -> const std::unordered_map<std::string, Memory> &
    {
        return m_memory;
    }
    [[nodiscard]] auto clone() const -> Storage *;
    auto add_interceptor(Interceptor interceptor) -> void;
    auto clear_interceptors() -> void;

    ~DynamicMemory() override = default;
    [[nodiscard]] auto create_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto new_info_logger(const std::string &path, InfoLogger **out) -> Status override;
    [[nodiscard]] auto new_reader(const std::string &path, Reader **out) -> Status override;
    [[nodiscard]] auto new_editor(const std::string &path, Editor **out) -> Status override;
    [[nodiscard]] auto new_logger(const std::string &path, Logger **out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, Size size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &path, Size &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;
};

class MemoryReader : public Reader {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    MemoryReader(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~MemoryReader() override = default;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;
};

class MemoryEditor : public Editor {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    MemoryEditor(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~MemoryEditor() override = default;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;
    [[nodiscard]] auto write(Slice in, Size offset) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class MemoryLogger : public Logger {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    MemoryLogger(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~MemoryLogger() override = default;
    [[nodiscard]] auto write(Slice in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class MemoryInfoLogger : public InfoLogger {
public:
    ~MemoryInfoLogger() override = default;
    auto logv(const char *, ...) -> void override {}
};

class StderrLogger : public InfoLogger {
public:
    ~StderrLogger() override = default;

    auto logv(const char *fmt, ...) -> void override
    {
        std::va_list args;
        va_start(args, fmt);
        std::vfprintf(stderr, fmt, args);
        va_end(args);
        std::fflush(stderr);
    }
};

template<std::size_t Length = 12>
static auto integral_key(Size key) -> std::string
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

inline auto validate_db(const Database &db)
{
    reinterpret_cast<const DatabaseImpl &>(db).TEST_validate();
}

// Modified from LevelDB.
class RandomGenerator {
private:
    using Engine = std::default_random_engine;

    std::string m_data;
    mutable Size m_pos {};
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(Size size = 4 /* KiB */ * 1'024);
    auto Generate(Size len) const -> Slice;

    // Not in LevelDB.
    template<class T>
    auto Next(T t_max) const -> T
    {
        std::uniform_int_distribution<T> dist {std::numeric_limits<T>::min(), t_max};
        return dist(m_rng);
    }

    // Not in LevelDB.
    template<class T>
    auto Next(T t_min, T t_max) const -> T
    {
        std::uniform_int_distribution<T> dist {t_min, t_max};
        return dist(m_rng);
    }
};

struct DatabaseCounts {
    Size records {};
    Size pages {};
    Size updates {};
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
    Size data_throughput {};
    Size pager_throughput {};
    Size wal_throughput {};
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

auto print_references(Pager &pager, PointerMap &pointers) -> void;

} // namespace Calico::Tools

#endif // CALICO_TOOLS_H