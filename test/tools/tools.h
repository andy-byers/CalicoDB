
#ifndef CALICO_TOOLS_H
#define CALICO_TOOLS_H

#include <calico/calico.h>
#include <climits>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>


#define CHECK_TRUE(cond) \
    do { \
        if (!(cond)) {     \
            std::fputs(#cond " is false\n", stderr); \
            std::abort(); \
        } \
    } while (0)

#define CHECK_FALSE(cond) \
    CHECK_TRUE(!(cond))

#define CHECK_OK(expr) \
    do { \
        if (auto assert_s = (expr); !assert_s.is_ok()) { \
            std::fputs(assert_s.what().data(), stderr); \
            std::abort(); \
        } \
    } while (0)

#define CHECK_EQ(lhs, rhs) \
    do { \
        if ((lhs) != (rhs)) { \
            std::fputs(#lhs " != " #rhs, stderr); \
            std::abort(); \
        } \
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

    [[nodiscard]]
    auto operator()() const -> Status
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
    mutable std::mutex m_mutex;

    friend class RandomMemoryEditor;
    friend class RandomMemoryReader;
    friend class AppendMemoryWriter;

    [[nodiscard]] auto try_intercept_syscall(Interceptor::Type type, const std::string &path) -> Status;
    [[nodiscard]] auto get_memory(const std::string &path) const -> Memory &;
    [[nodiscard]] auto read_file_at(const Memory &mem, Byte *data_out, Size &size_out, Size offset) -> Status;
    [[nodiscard]] auto write_file_at(Memory &mem, Slice in, Size offset) -> Status;

public:
    [[nodiscard]] auto clone() const -> Storage *;
    auto add_interceptor(Interceptor interceptor) -> void;
    auto clear_interceptors() -> void;

    ~DynamicMemory() override = default;
    [[nodiscard]] auto create_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string &path) -> Status override;
    [[nodiscard]] auto open_random_reader(const std::string &path, RandomReader **out) -> Status override;
    [[nodiscard]] auto open_random_editor(const std::string &path, RandomEditor **out) -> Status override;
    [[nodiscard]] auto open_logger(const std::string &path, Logger **out) -> Status override;
    [[nodiscard]] auto get_children(const std::string &path, std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto rename_file(const std::string &old_path, const std::string &new_path) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &path) const -> Status override;
    [[nodiscard]] auto resize_file(const std::string &path, Size size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &path, Size &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &path) -> Status override;
};

class RandomMemoryReader : public RandomReader {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    RandomMemoryReader(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~RandomMemoryReader() override = default;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;
};

class RandomMemoryEditor : public RandomEditor {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    RandomMemoryEditor(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~RandomMemoryEditor() override = default;
    [[nodiscard]] auto read(Byte *out, Size &size, Size offset) -> Status override;
    [[nodiscard]] auto write(Slice in, Size offset) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
};

class AppendMemoryWriter : public Logger {
    DynamicMemory::Memory *m_mem {};
    DynamicMemory *m_parent {};
    std::string m_path;

    AppendMemoryWriter(std::string path, DynamicMemory &parent, DynamicMemory::Memory &mem)
        : m_mem {&mem},
          m_parent {&parent},
          m_path {std::move(path)}
    {}

    friend class DynamicMemory;

public:
    ~AppendMemoryWriter() override = default;
    [[nodiscard]] auto write(Slice in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
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

inline auto expect_ok(const Status &s)
{
    if (!s.is_ok()) {
        std::fprintf(stderr, "error: %s\n", s.what().data());
        std::abort();
    }
}

inline auto expect_non_error(const Status &s)
{
    if (!s.is_ok() && !s.is_not_found()) {
        std::fprintf(stderr, "error: %s\n", s.what().data());
        std::abort();
    }
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

} // namespace Calico::Tools

#endif // CALICO_TOOLS_H