#ifndef CALICO_TEST_TOOLS_FAKES_H
#define CALICO_TEST_TOOLS_FAKES_H

#include "calico/calico.h"
#include "calico/storage.h"
#include "pager/pager.h"
#include "random.h"
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace calico {

using ReadInterceptor = std::function<Status(const std::string&, Bytes&, Size)>;
using WriteInterceptor = std::function<Status(const std::string&, BytesView, Size)>;
using OpenInterceptor = std::function<Status(const std::string&)>;
using SyncInterceptor = std::function<Status(const std::string&)>;

namespace interceptors {
    auto set_read(ReadInterceptor callback) -> void;
    auto set_write(WriteInterceptor callback) -> void;
    auto set_open(OpenInterceptor callback) -> void;
    auto set_sync(SyncInterceptor callback) -> void;

    [[nodiscard]] auto get_read() -> ReadInterceptor;
    [[nodiscard]] auto get_write() -> WriteInterceptor;
    [[nodiscard]] auto get_open() -> OpenInterceptor;
    [[nodiscard]] auto get_sync() -> SyncInterceptor;

    auto reset() -> void;
} // namespace interceptors

inline auto assert_error_42(const Status &s)
{
    CALICO_EXPECT_TRUE(s.is_system_error() and s.what() == "42");
}

class RandomHeapReader : public RandomReader {
public:
    RandomHeapReader(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapReader() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class RandomHeapEditor: public RandomEditor {
public:
    RandomHeapEditor(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapEditor() override = default;
    [[nodiscard]] auto read(Bytes&, Size) -> Status override;
    [[nodiscard]] auto write(BytesView, Size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class AppendHeapWriter: public AppendWriter {
public:
    AppendHeapWriter(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~AppendHeapWriter() override = default;
    [[nodiscard]] auto write(BytesView) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

private:
    std::string m_path;
    std::string *m_file {};
};

class HeapStorage: public Storage {
public:
    HeapStorage();
    ~HeapStorage() override = default;
    [[nodiscard]] auto create_directory(const std::string&) -> Status override;
    [[nodiscard]] auto remove_directory(const std::string&) -> Status override;
    [[nodiscard]] auto get_children(const std::string&, std::vector<std::string>&) const -> Status override;
    [[nodiscard]] auto open_random_reader(const std::string &, RandomReader**) -> Status override;
    [[nodiscard]] auto open_random_editor(const std::string &, RandomEditor**) -> Status override;
    [[nodiscard]] auto open_append_writer(const std::string &, AppendWriter**) -> Status override;
    [[nodiscard]] auto rename_file(const std::string &, const std::string &) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &, Size) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &) const -> Status override;
    [[nodiscard]] auto file_size(const std::string &, Size &) const -> Status override;
    auto clone() const -> Storage*;

    ReadInterceptor read_interceptor;
    WriteInterceptor write_interceptor;
    OpenInterceptor open_interceptor;
    SyncInterceptor sync_interceptor;

private:
    mutable std::mutex m_mutex;
    // TODO: Could use a custom allocator that is better for large contiguous chunks.
    std::unordered_map<std::string, std::string> m_files;
    std::unordered_set<std::string> m_directories;
};


template<Size Delay = 0>
struct FailOnce {
    explicit FailOnce(std::string matcher_path = {})
        : matcher {std::move(matcher_path)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!matcher.empty() && stob(path).starts_with(matcher)) {
            if (index++ == Delay)
                return error;
        }
        return Status::ok();
    }

    std::string matcher;
    Status error {Status::system_error("42")};
    Size index {};
};

template<Size Delay = 0>
struct FailAfter {
    explicit FailAfter(std::string matcher_path = {})
        : matcher {std::move(matcher_path)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!matcher.empty()) {
            if (stob(path).starts_with(matcher) && index++ >= Delay)
                return error;
        }
        return Status::ok();
    }

    std::string matcher;
    Status error {Status::system_error("42")};
    Size index {};
};

template<Size Delay>
struct FailEvery {
    explicit FailEvery(std::string matcher_path = {})
        : matcher {std::move(matcher_path)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!matcher.empty()) {
            if (stob(path).starts_with(matcher) && index++ == Delay) {
                index = 0;
                return error;
            }
        }
        return Status::ok();
    }

    std::string matcher;
    Status error {Status::system_error("42")};
    Size index {};
};

} // namespace calico

#endif // CALICO_TEST_TOOLS_FAKES_H
