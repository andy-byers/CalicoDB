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

namespace Calico {

using ReadInterceptor = std::function<Status(const std::string&, Span &, Size)>;
using WriteInterceptor = std::function<Status(const std::string&, Slice, Size)>;
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
    if (!s.is_system_error() || s.what().to_string() != "42") {
        fmt::print(stderr, "error: unexpected {} status: {}", get_status_name(s), s.is_ok() ? "NULL" : s.what().to_string());
        std::exit(EXIT_FAILURE);
    }
}

class RandomHeapReader : public RandomReader {
public:
    RandomHeapReader(std::string name, std::string &file)
        : m_path {std::move(name)},
          m_file {&file}
    {}

    ~RandomHeapReader() override = default;
    [[nodiscard]] auto read(Byte *, Size &, Size) -> Status override;

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
    [[nodiscard]] auto read(Byte *, Size &, Size) -> Status override;
    [[nodiscard]] auto write(Slice, Size) -> Status override;
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
    [[nodiscard]] auto write(Slice) -> Status override;
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

private:
    mutable std::mutex m_mutex;
    // TODO: Could use a custom allocator that is better for large contiguous chunks.
    std::unordered_map<std::string, std::string> m_files;
    std::unordered_set<std::string> m_directories;
};


template<Size Delay = 0>
struct FailOnce {
    explicit FailOnce(std::string filter_prefix = {})
        : prefix {std::move(filter_prefix)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!prefix.empty() && Slice {path}.starts_with(prefix)) {
            if (index++ == Delay)
                return error;
        }
        return ok();
    }

    std::string prefix;
    Status error {system_error("42")};
    Size index {};
};

template<Size Delay = 0>
struct FailAfter {
    explicit FailAfter(std::string filter_prefix = {})
        : prefix {std::move(filter_prefix)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!prefix.empty()) {
            if (Slice {path}.starts_with(prefix) && index++ >= Delay)
                return error;
        }
        return ok();
    }

    std::string prefix;
    Status error {system_error("42")};
    Size index {};
};

template<Size Delay>
struct FailEvery {
    explicit FailEvery(std::string filter_prefix = {})
        : prefix {std::move(filter_prefix)}
    {}

    auto operator()(const std::string &path, ...) -> Status
    {
        if (!prefix.empty()) {
            if (Slice {path}.starts_with(prefix) && index++ == Delay) {
                index = 0;
                return error;
            }
        }
        return ok();
    }

    std::string prefix;
    Status error {system_error("42")};
    Size index {};
};

using Outcome = unsigned;

struct RepeatPattern {
    auto operator()(Size) -> Size
    {
        return 0;
    }
};

struct RepeatFinalOutcome {
    auto operator()(Size index) -> Size
    {
        return index - 1;
    }
};

template<class Reset = RepeatFinalOutcome>
class SystemCallOutcomes {
public:
    explicit SystemCallOutcomes(std::string filter_prefix, std::vector<Outcome> pattern = {1})
        : m_prefix {std::move(filter_prefix)},
          m_pattern {std::move(pattern)}
    {
        CALICO_EXPECT_FALSE(m_pattern.empty());
    }

    [[nodiscard]]
    auto is_failure(const Status &s) -> bool
    {
        return get_status_name(s) == get_status_name(m_error) && s.what() == m_error.what();
    }

    [[nodiscard]]
    auto operator()(const std::string &path, ...) -> Status
    {
        if (Slice {path}.starts_with(m_prefix)) {
            auto status = m_pattern[m_index++] == 0
                ? m_error : ok();

            if (m_index == m_pattern.size())
                m_index = Reset {}(m_index);

            return status;
        }
        return ok();
    }

private:
    std::string m_prefix;
    std::vector<Outcome> m_pattern;
    Status m_error {system_error("42")};
    Size m_index {};
};


class DisabledWriteAheadLog: public WriteAheadLog {
public:
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]]
    auto flushed_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto current_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto bytes_written() const -> Size override
    {
        return 0;
    }


    auto log(WalPayloadIn) -> void override
    {

    }

    auto flush() -> void override
    {

    }

    auto advance() -> void override
    {

    }

    [[nodiscard]]
    auto roll_forward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto roll_backward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    auto cleanup(Id) -> void override
    {

    }

    [[nodiscard]] auto start_workers() -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto truncate(Id) -> Status override
    {
        return ok();
    }
};

} // namespace Calico

#endif // CALICO_TEST_TOOLS_FAKES_H
