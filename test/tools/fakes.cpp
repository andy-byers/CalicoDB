#include "fakes.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace calico {

/*
 * System call interceptors for fault injection during testing.
 */
namespace interceptors {
    std::mutex mutex;
    ReadInterceptor read;
    WriteInterceptor write;
    OpenInterceptor open;
    SyncInterceptor sync;

    auto set_read(ReadInterceptor callback) -> void
    {
        std::lock_guard lock {mutex};
        read = std::move(callback);
    }

    auto set_write(WriteInterceptor callback) -> void
    {
        std::lock_guard lock {mutex};
        write = std::move(callback);
    }

    auto set_open(OpenInterceptor callback) -> void
    {
        std::lock_guard lock {mutex};
        open = std::move(callback);
    }

    auto set_sync(SyncInterceptor callback) -> void
    {
        std::lock_guard lock {mutex};
        sync = std::move(callback);
    }

    auto get_read() -> ReadInterceptor
    {
        std::lock_guard lock {mutex};
        return read;
    }

    auto get_write() -> WriteInterceptor
    {
        std::lock_guard lock {mutex};
        return write;
    }

    auto get_open() -> OpenInterceptor
    {
        std::lock_guard lock {mutex};
        return open;
    }

    auto get_sync() -> SyncInterceptor
    {
        std::lock_guard lock {mutex};
        return sync;
    }

    auto reset() -> void
    {
        std::lock_guard lock {mutex};
        open = [](auto) {return Status::ok();};
        read = [](auto, auto, auto) {return Status::ok();};
        write = [](auto, auto, auto) {return Status::ok();};
        sync = [](auto) {return Status::ok();};
    }

} // namespace interceptors

#define INTERCEPT(expr) \
    do { \
        std::lock_guard intercept_lock {interceptors::mutex}; \
        auto intercept_status = (expr); \
        if (!intercept_status.is_ok()) \
            return intercept_status; \
    } while (0)

namespace fs = std::filesystem;

static auto read_file_at(const std::string &path, const std::string &file, Bytes &out, Size offset)
{
    INTERCEPT(interceptors::read(path, out, offset));

    Size r {};
    if (auto buffer = stob(file); offset < buffer.size()) {
        const auto diff = buffer.size() - offset;
        r = std::min(out.size(), diff);
        buffer.advance(offset);
        mem_copy(out, buffer, r);
    }
    out.truncate(r);
    return Status::ok();
}

static auto write_file_at(const std::string &path, std::string &file, BytesView in, Size offset)
{
    INTERCEPT(interceptors::write(path, in, offset));

    if (const auto write_end = offset + in.size(); file.size() < write_end)
        file.resize(write_end);
    mem_copy(stob(file).range(offset), in);
    return Status::ok();
}

static auto format_path(std::string path)
{
    CALICO_EXPECT_FALSE(path.empty());
    // TODO: Strip whitespace.
    if (path.back() == '/')
        path.erase(prev(cend(path)));
    return path;
}

auto RandomHeapReader::read(Bytes &out, Size offset) -> Status
{
    return read_file_at(m_path, *m_file, out, offset);
}

auto RandomHeapEditor::read(Bytes &out, Size offset) -> Status
{
    return read_file_at(m_path, *m_file, out, offset);
}

auto RandomHeapEditor::write(BytesView in, Size offset) -> Status
{
    return write_file_at(m_path, *m_file, in, offset);
}

auto RandomHeapEditor::sync() -> Status
{
    return interceptors::sync(m_path);
}

auto AppendHeapWriter::write(BytesView in) -> Status
{
    return write_file_at(m_path, *m_file, in, m_file->size());
}

auto AppendHeapWriter::sync() -> Status
{
    return interceptors::sync(m_path);
}

HeapStorage::HeapStorage()
{
    interceptors::reset();
}

auto HeapStorage::open_random_reader(const std::string &path, RandomReader **out) -> Status
{
    INTERCEPT(interceptors::open(path));
    std::lock_guard lock {m_mutex};

    if (auto itr = m_files.find(path); itr != end(m_files)) {
        *out = new RandomHeapReader {path, itr->second};
    } else {
        ThreePartMessage message;
        message.set_primary("could not open file");
        message.set_detail("file does not exist");
        message.set_hint("open a writer or editor to create the file");
        return message.not_found();
    }
    return Status::ok();
}

auto HeapStorage::open_random_editor(const std::string &path, RandomEditor **out) -> Status
{
    std::lock_guard lock {m_mutex};
    INTERCEPT(interceptors::open(path));

    if (auto itr = m_files.find(path); itr != end(m_files)) {
        *out = new RandomHeapEditor {path, itr->second};
    } else {
        auto [position, truthy] = m_files.emplace(path, std::string {});
        CALICO_EXPECT_TRUE(truthy);
        *out = new RandomHeapEditor {path, position->second};
    }
    return Status::ok();
}

auto HeapStorage::open_append_writer(const std::string &path, AppendWriter **out) -> Status
{
    INTERCEPT(interceptors::open(path));
    std::lock_guard lock {m_mutex};

    if (auto itr = m_files.find(path); itr != end(m_files)) {
        *out = new AppendHeapWriter {path, itr->second};
    } else {
        auto [position, truthy] = m_files.emplace(path, std::string {});
        CALICO_EXPECT_TRUE(truthy);
        *out = new AppendHeapWriter {path, position->second};
    }
    return Status::ok();
}

auto HeapStorage::remove_file(const std::string &name) -> Status
{
    std::lock_guard lock {m_mutex};
    auto itr = m_files.find(name);
    if (itr == end(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not remove file");
        message.set_detail("file does not exist");
        return message.system_error();
    }
    m_files.erase(itr);
    return Status::ok();
}

auto HeapStorage::resize_file(const std::string &name, Size size) -> Status
{
    std::lock_guard lock {m_mutex};
    auto itr = m_files.find(name);
    if (itr == end(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not resize file");
        message.set_detail("file does not exist");
        return message.system_error();
    }
    itr->second.resize(size);
    return Status::ok();
}

auto HeapStorage::rename_file(const std::string &old_name, const std::string &new_name) -> Status
{
    if (new_name.empty()) {
        ThreePartMessage message;
        message.set_primary("could not rename file");
        message.set_detail("new name is empty");
        return message.system_error();
    }
    std::lock_guard lock {m_mutex};
    auto node = m_files.extract(old_name);
    if (node.empty()) {
        ThreePartMessage message;
        message.set_primary("cannot rename file");
        message.set_detail("file \"{}\" does not exist", old_name);
        return message.system_error();
    }
    node.key() = new_name;
    m_files.insert(std::move(node));
    return Status::ok();
}

auto HeapStorage::file_size(const std::string &name, Size &out) const -> Status
{
    std::lock_guard lock {m_mutex};
    auto itr = m_files.find(name);
    if (itr == cend(m_files)) {
        ThreePartMessage message;
        message.set_primary("cannot get file size");
        message.set_detail("file \"{}\" does not exist", name);
        return message.system_error();
    }
    out = itr->second.size();
    return Status::ok();
}

auto HeapStorage::file_exists(const std::string &name) const -> Status
{
    std::lock_guard lock {m_mutex};
    const auto itr = m_files.find(name);
    if (itr == cend(m_files)) {
        ThreePartMessage message;
        message.set_primary("could not find file");
        message.set_detail("file \"{}\" does not exist", name);
        return message.not_found();
    }
    return Status::ok();
}

auto HeapStorage::get_children(const std::string &dir_path, std::vector<std::string> &out) const -> Status
{
    // TODO: This whole thing sucks and doesn't work! I'll figure it out later as it's surprisingly complicated to do correctly.
    //       Use std::filesystem::path to iterate through the paths. Or maybe even keep an auxiliary data structure, like a tree of some sort,
    //       to keep track of the directory structures, which can get arbitrarily complicated (but is not likely to in practice). For now just
    //       return all children, which works with the current design.

    std::lock_guard lock {m_mutex};
    auto itr = m_directories.find(format_path(dir_path));
    if (itr == cend(m_directories)) {
        ThreePartMessage message;
        message.set_primary("could not get children");
        message.set_detail("directory {} does not exist", dir_path);
        return message.system_error();
    }

    std::vector<std::string> all_files(m_files.size());
    std::transform(cbegin(m_files), cend(m_files), back_inserter(out), [](auto entry) {
        return entry.first;
    });
    return Status::ok();
}

auto HeapStorage::create_directory(const std::string &path) -> Status
{
    std::lock_guard lock {m_mutex};
    m_directories.insert(format_path(path));
    return Status::ok();
}

auto HeapStorage::remove_directory(const std::string &name) -> Status
{
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_NE(m_directories.find(name), cend(m_directories));
    m_directories.erase(name);
    return Status::ok();
}

auto HeapStorage::clone() const -> Storage*
{
    std::lock_guard lock {m_mutex};
    auto *store = new HeapStorage;
    store->m_files = m_files;
    store->m_directories = m_directories;
    return store;
}

#undef INTERCEPT

} // namespace calico
