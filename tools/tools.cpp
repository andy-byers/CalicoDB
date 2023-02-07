
#include "tools.h"
#include <algorithm>
#include "utils/types.h"

namespace Calico::Tools {

auto DynamicMemory::add_interceptor(Interceptor interceptor) -> void
{
    std::lock_guard lock {m_mutex};
    m_interceptors.emplace_back(std::move(interceptor));
}

auto DynamicMemory::clear_interceptors() -> void
{
    std::lock_guard lock {m_mutex};
    m_interceptors.clear();
}

#define Try_Intercept_From(source, type, path) \
    do { \
        if (auto intercept_s = (source).try_intercept_syscall(type, path); !intercept_s.is_ok()) { \
            return intercept_s; \
        } \
    } while (0)

namespace fs = std::filesystem;

auto DynamicMemory::read_file_at(const Memory &memory, Byte *data_out, Size &size_out, Size offset) -> Status
{
    Size read_size {};
    if (Slice buffer {memory.buffer}; offset < memory.buffer.size()) {
        read_size = std::min(size_out, buffer.size() - offset);
        std::memcpy(data_out, buffer.advance(offset).data(), read_size);
    }
    size_out = read_size;
    return ok();
}

auto DynamicMemory::write_file_at(Memory &memory, Slice in, Size offset) -> Status
{
    if (const auto write_end = offset + in.size(); memory.buffer.size() < write_end) {
        memory.buffer.resize(write_end);
    }
    std::memcpy(memory.buffer.data() + offset, in.data(), in.size());
    return ok();
}

auto RandomMemoryReader::read(Byte *out, Size &size, Size offset) -> Status
{
    {
        std::lock_guard lock {m_parent->m_mutex};
        Try_Intercept_From(*m_parent, Interceptor::READ, m_path);
    }
    return m_parent->read_file_at(*m_mem, out, size, offset);
}

auto RandomMemoryEditor::read(Byte *out, Size &size, Size offset) -> Status
{
    {
        std::lock_guard lock {m_parent->m_mutex};
        Try_Intercept_From(*m_parent, Interceptor::READ, m_path);
    }
    return m_parent->read_file_at(*m_mem, out, size, offset);
}

auto RandomMemoryEditor::write(Slice in, Size offset) -> Status
{
    {
        std::lock_guard lock {m_parent->m_mutex};
        Try_Intercept_From(*m_parent, Interceptor::WRITE, m_path);
    }
    return m_parent->write_file_at(*m_mem, in, offset);
}

auto RandomMemoryEditor::sync() -> Status
{
    std::lock_guard lock {m_parent->m_mutex};
    Try_Intercept_From(*m_parent, Interceptor::SYNC, m_path);
    return ok();
}

auto AppendMemoryWriter::write(Slice in) -> Status
{
    {
        std::lock_guard lock {m_parent->m_mutex};
        Try_Intercept_From(*m_parent, Interceptor::WRITE, m_path);
    }
    // TODO: Could produce incorrect results if the file size is changed after it is queried here. That really shouldn't ever happen
    //       in the library code, so it should be okay.
    return m_parent->write_file_at(*m_mem, in, m_mem->buffer.size());
}

auto AppendMemoryWriter::sync() -> Status
{
    std::lock_guard lock {m_parent->m_mutex};
    Try_Intercept_From(*m_parent, Interceptor::SYNC, m_path);
    return ok();
}

auto DynamicMemory::get_memory(const std::string &path) const -> Memory &
{
    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        itr = m_memory.insert(itr, {path, {}});
    }
    return itr->second;
}

auto DynamicMemory::create_directory(const std::string &) -> Status
{
    return Status::ok();
}

auto DynamicMemory::remove_directory(const std::string &) -> Status
{
    return Status::ok();
}

auto DynamicMemory::open_random_reader(const std::string &path, RandomReader **out) -> Status
{
    std::lock_guard lock {m_mutex};
    auto &mem = get_memory(path);
    Try_Intercept_From(*this, Interceptor::OPEN, path);
    
    if (mem.created) {
        *out = new RandomMemoryReader {path, *this, mem};
        return ok();
    }
    return not_found("cannot open file");
}

auto DynamicMemory::open_random_editor(const std::string &path, RandomEditor **out) -> Status
{
    std::lock_guard lock {m_mutex};
    auto &mem = get_memory(path);
    Try_Intercept_From(*this, Interceptor::OPEN, path);

    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    *out = new RandomMemoryEditor {path, *this, mem};
    return ok();
}

auto DynamicMemory::open_append_writer(const std::string &path, AppendWriter **out) -> Status
{
    std::lock_guard lock {m_mutex};
    auto &mem = get_memory(path);
    Try_Intercept_From(*this, Interceptor::OPEN, path);

    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    *out = new AppendMemoryWriter {path, *this, mem};
    return ok();
}

auto DynamicMemory::remove_file(const std::string &path) -> Status
{
    std::lock_guard lock {m_mutex};
    auto &mem = get_memory(path);
    Try_Intercept_From(*this, Interceptor::UNLINK, path);

    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        return not_found("cannot remove file");
    }
    // Don't actually get rid of any memory. We should be able to unlink a file and still access it
    // through open file descriptors, so if any readers or writers have this file open, they should
    // still be able to use it.
    itr->second.created = false;
    return ok();
}

auto DynamicMemory::resize_file(const std::string &path, Size size) -> Status
{
    std::lock_guard lock {m_mutex};
    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        return system_error("cannot resize file");
    }
    itr->second.buffer.resize(size);
    return ok();
}

auto DynamicMemory::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    if (new_path.empty()) {
        return system_error("could not rename file: new name has zero length");
    }

    std::lock_guard lock {m_mutex};
    auto node = m_memory.extract(old_path);
    if (node.empty()) {
        return system_error("cannot rename file: file \"{}\" does not exist", old_path);
    }

    node.key() = new_path;
    m_memory.insert(std::move(node));
    return ok();
}

auto DynamicMemory::file_size(const std::string &path, Size &out) const -> Status
{
    std::lock_guard lock {m_mutex};
    auto itr = m_memory.find(path);
    if (itr == cend(m_memory)) {
        return system_error("cannot get file size: file \"{}\" does not exist", path);
    }
    out = itr->second.buffer.size();
    return ok();
}

auto DynamicMemory::file_exists(const std::string &path) const -> Status
{
    std::lock_guard lock {m_mutex};
    if (const auto &mem = get_memory(path); mem.created) {
        return ok();
    }
    return not_found("cannot find file: file \"{}\" does not exist", path);
}

auto DynamicMemory::get_children(const std::string &dir_path, std::vector<std::string> &out) const -> Status
{
    auto prefix = dir_path;
    if (prefix.back() != '/') {
        prefix.append("/");
    }
    for (const auto &[path, mem]: m_memory) {
        if (mem.created && Slice {path}.starts_with(prefix)) {
            out.emplace_back(path.substr(prefix.size()));
        }
    }
    return ok();
}

auto DynamicMemory::clone() const -> Storage*
{
    auto *storage = new DynamicMemory;
    {
        std::lock_guard lock {m_mutex};
        storage->m_memory = m_memory;
    }
    return storage;
}

auto DynamicMemory::try_intercept_syscall(Interceptor::Type type, const std::string &path) -> Status
{
    Slice filename {path};
    for (const auto &interceptor: m_interceptors) {
        if (interceptor.type == type && filename.starts_with(interceptor.prefix)) {
            Calico_Try_S(interceptor());
        }
    }
    return Status::ok();
}

#undef Try_Intercept_Syscall

RandomGenerator::RandomGenerator(Size size)
    : m_data(size, '\0'),
      m_rng {42}
{
    std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine {m_rng};
    std::generate(begin(m_data), end(m_data), std::ref(engine));
}

auto RandomGenerator::Generate(Size len) const -> Slice
{
    if (m_pos + len > m_data.size()) {
        m_pos = 0;
        CALICO_EXPECT_LT(len, m_data.size());
    }
    m_pos += len;
    return {m_data.data() + m_pos - len, static_cast<Size>(len)};
}

} // namespace Calico::Tools
