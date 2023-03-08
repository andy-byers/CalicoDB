
#include "tools.h"
#include "env_posix.h"
#include "types.h"
#include <algorithm>
#include <iomanip>
#include <iostream>

namespace calicodb::tools
{

#define TRY_INTERCEPT_FROM(source, type, path)                                                     \
    do {                                                                                           \
        if (auto intercept_s = (source).try_intercept_syscall(type, path); !intercept_s.is_ok()) { \
            return intercept_s;                                                                    \
        }                                                                                          \
    } while (0)

namespace fs = std::filesystem;

auto FakeEnv::read_file_at(const Memory &memory, char *data_out, std::size_t &size_out, std::size_t offset) -> Status
{
    std::size_t read_size {};
    if (Slice buffer {memory.buffer}; offset < memory.buffer.size()) {
        read_size = std::min(size_out, buffer.size() - offset);
        std::memcpy(data_out, buffer.advance(offset).data(), read_size);
    }
    size_out = read_size;
    return Status::ok();
}

auto FakeEnv::write_file_at(Memory &memory, Slice in, std::size_t offset) -> Status
{
    if (const auto write_end = offset + in.size(); memory.buffer.size() < write_end) {
        memory.buffer.resize(write_end);
    }
    std::memcpy(memory.buffer.data() + offset, in.data(), in.size());
    return Status::ok();
}

auto FakeReader::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    return m_parent->read_file_at(*m_mem, out, *size, offset);
}

auto FakeEditor::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    return m_parent->read_file_at(*m_mem, out, *size, offset);
}

auto FakeEditor::write(Slice in, std::size_t offset) -> Status
{
    return m_parent->write_file_at(*m_mem, in, offset);
}

auto FakeEditor::sync() -> Status
{
    return Status::ok();
}

auto FakeLogger::write(Slice in) -> Status
{
    return m_parent->write_file_at(*m_mem, in, m_mem->buffer.size());
}

auto FakeLogger::sync() -> Status
{
    return Status::ok();
}

auto FakeEnv::get_memory(const std::string &path) const -> Memory &
{
    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        itr = m_memory.insert(itr, {path, {}});
    }
    return itr->second;
}

auto FakeEnv::new_reader(const std::string &path, Reader **out) -> Status
{
    auto &mem = get_memory(path);
    if (mem.created) {
        *out = new FakeReader {path, *this, mem};
        return Status::ok();
    }
    return Status::not_found("cannot put file");
}

auto FakeEnv::new_editor(const std::string &path, Editor **out) -> Status
{
    auto &mem = get_memory(path);
    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    *out = new FakeEditor {path, *this, mem};
    return Status::ok();
}

auto FakeEnv::new_logger(const std::string &path, Logger **out) -> Status
{
    auto &mem = get_memory(path);
    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    *out = new FakeLogger {path, *this, mem};
    return Status::ok();
}

auto FakeEnv::new_info_logger(const std::string &, InfoLogger **out) -> Status
{
    *out = new FakeInfoLogger;
    return Status::ok();
}

auto FakeEnv::remove_file(const std::string &path) -> Status
{
    auto &mem = get_memory(path);
    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        return Status::not_found("cannot remove file");
    }
    // Don't actually get rid of any memory. We should be able to unlink a file and still access it
    // through open file descriptors, so if any readers or writers have this file put, they should
    // still be able to use it.
    itr->second.created = false;
    return Status::ok();
}

auto FakeEnv::resize_file(const std::string &path, std::size_t size) -> Status
{
    auto itr = m_memory.find(path);
    if (itr == end(m_memory)) {
        return Status::system_error("cannot resize file");
    }
    itr->second.buffer.resize(size);
    return Status::ok();
}

auto FakeEnv::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    if (new_path.empty()) {
        return Status::invalid_argument("name has zero length");
    }

    auto node = m_memory.extract(old_path);
    if (node.empty()) {
        return Status::not_found("file does not exist");
    }

    node.key() = new_path;
    m_memory.insert(std::move(node));
    return Status::ok();
}

auto FakeEnv::file_size(const std::string &path, std::size_t *out) const -> Status
{
    auto itr = m_memory.find(path);
    if (itr == cend(m_memory)) {
        return Status::not_found("file does not exist");
    }
    *out = itr->second.buffer.size();
    return Status::ok();
}

auto FakeEnv::file_exists(const std::string &path) const -> Status
{
    if (const auto &itr = m_memory.find(path); itr != end(m_memory)) {
        if (itr->second.created) {
            return Status::ok();
        }
    }
    return Status::not_found("file does not exist");
}

auto FakeEnv::get_children(const std::string &path, std::vector<std::string> *out) const -> Status
{
    auto prefix = path.back() == '/' ? path : path + '/';
    for (const auto &[filename, mem] : m_memory) {
        if (mem.created && Slice {filename}.starts_with(prefix)) {
            out->emplace_back(filename.substr(prefix.size()));
        }
    }
    return Status::ok();
}

auto FakeEnv::clone() const -> Env *
{
    auto *env = new FaultInjectionEnv;
    env->m_memory = m_memory;
    return env;
}

auto FaultInjectionEnv::try_intercept_syscall(Interceptor::Type type, const std::string &path) -> Status
{
    Slice filename {path};
    for (const auto &interceptor : m_interceptors) {
        if (interceptor.type == type && filename.starts_with(interceptor.prefix)) {
            CDB_TRY(interceptor());
        }
    }
    return Status::ok();
}
auto FaultInjectionEnv::add_interceptor(Interceptor interceptor) -> void
{
    m_interceptors.emplace_back(std::move(interceptor));
}

auto FaultInjectionEnv::clear_interceptors() -> void
{
    m_interceptors.clear();
}

auto FaultInjectionReader::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    {
        TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kRead, m_path);
    }
    return FakeReader::read(out, size, offset);
}

auto FaultInjectionEditor::read(char *out, std::size_t *size, std::size_t offset) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kRead, m_path);
    return FakeEditor::read(out, size, offset);
}

auto FaultInjectionEditor::write(Slice in, std::size_t offset) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kWrite, m_path);
    return FakeEditor::write(in, offset);
}

auto FaultInjectionEditor::sync() -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kSync, m_path);
    return FakeEditor::sync();
}

auto FaultInjectionLogger::write(Slice in) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kWrite, m_path);
    return FakeLogger::write(in);
}

auto FaultInjectionLogger::sync() -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kSync, m_path);
    return FakeLogger::sync();
}

auto FaultInjectionEnv::new_reader(const std::string &path, Reader **out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeReader *reader;
    CDB_TRY(FakeEnv::new_reader(path, reinterpret_cast<Reader **>(&reader)));
    *out = new FaultInjectionReader {*reader};
    delete reader;
    return Status::ok();
}

auto FaultInjectionEnv::new_editor(const std::string &path, Editor **out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeEditor *editor;
    CDB_TRY(FakeEnv::new_editor(path, reinterpret_cast<Editor **>(&editor)));
    *out = new FaultInjectionEditor {*editor};
    delete editor;
    return Status::ok();
}

auto FaultInjectionEnv::new_logger(const std::string &path, Logger **out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeLogger *logger;
    CDB_TRY(FakeEnv::new_logger(path, reinterpret_cast<Logger **>(&logger)));
    *out = new FaultInjectionLogger {*logger};
    delete logger;
    return Status::ok();
}

auto FaultInjectionEnv::new_info_logger(const std::string &, InfoLogger **out) -> Status
{
    *out = new FaultInjectionInfoLogger;
    return Status::ok();
}

auto FaultInjectionEnv::remove_file(const std::string &path) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kUnlink, path);
    return FakeEnv::remove_file(path);
}

auto FaultInjectionEnv::resize_file(const std::string &path, std::size_t size) -> Status
{
    return FakeEnv::resize_file(path, size);
}

auto FaultInjectionEnv::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kRename, old_path);
    return FakeEnv::rename_file(old_path, new_path);
}

auto FaultInjectionEnv::file_size(const std::string &path, std::size_t *out) const -> Status
{
//    TRY_INTERCEPT_FROM(*this, Interceptor::FileSize, path);
    return FakeEnv::file_size(path, out);
}

auto FaultInjectionEnv::file_exists(const std::string &path) const -> Status
{
//    TRY_INTERCEPT_FROM(*this, Interceptor::Exists, path);
    return FakeEnv::file_exists(path);
}

auto FaultInjectionEnv::get_children(const std::string &dir_path, std::vector<std::string> *out) const -> Status
{
    return FakeEnv::get_children(dir_path, out);
}

auto FaultInjectionEnv::clone() const -> Env *
{
    auto *env = new FaultInjectionEnv;
    env->m_memory = m_memory;
    env->m_interceptors = m_interceptors;
    return env;
}

RandomGenerator::RandomGenerator(std::size_t size)
    : m_data(size, '\0'),
      m_rng {42}
{
    std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine {m_rng};
    std::generate(begin(m_data), end(m_data), std::ref(engine));
}

auto RandomGenerator::Generate(std::size_t len) const -> Slice
{
    if (m_pos + len > m_data.size()) {
        m_pos = 0;
        CDB_EXPECT_LT(len, m_data.size());
    }
    m_pos += len;
    return {m_data.data() + m_pos - len, static_cast<std::size_t>(len)};
}

auto print_references(Pager &pager) -> void
{
    for (auto pid = Id::root(); pid.value <= pager.page_count(); ++pid.value) {
        std::cerr << std::setw(6) << pid.value << ": ";
        if (PointerMap::lookup(pager, pid) == pid) {
            std::cerr << "pointer map\n";
            continue;
        }
        if (pid.is_root()) {
            std::cerr << "node -> NULL\n";
            continue;
        }
        PointerMap::Entry entry;
        CHECK_OK(PointerMap::read_entry(pager, pid, &entry));
        switch (entry.type) {
        case PointerMap::kTreeNode:
            std::cerr << "node";
            break;
        case PointerMap::kTreeRoot:
            break;
        case PointerMap::kFreelistLink:
            std::cerr << "freelist link";
            break;
        case PointerMap::kOverflowHead:
            std::cerr << "overflow head";
            break;
        case PointerMap::kOverflowLink:
            std::cerr << "overflow link";
            break;
        }
        if (entry.type == PointerMap::kTreeRoot) {
            std::cerr << "root for table " << entry.back_ptr.value << '\n';
        } else {
            std::cerr << " -> " << entry.back_ptr.value << '\n';
        }
    }
}

#undef TRY_INTERCEPT_FROM

} // namespace calicodb::Tools
