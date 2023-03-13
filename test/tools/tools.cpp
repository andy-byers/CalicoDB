// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include "env_posix.h"
#include "logging.h"
#include "types.h"
#include "wal_reader.h"
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

auto FakeEnv::read_file_at(const Memory &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    std::size_t read_size {};
    if (Slice buffer {mem.buffer}; offset < mem.buffer.size()) {
        read_size = std::min(size, buffer.size() - offset);
        std::memcpy(scratch, buffer.advance(offset).data(), read_size);
    }
    if (out != nullptr) {
        *out = Slice {scratch, read_size};
    }
    return Status::ok();
}

auto FakeEnv::write_file_at(Memory &mem, std::size_t offset, const Slice &in) -> Status
{
    if (const auto write_end = offset + in.size(); mem.buffer.size() < write_end) {
        mem.buffer.resize(write_end);
    }
    std::memcpy(mem.buffer.data() + offset, in.data(), in.size());
    return Status::ok();
}

auto FakeReader::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    return m_parent->read_file_at(*m_mem, offset, size, scratch, out);
}

auto FakeEditor::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    return m_parent->read_file_at(*m_mem, offset, size, scratch, out);
}

auto FakeEditor::write(std::size_t offset, const Slice &in) -> Status
{
    return m_parent->write_file_at(*m_mem, offset, in);
}

auto FakeEditor::sync() -> Status
{
    return Status::ok();
}

auto FakeLogger::write(const Slice &in) -> Status
{
    return m_parent->write_file_at(*m_mem, m_mem->buffer.size(), in);
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

auto FakeEnv::new_reader(const std::string &path, Reader *&out) -> Status
{
    auto &mem = get_memory(path);
    if (mem.created) {
        out = new FakeReader {path, *this, mem};
        return Status::ok();
    }
    return Status::not_found("cannot open file");
}

auto FakeEnv::new_editor(const std::string &path, Editor *&out) -> Status
{
    auto &mem = get_memory(path);
    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    out = new FakeEditor {path, *this, mem};
    return Status::ok();
}

auto FakeEnv::new_logger(const std::string &path, Logger *&out) -> Status
{
    auto &mem = get_memory(path);
    if (!mem.created) {
        mem.buffer.clear();
        mem.created = true;
    }
    out = new FakeLogger {path, *this, mem};
    return Status::ok();
}

auto FakeEnv::new_info_logger(const std::string &, InfoLogger *&out) -> Status
{
    out = new FakeInfoLogger;
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
        return Status::io_error("cannot resize file");
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

auto FakeEnv::file_size(const std::string &path, std::size_t &out) const -> Status
{
    auto itr = m_memory.find(path);
    if (itr == cend(m_memory)) {
        return Status::not_found("file does not exist");
    }
    out = itr->second.buffer.size();
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

auto FakeEnv::get_children(const std::string &path, std::vector<std::string> &out) const -> Status
{
    auto prefix = path.back() == '/' ? path : path + '/';
    for (const auto &[filename, mem] : m_memory) {
        if (mem.created && Slice {filename}.starts_with(prefix)) {
            out.emplace_back(filename.substr(prefix.size()));
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

auto FaultInjectionReader::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    {
        TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kRead, m_path);
    }
    return FakeReader::read(offset, size, scratch, out);
}

auto FaultInjectionEditor::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kRead, m_path);
    return FakeEditor::read(offset, size, scratch, out);
}

auto FaultInjectionEditor::write(std::size_t offset, const Slice &in) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kWrite, m_path);
    return FakeEditor::write(offset, in);
}

auto FaultInjectionEditor::sync() -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kSync, m_path);
    return FakeEditor::sync();
}

auto FaultInjectionLogger::write(const Slice &in) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kWrite, m_path);
    return FakeLogger::write(in);
}

auto FaultInjectionLogger::sync() -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kSync, m_path);
    return FakeLogger::sync();
}

auto FaultInjectionEnv::new_reader(const std::string &path, Reader *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeReader *reader;
    CDB_TRY(FakeEnv::new_reader(path, reinterpret_cast<Reader *&>(reader)));
    out = new FaultInjectionReader {*reader};
    delete reader;
    return Status::ok();
}

auto FaultInjectionEnv::new_editor(const std::string &path, Editor *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeEditor *editor;
    CDB_TRY(FakeEnv::new_editor(path, reinterpret_cast<Editor *&>(editor)));
    out = new FaultInjectionEditor {*editor};
    delete editor;
    return Status::ok();
}

auto FaultInjectionEnv::new_logger(const std::string &path, Logger *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, path);
    FakeLogger *logger;
    CDB_TRY(FakeEnv::new_logger(path, reinterpret_cast<Logger *&>(logger)));
    out = new FaultInjectionLogger {*logger};
    delete logger;
    return Status::ok();
}

auto FaultInjectionEnv::new_info_logger(const std::string &, InfoLogger *&out) -> Status
{
    out = new FaultInjectionInfoLogger;
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

auto FaultInjectionEnv::file_size(const std::string &path, std::size_t &out) const -> Status
{
    //    TRY_INTERCEPT_FROM(*this, Interceptor::FileSize, path);
    return FakeEnv::file_size(path, out);
}

auto FaultInjectionEnv::file_exists(const std::string &path) const -> Status
{
    //    TRY_INTERCEPT_FROM(*this, Interceptor::Exists, path);
    return FakeEnv::file_exists(path);
}

auto FaultInjectionEnv::get_children(const std::string &dir_path, std::vector<std::string> &out) const -> Status
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
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        std::cerr << std::setw(6) << page_id.value << ": ";
        if (PointerMap::lookup(pager, page_id) == page_id) {
            std::cerr << "pointer map\n";
            continue;
        }
        if (page_id.is_root()) {
            std::cerr << "node -> NULL\n";
            continue;
        }
        PointerMap::Entry entry;
        CHECK_OK(PointerMap::read_entry(pager, page_id, entry));
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

auto print_wals(Env &env, std::size_t page_size, const std::string &prefix) -> void
{
    const auto [dir, base] = split_path(prefix);
    std::vector<std::string> possible_segments;
    CHECK_OK(env.get_children(dir, possible_segments));

    std::string tail_buffer(wal_block_size(page_size), '\0');
    std::string data_buffer(wal_scratch_size(page_size), '\0');

    for (auto &name : possible_segments) {
        name = join_paths(dir, name);
        if (!decode_segment_name(prefix, name).is_null()) {
            Reader *file;
            CHECK_OK(env.new_reader(name, file));
            WalReader reader {*file, tail_buffer};
            std::cerr << "Start of segment " << name << '\n';
            for (;;) {
                Span payload {data_buffer};
                auto s = reader.read(payload);
                if (s.is_not_found()) {
                    std::cerr << "End of segment\n";
                    break;
                } else if (!s.is_ok()) {
                    std::cerr << "Encountered \"" << get_status_name(s) << "\" status: " << s.to_string() << '\n';
                    break;
                }
                const auto decoded = decode_payload(payload);
                if (std::holds_alternative<DeltaDescriptor>(decoded)) {
                    const auto deltas = std::get<DeltaDescriptor>(decoded);
                    std::cerr << "    Delta: page_id=" << deltas.page_id.value << ", lsn=" << deltas.lsn.value << ", deltas=[\n";
                    std::size_t i {};
                    for (const auto &[offset, data] : deltas.deltas) {
                        std::cerr << "        " << i++ << ": offset=" << offset << ", data=" << escape_string(data) << '\n';
                    }
                } else if (std::holds_alternative<ImageDescriptor>(decoded)) {
                    const auto image = std::get<ImageDescriptor>(decoded);
                    std::uint64_t before_lsn {};
                    if (image.image.size() >= 8) {
                        before_lsn = get_u64(image.image.data() + FileHeader::kSize * image.page_id.is_root());
                    }
                    std::cerr << "    Image: page_id=" << image.page_id.value << ", lsn=" << image.lsn.value << ", before_lsn=" << before_lsn << ", image_size=" << image.image.size() << '\n';
                } else if (std::holds_alternative<VacuumDescriptor>(decoded)) {
                    const auto vacuum = std::get<VacuumDescriptor>(decoded);
                    std::cerr << "    Vacuum: is_start=" << vacuum.is_start << ", lsn=" << vacuum.lsn.value << '\n';
                }
            }
        }
    }
}

#undef TRY_INTERCEPT_FROM

} // namespace calicodb::tools
