// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include "env_posix.h"
#include "logging.h"
#include <algorithm>
#include <iomanip>
#include <iostream>

namespace calicodb::tools
{

#define TRY_INTERCEPT_FROM(source, type, filename)                                                     \
    do {                                                                                               \
        if (auto intercept_s = (source).try_intercept_syscall(type, filename); !intercept_s.is_ok()) { \
            return intercept_s;                                                                        \
        }                                                                                              \
    } while (0)

namespace fs = std::filesystem;

auto FakeEnv::read_file_at(const Memory &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    std::size_t read_size = 0;
    if (Slice buffer(mem.buffer); offset < mem.buffer.size()) {
        read_size = std::min(size, buffer.size() - offset);
        std::memcpy(scratch, buffer.advance(offset).data(), read_size);
        if (out != nullptr) {
            *out = Slice(scratch, read_size);
        }
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

auto FakeFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    return m_parent->read_file_at(*m_mem, offset, size, scratch, out);
}

auto FakeFile::write(std::size_t offset, const Slice &in) -> Status
{
    return m_parent->write_file_at(*m_mem, offset, in);
}

auto FakeFile::sync() -> Status
{
    return Status::ok();
}

auto FakeEnv::get_memory(const std::string &filename) const -> Memory &
{
    auto itr = m_memory.find(filename);
    if (itr == end(m_memory)) {
        itr = m_memory.insert(itr, {filename, {}});
    }
    return itr->second;
}

auto FakeEnv::new_file(const std::string &filename, File *&out) -> Status
{
    auto &mem = get_memory(filename);
    out = new FakeFile {filename, *this, mem};
    mem.created = true;
    return Status::ok();
}

auto FakeEnv::new_log_file(const std::string &, LogFile *&out) -> Status
{
    out = new FakeLogFile;
    return Status::ok();
}

auto FakeEnv::remove_file(const std::string &filename) -> Status
{
    auto &mem = get_memory(filename);
    auto itr = m_memory.find(filename);
    if (itr == end(m_memory)) {
        return Status::not_found("cannot remove file");
    }
    // Don't actually get rid of any memory. We should be able to unlink a file and still access it
    // through open file descriptors, so if any readers or writers have this file put, they should
    // still be able to use it.
    itr->second.created = false;
    return Status::ok();
}

auto FakeEnv::resize_file(const std::string &filename, std::size_t size) -> Status
{
    auto itr = m_memory.find(filename);
    if (itr == end(m_memory)) {
        return Status::io_error("cannot resize file");
    }
    itr->second.buffer.resize(size);
    return Status::ok();
}

auto FakeEnv::rename_file(const std::string &old_filename, const std::string &new_filename) -> Status
{
    if (new_filename.empty()) {
        return Status::invalid_argument("name has zero length");
    }

    auto node = m_memory.extract(old_filename);
    if (node.empty()) {
        return Status::not_found("file does not exist");
    }

    node.key() = new_filename;
    m_memory.insert(std::move(node));
    return Status::ok();
}

auto FakeEnv::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    auto itr = m_memory.find(filename);
    if (itr == cend(m_memory)) {
        return Status::not_found("file does not exist");
    }
    out = itr->second.buffer.size();
    return Status::ok();
}

auto FakeEnv::file_exists(const std::string &filename) const -> bool
{
    if (const auto &itr = m_memory.find(filename); itr != end(m_memory)) {
        return itr->second.created;
    }
    return false;
}

auto FakeEnv::get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status
{
    auto prefix = dirname.back() == '/' ? dirname : dirname + '/';
    for (const auto &[filename, mem] : m_memory) {
        if (mem.created && Slice(filename).starts_with(prefix)) {
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

auto FaultInjectionEnv::try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status
{
    for (const auto &interceptor : m_interceptors) {
        if (interceptor.type == type && filename.find(interceptor.prefix) == 0) {
            CALICODB_TRY(interceptor());
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

auto FaultInjectionFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kRead, m_filename);
    return FakeFile::read(offset, size, scratch, out);
}

auto FaultInjectionFile::write(std::size_t offset, const Slice &in) -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kWrite, m_filename);
    return FakeFile::write(offset, in);
}

auto FaultInjectionFile::sync() -> Status
{
    TRY_INTERCEPT_FROM(reinterpret_cast<FaultInjectionEnv &>(*m_parent), Interceptor::kSync, m_filename);
    return FakeFile::sync();
}

auto FaultInjectionEnv::new_file(const std::string &filename, File *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, filename);
    FakeFile *file;
    CALICODB_TRY(FakeEnv::new_file(filename, reinterpret_cast<File *&>(file)));
    out = new FaultInjectionFile(*file);
    delete file;
    return Status::ok();
}

auto FaultInjectionEnv::new_log_file(const std::string &filename, LogFile *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, filename);
    FakeLogFile *file;
    CALICODB_TRY(FakeEnv::new_log_file(filename, reinterpret_cast<LogFile *&>(file)));
    out = new FaultInjectionLogFile;
    delete file;
    return Status::ok();
}

auto FaultInjectionEnv::remove_file(const std::string &filename) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kUnlink, filename);
    return FakeEnv::remove_file(filename);
}

auto FaultInjectionEnv::resize_file(const std::string &filename, std::size_t size) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kResize, filename);
    return FakeEnv::resize_file(filename, size);
}

auto FaultInjectionEnv::rename_file(const std::string &old_filename, const std::string &new_filename) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kRename, old_filename);
    return FakeEnv::rename_file(old_filename, new_filename);
}

auto FaultInjectionEnv::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    return FakeEnv::file_size(filename, out);
}

auto FaultInjectionEnv::file_exists(const std::string &filename) const -> bool
{
    return FakeEnv::file_exists(filename);
}

auto FaultInjectionEnv::get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status
{
    return FakeEnv::get_children(dirname, out);
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
      m_rng(42)
{
    std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine(m_rng);
    std::generate(begin(m_data), end(m_data), std::ref(engine));
}

auto RandomGenerator::Generate(std::size_t len) const -> Slice
{
    if (m_pos + len > m_data.size()) {
        m_pos = 0;
        CALICODB_EXPECT_LT(len, m_data.size());
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
            std::cerr << "NULL <- node -> ...\n";
            continue;
        }
        PointerMap::Entry entry;
        CHECK_OK(PointerMap::read_entry(pager, page_id, entry));

        Page page;
        CHECK_OK(pager.acquire(page_id, page));

        switch (entry.type) {
            case PointerMap::kTreeNode:
                std::cerr << entry.back_ptr.value << " -> node -> ...\n";
                break;
            case PointerMap::kTreeRoot:
                std::cerr << "1 -> root for table " << entry.back_ptr.value << " -> ...\n";
                break;
            case PointerMap::kFreelistLink:
                std::cerr << entry.back_ptr.value << " -> freelist link -> " << get_u64(page.data() + 8) << '\n';
                break;
            case PointerMap::kOverflowHead:
                std::cerr << entry.back_ptr.value << " -> overflow head -> " << get_u64(page.data() + 8) << '\n';
                break;
            case PointerMap::kOverflowLink:
                std::cerr << entry.back_ptr.value << " -> overflow link -> " << get_u64(page.data() + 8) << '\n';
                break;
        }
        pager.release(std::move(page));
    }
}

#undef TRY_INTERCEPT_FROM

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    CHECK_OK(env.file_size(filename, file_size));

    std::string buffer(file_size, '\0');

    File *file;
    CHECK_OK(env.new_file(filename, file));

    CHECK_OK(file->read_exact(0, file_size, buffer.data()));

    delete file;
    return buffer;
}

auto fill_db(DB &db, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    return fill_db(db, *db.default_table(), random, num_records, max_payload_size);
}

auto fill_db(DB &db, Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    CHECK_TRUE(max_payload_size > 0);
    const auto base_db_size = reinterpret_cast<const DBImpl &>(db).TEST_state().record_count;
    std::map<std::string, std::string> records;

    for (;;) {
        const auto db_size = reinterpret_cast<const DBImpl &>(db).TEST_state().record_count;
        if (db_size >= base_db_size + num_records) {
            break;
        }
        const auto ksize = random.Next(1, max_payload_size);
        const auto vsize = random.Next(max_payload_size - ksize);
        const auto k = random.Generate(ksize);
        const auto v = random.Generate(vsize);
        CHECK_OK(db.put(table, k, v));
        records[k.to_string()] = v.to_string();
    }
    return records;
}

auto expect_db_contains(const DB &db, const std::map<std::string, std::string> &map) -> void
{
    return expect_db_contains(db, *db.default_table(), map);
}

auto expect_db_contains(const DB &db, const Table &table, const std::map<std::string, std::string> &map) -> void
{
    std::size_t i = 0;
    for (const auto &[key, value] : map) {
        std::string result;
        CHECK_OK(db.get(table, key, &result));
        CHECK_EQ(result, value);
        ++i;
    }
}

} // namespace calicodb::tools
