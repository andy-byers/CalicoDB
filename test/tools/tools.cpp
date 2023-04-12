// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include "encoding.h"
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

auto FakeEnv::get_file_contents(const std::string &filename) const -> std::string
{
    const auto file = m_state.find(filename);
    if (file == end(m_state) || !file->second.created) {
        return "";
    }
    return file->second.buffer;
}

auto FakeEnv::put_file_contents(const std::string &filename, std::string contents) -> void
{
    auto file = m_state.find(filename);
    if (file == end(m_state)) {
        file = m_state.insert(file, {filename, FileState()});
    }
    file->second.buffer = std::move(contents);
    file->second.created = true;
}

auto FakeEnv::read_file_at(const FileState &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    if (offset < mem.buffer.size()) {
        const auto read_size = std::min(size, mem.buffer.size() - offset);
        std::memcpy(scratch, mem.buffer.data() + offset, read_size);
        if (out != nullptr) {
            *out = Slice(scratch, read_size);
        }
    }
    return Status::ok();
}

auto FakeEnv::write_file_at(FileState &mem, std::size_t offset, const Slice &in) -> Status
{
    if (const auto write_end = offset + in.size(); mem.buffer.size() < write_end) {
        mem.buffer.resize(write_end);
    }
    std::memcpy(mem.buffer.data() + offset, in.data(), in.size());
    return Status::ok();
}

auto FakeFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    return m_env->read_file_at(*m_state, offset, size, scratch, out);
}

auto FakeFile::write(std::size_t offset, const Slice &in) -> Status
{
    return m_env->write_file_at(*m_state, offset, in);
}

auto FakeFile::sync() -> Status
{
    return Status::ok();
}

auto FakeEnv::open_or_create_file(const std::string &filename) const -> FileState &
{
    auto itr = m_state.find(filename);
    if (itr == end(m_state)) {
        itr = m_state.insert(itr, {filename, {}});
    }
    return itr->second;
}

auto FakeEnv::new_file(const std::string &filename, File *&out) -> Status
{
    auto &mem = open_or_create_file(filename);
    out = new FakeFile(filename, *this, mem);
    if (!mem.created) {
        mem.created = true;
        mem.buffer.clear();
    }
    return Status::ok();
}

auto FakeEnv::new_log_file(const std::string &, LogFile *&out) -> Status
{
    out = new FakeLogFile;
    return Status::ok();
}

auto FakeEnv::remove_file(const std::string &filename) -> Status
{
    auto itr = m_state.find(filename);
    if (itr == end(m_state)) {
        return Status::not_found('"' + filename + "\" does not exist");
    }
    // Don't actually get rid of any memory. We should be able to unlink a file and still access it
    // through open file descriptors, so if anyone has this file open, they should still be able to
    // access it.
    itr->second.created = false;
    return Status::ok();
}

auto FakeEnv::resize_file(const std::string &filename, std::size_t size) -> Status
{
    auto itr = m_state.find(filename);
    if (itr == end(m_state)) {
        return Status::not_found('"' + filename + "\" does not exist");
    }
    itr->second.buffer.resize(size);
    return Status::ok();
}

auto FakeEnv::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    auto itr = m_state.find(filename);
    if (itr == cend(m_state) || !itr->second.created) {
        return Status::not_found("file does not exist");
    }
    out = itr->second.buffer.size();
    return Status::ok();
}

auto FakeEnv::file_exists(const std::string &filename) const -> bool
{
    if (const auto &itr = m_state.find(filename); itr != end(m_state)) {
        return itr->second.created;
    }
    return false;
}

auto FakeEnv::clone() const -> Env *
{
    auto *env = new FakeEnv;
    env->m_state = m_state;
    return env;
}

TestEnv::TestEnv()
    : EnvWrapper(*new tools::FakeEnv)
{
}

TestEnv::TestEnv(Env &env)
    : EnvWrapper(env)
{
}

TestEnv::~TestEnv()
{
    delete target();
}

auto TestEnv::save_file_contents(const std::string &filename) -> void
{
    auto state = m_state.find(filename);
    CHECK_TRUE(state != end(m_state));
    state->second.saved_state = read_file_to_string(*target(), filename);
}

auto TestEnv::overwrite_file(const std::string &filename, const std::string &contents) -> void
{
    write_string_to_file(*target(), filename, contents, 0);
    CHECK_OK(target()->resize_file(filename, contents.size()));
}

auto TestEnv::clone() -> Env *
{
    auto *env = new TestEnv(
        *reinterpret_cast<FakeEnv *>(target())->clone());
    for (const auto &state : m_state) {
        const auto file = read_file_to_string(*this, state.first);
        write_string_to_file(*env, state.first, file, 0);
    }
    return env;
}

auto TestEnv::drop_after_last_sync() -> void
{
    for (const auto &[filename, _] : m_state) {
        drop_after_last_sync(filename);
    }
}

auto TestEnv::drop_after_last_sync(const std::string &filename) -> void
{
    const auto state = m_state.find(filename);
    if (state != end(m_state) && !state->second.unlinked) {
        overwrite_file(filename, state->second.saved_state);
    }
}

auto TestEnv::new_file(const std::string &filename, File *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kOpen, filename);

    auto s = target()->new_file(filename, out);
    if (s.is_ok()) {
        auto state = m_state.find(filename);
        if (state == end(m_state)) {
            state = m_state.insert(state, {filename, FileState()});
        }
        state->second.unlinked = false;
        out = new TestFile(filename, *out, *this);
    }
    return s;
}

auto TestEnv::resize_file(const std::string &filename, std::size_t file_size) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kResize, filename);
    return target()->resize_file(filename, file_size);
}

auto TestEnv::remove_file(const std::string &filename) -> Status
{
    TRY_INTERCEPT_FROM(*this, Interceptor::kUnlink, filename);

    auto s = target()->remove_file(filename);
    if (s.is_ok()) {
        auto state = m_state.find(filename);
        CHECK_TRUE(state != end(m_state));
        state->second.unlinked = true;
    }
    return s;
}

auto TestEnv::try_intercept_syscall(Interceptor::Type type, const std::string &filename) -> Status
{
    const auto state = m_state.find(filename);
    if (state != end(m_state)) {
        for (const auto &interceptor : state->second.interceptors) {
            if (interceptor.type == type) {
                return interceptor.callback();
            }
        }
    }
    return Status::ok();
}

auto TestEnv::add_interceptor(const std::string &filename, Interceptor interceptor) -> void
{
    auto state = m_state.find(filename);
    if (state == end(m_state)) {
        state = m_state.insert(state, {filename, FileState()});
    }
    state->second.interceptors.emplace_back(std::move(interceptor));
}

auto TestEnv::clear_interceptors() -> void
{
    for (auto &state : m_state) {
        state.second.interceptors.clear();
    }
}

auto TestEnv::clear_interceptors(const std::string &filename) -> void
{
    auto state = m_state.find(filename);
    if (state != end(m_state)) {
        state->second.interceptors.clear();
    }
}

TestFile::TestFile(std::string filename, File &file, TestEnv &env)
    : m_filename(std::move(filename)),
      m_env(&env),
      m_file(&file)
{
}

TestFile::~TestFile()
{
    m_env->drop_after_last_sync(m_filename);

    delete m_file;
}

auto TestFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    TRY_INTERCEPT_FROM(*m_env, Interceptor::kRead, m_filename);
    return m_file->read(offset, size, scratch, out);
}

auto TestFile::write(std::size_t offset, const Slice &in) -> Status
{
    TRY_INTERCEPT_FROM(*m_env, Interceptor::kWrite, m_filename);
    return m_file->write(offset, in);
}

auto TestFile::sync() -> Status
{
    TRY_INTERCEPT_FROM(*m_env, Interceptor::kSync, m_filename);
    auto s = m_file->sync();
    if (s.is_ok()) {
        m_env->save_file_contents(m_filename);
    }
    return s;
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
                std::cerr << entry.back_ptr.value << " -> freelist link -> " << get_u32(page.data()) << '\n';
                break;
            case PointerMap::kOverflowHead:
                std::cerr << entry.back_ptr.value << " -> overflow head -> " << get_u32(page.data()) << '\n';
                break;
            case PointerMap::kOverflowLink:
                std::cerr << entry.back_ptr.value << " -> overflow link -> " << get_u32(page.data()) << '\n';
                break;
        }
        pager.release(std::move(page));
    }
}

#undef TRY_INTERCEPT_FROM

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    const auto s = env.file_size(filename, file_size);
    if (s.is_not_found()) {
        // File was unlinked.
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    CHECK_OK(env.new_file(filename, file));
    CHECK_OK(file->read_exact(0, file_size, buffer.data()));
    delete file;

    return buffer;
}

auto write_string_to_file(Env &env, const std::string &filename, std::string buffer, long offset) -> void
{
    std::size_t write_pos;
    if (offset < 0) {
        CHECK_OK(env.file_size(filename, write_pos));
    } else {
        write_pos = offset;
    }
    File *file;
    CHECK_OK(env.new_file(filename, file));
    CHECK_OK(file->write(write_pos, buffer));
    delete file;
}

auto fill_db(DB &db, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    return fill_db(db, *db.default_table(), random, num_records, max_payload_size);
}

auto fill_db(DB &db, Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    CHECK_TRUE(max_payload_size > 0);
    std::map<std::string, std::string> records;

    for (std::size_t i = 0; i < num_records; ++i) {
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
    for (const auto &[key, value] : map) {
        std::string result;
        CHECK_OK(db.get(table, key, &result));
        CHECK_EQ(result, value);
    }
}

FakeWal::FakeWal(const Parameters &param)
    : m_param(param)
{
}

auto FakeWal::read(Id page_id, char *&out) -> Status
{
    for (const auto &map : {m_pending, m_committed}) {
        const auto itr = map.find(page_id);
        if (itr != end(map)) {
            std::memcpy(out, itr->second.c_str(), m_param.page_size);
            return Status::ok();
        }
    }
    out = nullptr;
    return Status::ok();
}

auto FakeWal::write(const CacheEntry *dirty, std::size_t db_size) -> Status
{
    for (auto *p = dirty; p; p = p->next) {
        m_pending.insert_or_assign(p->page_id, std::string(p->page, m_param.page_size));
    }
    if (db_size) {
        for (const auto &[k, v] : m_pending) {
            m_committed.insert_or_assign(k, v);
        }
        m_pending.clear();
        m_db_size = db_size;
    }
    return Status::ok();
}

auto FakeWal::needs_checkpoint() const -> bool
{
    return m_committed.size() > 1'000;
}

auto FakeWal::checkpoint(File &db_file, std::size_t *db_size) -> Status
{
    // TODO: Need the env to resize the file.
    CALICODB_EXPECT_TRUE(m_pending.empty());
    // Write back to the DB sequentially.
    for (const auto &[page_id, page] : m_committed) {
        const auto offset = page_id.as_index() * m_param.page_size;
        CALICODB_TRY(db_file.write(offset, page));
    }
    if (db_size != nullptr) {
        *db_size = m_db_size;
    }
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::abort() -> Status
{
    m_pending.clear();
    return Status::ok();
}

auto FakeWal::close() -> Status
{
    m_pending.clear();
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::statistics() const -> WalStatistics
{
    return WalStatistics {};
}

} // namespace calicodb::tools
