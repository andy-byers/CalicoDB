//
// Created by andy-byers on 4/12/23.
//

#include "env_helpers.h"

namespace calicodb::tools 
{

#define TRY_INTERCEPT_FROM(source, type, filename)                                                     \
    do {                                                                                               \
        if (auto intercept_s = (source).try_intercept_syscall(type, filename); !intercept_s.is_ok()) { \
            return intercept_s;                                                                        \
        }                                                                                              \
    } while (0)


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

} // namespace calicodb::tools