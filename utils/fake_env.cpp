// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fake_env.h"

namespace calicodb
{

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

auto FakeFile::shm_map(std::size_t r, bool, volatile void *&out) -> Status
{
    while (m_shm.size() <= r) {
        m_shm.emplace_back();
        m_shm.back().resize(File::kShmRegionSize);
    }
    out = m_shm[r].data();
    return Status::ok();
}

auto FakeFile::shm_unmap(bool unlink) -> void
{
    if (unlink) {
        m_shm.clear();
    }
}

auto FakeEnv::new_logger(const std::string &, Logger *&) -> Status
{
    return Status::ok();
}

auto FakeEnv::new_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    auto itr = m_state.find(filename);
    if (itr == end(m_state)) {
        itr = m_state.insert(itr, {filename, {}});
    }
    if (!itr->second.created) {
        if (mode & Env::kCreate) {
            itr->second.created = true;
            itr->second.buffer.clear();
        } else {
            return Status::io_error();
        }
    }
    out = new FakeFile(filename, *this, itr->second);
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

auto FakeEnv::srand(unsigned seed) -> void
{
    ::srand(seed);
}

auto FakeEnv::rand() -> unsigned
{
    return static_cast<unsigned>(::rand());
}

auto FakeEnv::clone() const -> Env *
{
    auto *env = new FakeEnv;
    env->m_state = m_state;
    return env;
}

} // namespace calicodb