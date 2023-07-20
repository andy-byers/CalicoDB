// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_COMMON_H
#define CALICODB_UTILS_COMMON_H

#include "calicodb/env.h"
#include <algorithm>
#include <climits>
#include <functional>
#include <random>

namespace calicodb
{

class Pager;

template <std::size_t Length = 16>
static auto numeric_key(std::size_t key, char padding = '0') -> std::string
{
    const auto key_string = std::to_string(key);
    assert(Length >= key_string.size());
    return std::string(Length - key_string.size(), padding) + key_string;
}

class FileWrapper : public File
{
public:
    explicit FileWrapper(File &target)
        : m_target(&target)
    {
    }

    ~FileWrapper() override = default;

    auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
    {
        return m_target->read(offset, size, scratch, out);
    }

    auto write(std::size_t offset, const Slice &in) -> Status override
    {
        return m_target->write(offset, in);
    }

    auto resize(std::size_t size) -> Status override
    {
        return m_target->resize(size);
    }

    auto sync() -> Status override
    {
        return m_target->sync();
    }

    auto file_lock(FileLockMode mode) -> Status override
    {
        return m_target->file_lock(mode);
    }

    auto file_unlock() -> void override
    {
        return m_target->file_unlock();
    }

    auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override
    {
        return m_target->shm_map(r, extend, out);
    }

    auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override
    {
        return m_target->shm_lock(r, n, flags);
    }

    auto shm_unmap(bool unlink) -> void override
    {
        return m_target->shm_unmap(unlink);
    }

    auto shm_barrier() -> void override
    {
        return m_target->shm_barrier();
    }

protected:
    File *m_target;
};

// Modified from LevelDB.
class RandomGenerator
{
private:
    using Engine = std::default_random_engine;

    mutable std::string m_data;
    mutable std::size_t m_pos = 0;
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(std::size_t size = 2 * 1'024 * 1'024 /* 2 MiB */)
        : m_data(size, '\0'),
          m_pos(size),
          m_rng(42)
    {
    }

    auto Generate(std::size_t len) const -> std::string_view
    {
        if (m_pos + len > m_data.size()) {
            m_pos = 0;
            assert(len < m_data.size());
            // Generate data each time the end is passed, rather than once in the constructor.
            std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine(m_rng);
            std::generate(begin(m_data), end(m_data), std::ref(engine));
        }
        m_pos += len;
        return {m_data.data() + m_pos - len, static_cast<std::size_t>(len)};
    }

    // Not in LevelDB.
    auto Next(std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist(0, t_max);
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(std::uint64_t t_min, std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist(t_min, t_max);
        return dist(m_rng);
    }
};

// Print information about each database page to `os`
auto print_database_overview(std::ostream &os, Pager &pager) -> void;

} // namespace calicodb

#endif // CALICODB_UTILS_COMMON_H
