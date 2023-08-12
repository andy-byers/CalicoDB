// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_COMMON_H
#define CALICODB_UTILS_COMMON_H

#include "calicodb/cursor.h"
#include "calicodb/env.h"
#include "calicodb/tx.h"
#include <algorithm>
#include <climits>
#include <functional>
#include <random>

namespace calicodb
{

class Pager;

template <size_t Length = 16>
static auto numeric_key(size_t key, char padding = '0') -> std::string
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

    auto read(size_t offset, size_t size, char *scratch, Slice *out) -> Status override
    {
        return m_target->read(offset, size, scratch, out);
    }

    auto write(size_t offset, const Slice &in) -> Status override
    {
        return m_target->write(offset, in);
    }

    auto resize(size_t size) -> Status override
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

    auto shm_map(size_t r, bool extend, volatile void *&out) -> Status override
    {
        return m_target->shm_map(r, extend, out);
    }

    auto shm_lock(size_t r, size_t n, ShmLockFlag flags) -> Status override
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
    mutable size_t m_pos = 0;
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(size_t size = 2 * 1'024 * 1'024 /* 2 MiB */)
        : m_data(size, '\0'),
          m_rng(42)
    {
        std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine(m_rng);
        std::generate(begin(m_data), end(m_data), std::ref(engine));
    }

    auto Generate(size_t len) const -> Slice
    {
        if (m_pos + len > m_data.size()) {
            m_pos = 0;
            assert(len < m_data.size());
            std::shuffle(begin(m_data), end(m_data), m_rng);
        }
        m_pos += len;
        return {m_data.data() + m_pos - len, static_cast<size_t>(len)};
    }

    // Not in LevelDB.
    auto Next(uint64_t t_max) const -> uint64_t
    {
        std::uniform_int_distribution<uint64_t> dist(0, t_max);
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(uint64_t t_min, uint64_t t_max) const -> uint64_t
    {
        std::uniform_int_distribution<uint64_t> dist(t_min, t_max);
        return dist(m_rng);
    }
};

// Print information about each database page to `os`
auto print_database_overview(std::ostream &os, Pager &pager) -> void;

using TestCursor = std::unique_ptr<Cursor>;

inline auto test_open_bucket(const Tx &tx, const Slice &name, TestCursor &c_out) -> Status
{
    Cursor *c;
    auto s = tx.open_bucket(name, c);
    c_out.reset(c);
    return s;
}

inline auto test_create_and_open_bucket(Tx &tx, const BucketOptions &options, const Slice &name, TestCursor &c_out) -> Status
{
    Cursor *c;
    auto s = tx.create_bucket(options, name, &c);
    c_out.reset(c);
    return s;
}

} // namespace calicodb

#endif // CALICODB_UTILS_COMMON_H
