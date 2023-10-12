// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_COMMON_H
#define CALICODB_UTILS_COMMON_H

#include "calicodb/bucket.h"
#include "calicodb/config.h"
#include "calicodb/cursor.h"
#include "calicodb/env.h"
#include "calicodb/tx.h"
#include "internal.h"
#include <algorithm>
#include <climits>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>

namespace calicodb
{

class Pager;

inline auto get_full_filename(const std::string &filename) -> std::string
{
    std::string result(default_env().max_filename() + 1, '\0');
    const auto s = default_env().full_filename(filename.c_str(), result.data(), result.size() - 1);
    assert(s.is_ok());
    result.resize(std::strlen(result.c_str()));
    return result;
}

template <size_t Length = 16>
auto numeric_key(size_t key, char padding = '0') -> std::string
{
    const auto key_string = std::to_string(key);
    assert(Length >= key_string.size());
    return std::string(Length - key_string.size(), padding) + key_string;
}

// NOTE: Member functions are not thread-safe.
class DebugAllocator
{
public:
    static auto config() -> AllocatorConfig *;

    static auto set_limit(size_t limit) -> size_t;
    static auto set_junk_byte(char c) -> char;

    // Allocation hook for testing
    using Hook = int (*)(void *);

    // Set a callback that is called in malloc() and realloc() with the provided `arg`.
    // If the result is nonzero, a nullptr is returned immediately, before the actual
    // allocation routine is called. Used for injecting random errors during testing.
    static auto set_hook(Hook hook, void *arg) -> void;

    // Get the total number of bytes allocated through malloc() and realloc() that have
    // not yet been passed to free()
    static auto bytes_used() -> size_t;

    static auto size_of(void *ptr) -> size_t;
};

class FileWrapper : public File
{
public:
    explicit FileWrapper(File &target)
        : m_target(&target)
    {
    }

    ~FileWrapper() override = default;

    auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status override
    {
        return m_target->read(offset, size, scratch, out);
    }

    auto write(uint64_t offset, const Slice &in) -> Status override
    {
        return m_target->write(offset, in);
    }

    auto get_size(uint64_t &size_out) const -> Status override
    {
        return m_target->get_size(size_out);
    }

    auto resize(uint64_t size) -> Status override
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
        return {m_data.data() + m_pos - len, len};
    }

    // Not in LevelDB.
    auto Next(size_t t_max) const -> size_t
    {
        std::uniform_int_distribution<size_t> dist(0, t_max);
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(size_t t_min, size_t t_max) const -> size_t
    {
        std::uniform_int_distribution<size_t> dist(t_min, t_max);
        return dist(m_rng);
    }
};

inline auto to_string(const Slice &s) -> std::string
{
    return {s.data(), s.size()};
}

inline auto to_slice(const std::string &s) -> Slice
{
    return {s.data(), s.size()};
}

template <class T>
auto operator<<(std::ostream &os, const Id &id) -> std::ostream &
{
    return os << "Id(" << id.value << ')';
}

template <class T>
auto operator<<(std::ostream &os, const Slice &slice) -> std::ostream &
{
    return os << to_string(slice);
}

// Print information about each database page to `os`
auto print_database_overview(std::ostream &os, Pager &pager) -> void;

using TestBucket = std::unique_ptr<Bucket>;
using TestCursor = std::unique_ptr<Cursor>;

inline auto test_new_cursor(const Bucket &b) -> TestCursor
{
    return TestCursor(b.new_cursor());
}

inline auto test_open_bucket(const Tx &tx, const Slice &name, TestBucket &b_out) -> Status
{
    Bucket *b;
    auto s = tx.open_bucket(name, b);
    b_out.reset(b);
    return s;
}

inline auto test_open_bucket(const Bucket &b, const Slice &key, TestBucket &b_out) -> Status
{
    Bucket *b2;
    auto s = b.open_bucket(key, b2);
    b_out.reset(b2);
    return s;
}

inline auto test_create_and_open_bucket(Tx &tx, const Slice &name, TestBucket &b_out) -> Status
{
    Bucket *b;
    auto s = tx.create_bucket(name, &b);
    b_out.reset(b);
    return s;
}

inline auto test_create_and_open_bucket(Bucket &b, const Slice &key, TestBucket &b_out) -> Status
{
    Bucket *b2;
    auto s = b.create_bucket(key, &b2);
    b_out.reset(b2);
    return s;
}

// Counting semaphore
class Semaphore
{
    mutable std::mutex m_mu;
    std::condition_variable m_cv;
    size_t m_available;

public:
    explicit Semaphore(size_t n = 0)
        : m_available(n)
    {
    }

    Semaphore(Semaphore &) = delete;
    void operator=(Semaphore &) = delete;

    auto wait() -> void
    {
        std::unique_lock lock(m_mu);
        m_cv.wait(lock, [this] {
            return m_available > 0;
        });
        --m_available;
    }

    auto signal(size_t n = 1) -> void
    {
        m_mu.lock();
        m_available += n;
        m_mu.unlock();
        m_cv.notify_all();
    }
};

// Reusable thread barrier
class Barrier
{
    Semaphore m_phase_1;
    Semaphore m_phase_2;
    mutable std::mutex m_mu;
    const size_t m_max_count;
    size_t m_count = 0;

public:
    explicit Barrier(size_t max_count)
        : m_max_count(max_count)
    {
    }

    Barrier(Barrier &) = delete;
    void operator=(Barrier &) = delete;

    // Wait for m_max_count threads to call this routine
    auto wait() -> void
    {
        m_mu.lock();
        if (++m_count == m_max_count) {
            m_phase_1.signal(m_max_count);
        }
        m_mu.unlock();
        m_phase_1.wait();

        m_mu.lock();
        if (--m_count == 0) {
            m_phase_2.signal(m_max_count);
        }
        m_mu.unlock();
        m_phase_2.wait();
    }
};

} // namespace calicodb

#endif // CALICODB_UTILS_COMMON_H
