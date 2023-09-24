// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "temp.h"
#include "calicodb/env.h"
#include "calicodb/wal.h"
#include "logging.h"
#include "mem.h"
#include "page.h"
#include "unique_ptr.h"
#include "wal_internal.h"

namespace calicodb
{

namespace
{

class TempEnv
    : public Env,
      public HeapObject
{
public:
    explicit TempEnv(size_t sector_size)
        : m_file(sector_size)
    {
    }

    ~TempEnv() override = default;

    auto new_logger(const char *, Logger *&logger_out) -> Status override
    {
        logger_out = nullptr;
        return Status::ok();
    }

    auto new_file(const char *filename, OpenMode, File *&file_out) -> Status override
    {
        if (m_filename.is_empty()) {
            if (append_strings(m_filename, filename)) {
                return Status::no_memory();
            }
        } else if (Slice(m_filename) != Slice(filename)) {
            // Only supports a single file: the database file. The WAL is simulated by TempWal.
            return Status::not_supported();
        }
        return new_temp_file(file_out);
    }

    auto new_temp_file(File *&file_out) -> Status
    {
        class TempFile
            : public File,
              public HeapObject
        {
            SectorFile *const m_file;

        public:
            explicit TempFile(SectorFile &file)
                : m_file(&file)
            {
            }

            ~TempFile() override = default;

            auto read(size_t offset, size_t size, char *scratch, Slice *data_out) -> Status override
            {
                if (offset >= m_file->actual_size) {
                    size = 0;
                } else if (offset + size > m_file->actual_size) {
                    size = m_file->actual_size - offset;
                }
                const auto max_chunk = m_file->sector_size;
                auto *sectors = m_file->sectors.ptr();
                auto *out = scratch;
                auto idx = offset / max_chunk;
                offset %= max_chunk;
                for (auto leftover = size; leftover; ++idx) {
                    const auto chunk = minval(max_chunk - offset, leftover);
                    std::memcpy(out, sectors[idx] + offset, chunk);
                    leftover -= chunk;
                    out += chunk;
                    offset = 0;
                }
                if (data_out) {
                    *data_out = Slice(scratch, size);
                }
                return Status::ok();
            }

            auto write(size_t offset, const Slice &data) -> Status override
            {
                if (m_file->actual_size < offset + data.size() &&
                    m_file->resize(offset + data.size())) {
                    return Status::no_memory();
                }
                const auto max_chunk = m_file->sector_size;
                auto *sectors = m_file->sectors.ptr();
                auto idx = offset / max_chunk;
                offset %= max_chunk;
                for (auto in = data; !in.is_empty(); ++idx) {
                    const auto chunk = minval(max_chunk - offset, in.size());
                    std::memcpy(sectors[idx] + offset, in.data(), chunk);
                    in.advance(chunk);
                    offset = 0;
                }
                return Status::ok();
            }

            auto resize(size_t size) -> Status override
            {
                return m_file->resize(size)
                           ? Status::no_memory()
                           : Status::ok();
            }

            auto sync() -> Status override
            {
                return Status::ok();
            }

            auto file_lock(FileLockMode) -> Status override
            {
                return Status::ok();
            }

            auto shm_map(size_t, bool, volatile void *&) -> Status override
            {
                return Status::not_supported();
            }

            auto shm_lock(size_t, size_t, ShmLockFlag) -> Status override
            {
                return Status::not_supported();
            }

            auto shm_unmap(bool) -> void override {}
            auto shm_barrier() -> void override {}
            auto file_unlock() -> void override {}
        };

        file_out = new (std::nothrow) TempFile(m_file);
        return file_out ? Status::ok() : Status::no_memory();
    }

    auto file_size(const char *filename, size_t &size_out) const -> Status override
    {
        if (file_exists(filename)) {
            size_out = m_file.actual_size;
            return Status::ok();
        }
        return Status::invalid_argument();
    }

    auto remove_file(const char *filename) -> Status override
    {
        if (file_exists(filename)) {
            m_filename.clear();
            return Status::ok();
        }
        return Status::invalid_argument();
    }

    [[nodiscard]] auto file_exists(const char *filename) const -> bool override
    {
        return !m_filename.is_empty() &&
               Slice(m_filename.c_str(), m_filename.size()) == Slice(filename);
    }

    auto srand(unsigned seed) -> void override
    {
        ::srand(seed);
    }

    auto rand() -> unsigned override
    {
        // This method is not called by the library. Normally, rand() is called by WalImpl to
        // generate a salt, but this class is only ever used with TempWal. If random numbers
        // are ever needed for in-memory databases, we should use a better PRNG.
        return static_cast<unsigned>(::rand());
    }

    auto sleep(unsigned micros) -> void override
    {
        default_env().sleep(micros);
    }

private:
    friend class TempWal;

    String m_filename;

    struct SectorFile final {
        Buffer<char *> sectors;
        const size_t sector_size;
        size_t actual_size;

        explicit SectorFile(size_t sector_size)
            : sector_size(sector_size),
              actual_size(0)
        {
        }

        ~SectorFile()
        {
            [[maybe_unused]] const auto rc = resize(0);
            CALICODB_EXPECT_EQ(rc, 0);
        }

        [[nodiscard]] auto resize(size_t size) -> int
        {
            const auto new_len = (size + sector_size - 1) / sector_size;
            // Free pages if shrinking the file.
            const auto old_len = sectors.len();
            for (size_t i = new_len; i < old_len; ++i) {
                Mem::deallocate(sectors[i]);
                // Clear pointers in case realloc() fails.
                sectors[i] = nullptr;
            }
            // Resize the page pointer array.
            if (sectors.realloc(new_len)) {
                // Alloc::reallocate() might fail when trimming an allocation, but not if the new size
                // is 0. In that case, the underlying reallocation function is not called. Instead, the
                // memory is freed using Alloc::deallocate().
                CALICODB_EXPECT_NE(new_len, 0);
                return -1;
            }
            // Allocate pages if growing the file.
            for (size_t i = old_len; i < new_len; ++i) {
                auto *&page = sectors[i];
                if (auto *ptr = Mem::allocate(sector_size)) {
                    std::memset(ptr, 0, sector_size);
                    page = static_cast<char *>(ptr);
                } else {
                    // Clear the rest of the pointers so the destructor doesn't mess up.
                    std::memset(page, 0, (new_len - i) * sizeof(char *));
                    return -1;
                }
            }
            actual_size = size;
            return 0;
        }

        [[nodiscard]] auto ensure_large_enough(size_t size) -> int
        {
            if (size > sectors.len() * sector_size && resize(size)) {
                return -1;
            }
            CALICODB_EXPECT_LE(size, sectors.len() * sector_size);
            return 0;
        }
    } m_file;
};

// In-memory WAL
// This WAL implementation only needs to save the most-recent version of each page (we
// can only rollback once, and there are no other connections).
class TempWal : public Wal
{
public:
    [[nodiscard]] static auto create(const WalOptionsExtra &options) -> TempWal *
    {
        auto *wal = Mem::new_object<TempWal>(
            reinterpret_cast<TempEnv &>(*options.env),
            *options.stat);
        if (wal->m_table.grow()) {
            Mem::deallocate(wal);
            return nullptr;
        }
        return wal;
    }

    explicit TempWal(TempEnv &env, Stats &stat)
        : m_env(&env),
          m_stat(&stat)
    {
    }

    ~TempWal() override
    {
        m_table.clear();
    }

    auto start_read(bool &changed) -> Status override
    {
        changed = false;
        return Status::ok();
    }

    auto read(uint32_t page_id, uint32_t page_size, char *&page) -> Status override
    {
        const auto itr = m_table.find(page_id);
        if (itr != m_table.end() && *itr) {
            const auto copy_size = minval(page_size, m_page_size);
            std::memcpy(page, (*itr)->page(), copy_size);
            m_stat->read_wal += copy_size;
        } else {
            page = nullptr;
        }
        return Status::ok();
    }

    auto start_write() -> Status override
    {
        return Status::ok();
    }

    auto write(Pages &writer, uint32_t page_size, size_t db_size) -> Status override
    {
        if (m_table.occupied == 0) {
            m_page_size = page_size;
        }
        CALICODB_EXPECT_EQ(m_page_size, page_size);
        for (; writer.value(); writer.next()) {
            if (m_table.occupied * 2 >= m_table.data.len()) {
                if (m_table.grow()) {
                    return Status::no_memory();
                }
            }
            auto *ref = writer.value();
            auto **itr = m_table.find(ref->page_id);
            if (*itr == nullptr) {
                *itr = PageEntry::create(ref->page_id, m_page_size);
                if (*itr == nullptr) {
                    return Status::no_memory();
                }
                ++m_table.occupied;
            }
            std::memcpy((*itr)->page(), ref->data, page_size);
            m_stat->write_wal += page_size;
        }
        if (db_size && commit(db_size)) {
            return Status::no_memory();
        }
        return Status::ok();
    }

    auto rollback(const Rollback &hook, void *object) -> void override
    {
        // This routine will call undo() on frames in a different order than the normal WAL
        // class. This shouldn't make a difference to the pager (the only caller).
        m_table.for_each([hook, object](auto &page) {
            hook(object, page->key);
            return 0;
        });
    }

    auto finish_write() -> void override
    {
        m_table.clear();
    }

    auto finish_read() -> void override
    {
        CALICODB_EXPECT_EQ(m_table.occupied, 0);
    }

    auto open(const WalOptions &, const char *) -> Status override
    {
        return Status::ok();
    }

    auto close(char *, uint32_t) -> Status override
    {
        return Status::ok();
    }

    auto checkpoint(bool, char *, uint32_t) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto wal_size() const -> size_t override
    {
        return 0;
    }

    [[nodiscard]] auto db_size() const -> size_t override
    {
        return static_cast<uint32_t>(m_env->m_file.sectors.len());
    }

private:
    [[nodiscard]] auto commit(size_t db_size) -> int
    {
        if (m_env->m_file.ensure_large_enough(db_size * m_page_size)) {
            return -1;
        }
        UserPtr<File> file;
        auto s = m_env->new_temp_file(file.ref());
        if (!s.is_ok()) {
            return -1;
        }
        const auto rc = m_table.for_each([this, &file, db_size](auto *page) {
            if (page->key <= db_size) {
                auto s = file->write((page->key - 1) * m_page_size, Slice(page->page(), m_page_size));
                if (!s.is_ok()) {
                    return -1;
                }
                m_stat->read_wal += m_page_size;
                m_stat->write_db += m_page_size;
            }
            return 0;
        });
        if (rc || m_env->m_file.resize(db_size * m_page_size)) {
            return -1;
        }
        m_table.clear();
        return 0;
    }

    struct PageEntry {
        uint32_t key;

        [[nodiscard]] auto page() -> char *
        {
            return reinterpret_cast<char *>(this + 1);
        }

        static auto create(uint32_t key, size_t page_size) -> PageEntry *
        {
            auto *ptr = static_cast<PageEntry *>(
                Mem::allocate(sizeof(PageEntry) + page_size));
            if (ptr) {
                ptr->key = key;
            }
            return ptr;
        }
    };

    // Simple hash table for pages written to the WAL
    // Also, we never need to
    // remove single pages, which simplifies the implementation. Uses linear probing.
    struct PageTable {
        Buffer<PageEntry *> data;
        size_t occupied = 0;

        [[nodiscard]] auto end() -> PageEntry **
        {
            return data.ptr() + data.len();
        }

        template <class Action>
        auto for_each(Action &&action) -> int
        {
            for (size_t i = 0; i < data.len(); ++i) {
                // Caller may need to free and clear the pointer, so provide a reference.
                auto *&ptr = data[i];
                if (ptr && action(ptr)) {
                    return -1;
                }
            }
            return 0;
        }

        [[nodiscard]] auto find(uint32_t key) -> PageEntry **
        {
            // grow() must succeed at least once before this method is called.
            CALICODB_EXPECT_LT(occupied, data.len());
            CALICODB_EXPECT_FALSE(data.is_empty());

            // Robert Jenkins' 32-bit integer hash function
            // Source: https://gist.github.com/badboy/6267743.
            const auto hash = [](uint32_t x) {
                x = (x + 0x7ED55D16) + (x << 12);
                x = (x ^ 0xC761C23C) ^ (x >> 19);
                x = (x + 0x165667B1) + (x << 5);
                x = (x + 0xD3A2646C) ^ (x << 9);
                x = (x + 0xFD7046C5) + (x << 3);
                x = (x ^ 0xB55A4F09) ^ (x >> 16);
                return x;
            };

            size_t tries = 0;
            for (auto h = hash(key);; ++h, ++tries) {
                CALICODB_EXPECT_LT(tries, data.len());
                auto *&ptr = data[h & (data.len() - 1)];
                if (ptr == nullptr || ptr->key == key) {
                    return &ptr;
                }
            }
        }

        [[nodiscard]] auto grow() -> int
        {
            uint32_t capacity = 4;
            while (capacity <= data.len()) {
                capacity *= 2;
            }
            PageTable table;
            if (table.data.realloc(capacity)) {
                return -1;
            }
            std::memset(table.data.ptr(), 0, capacity * sizeof(PageEntry *));
            for_each([&table](auto *page) {
                *table.find(page->key) = page;
                return 0;
            });
            data = move(table.data);
            return 0;
        }

        auto clear() -> void
        {
            for_each([this](auto *&page) {
                Mem::deallocate(page);
                page = nullptr;
                --occupied;
                return 0;
            });
        }
    } m_table;

    TempEnv *const m_env;
    Stats *const m_stat;
    uint32_t m_page_size;
};

} // namespace

auto new_temp_env(size_t sector_size) -> Env *
{
    return new (std::nothrow) TempEnv(sector_size);
}

auto new_temp_wal(const WalOptionsExtra &options) -> Wal *
{
    return TempWal::create(options);
}

} // namespace calicodb