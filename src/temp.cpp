// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source Status::Code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "temp.h"
#include "alloc.h"
#include "logging.h"
#include "page.h"
#include "ptr.h"
#include "stat.h"
#include "wal.h"
#include <random>

namespace calicodb
{

namespace
{

class TempEnv
    : public Env,
      public HeapObject
{
public:
    ~TempEnv() override = default;

    auto new_logger(const char *, Logger *&logger_out) -> Status override
    {
        logger_out = nullptr;
        return Status::ok();
    }

    auto new_file(const char *filename, OpenMode, File *&file_out) -> Status override
    {
        class TempFile
            : public File,
              public HeapObject
        {
            PagedFile *const m_file;

        public:
            explicit TempFile(PagedFile &file)
                : m_file(&file)
            {
            }

            ~TempFile() override = default;

            auto read(size_t offset, size_t size, char *scratch, Slice *data_out) -> Status override
            {
                const auto page_id = Id::from_index(offset / kPageSize);

                // This class is incapable of reading anything other than a full page of data.
                CALICODB_EXPECT_EQ(offset, page_id.as_index() * kPageSize);
                CALICODB_EXPECT_EQ(size, kPageSize);

                const auto *page = m_file->fetch_page(page_id);
                if (page == nullptr) {
                    // page_id is out of bounds.
                    page = "";
                    size = 0;
                }

                std::memcpy(scratch, page, size);
                if (data_out) {
                    *data_out = Slice(scratch, size);
                }
                return Status::ok();
            }

            auto write(size_t offset, const Slice &data) -> Status override
            {
                const auto page_id = Id::from_index(offset / kPageSize);

                // This class is incapable of reading anything other than a full page of data.
                CALICODB_EXPECT_EQ(offset, page_id.as_index() * kPageSize);
                CALICODB_EXPECT_EQ(data.size(), kPageSize);

                if (m_file->ensure_large_enough(page_id.value)) {
                    return Status::no_memory();
                }
                auto *page = m_file->fetch_page(page_id);
                std::memcpy(page, data.data(), data.size());
                return Status::ok();
            }

            auto resize(size_t size) -> Status override
            {
                const auto page_count = size / kPageSize;
                CALICODB_EXPECT_EQ(size, page_count * kPageSize);
                if (m_file->resize(page_count)) {
                    return Status::no_memory();
                }
                return Status::ok();
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
        CALICODB_EXPECT_TRUE(m_filename.is_empty());
        if (append_strings(m_filename, filename)) {
            return Status::no_memory();
        }

        file_out = new (std::nothrow) TempFile(m_file);
        return file_out ? Status::ok() : Status::no_memory();
    }

    auto file_size(const char *filename, size_t &size_out) const -> Status override
    {
        if (file_exists(filename)) {
            size_out = m_file.pages.len() * kPageSize;
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
               Slice(m_filename.c_str(), m_filename.length()) == Slice(filename);
    }

    auto srand(unsigned seed) -> void override
    {
        m_rng.seed(seed);
    }

    auto rand() -> unsigned override
    {
        return std::uniform_int_distribution<unsigned>()(m_rng);
    }

    auto sleep(unsigned micros) -> void override
    {
        Env::default_env().sleep(micros);
    }

private:
    friend class TempWal;

    std::default_random_engine m_rng;
    String m_filename;

    struct PagedFile final {
        Buffer<char *> pages;

        ~PagedFile()
        {
            [[maybe_unused]] const auto rc = resize(0);
            CALICODB_EXPECT_EQ(rc, 0);
        }

        [[nodiscard]] auto resize(size_t new_len) -> int
        {
            // Free pages if shrinking the file.
            const auto old_len = pages.len();
            for (size_t i = new_len; i < old_len; ++i) {
                Alloc::deallocate(pages.ptr()[i]);
                // Clear pointers in case realloc() fails.
                pages.ptr()[i] = nullptr;
            }
            // Resize the page pointer array.
            if (pages.realloc(new_len)) {
                // Alloc::reallocate() might fail when trimming an allocation, but not if the new size
                // is 0. In that case, the underlying reallocation function is not called. Instead, the
                // memory is freed using Alloc::deallocate(), and a pointer (non-null) is returned to a
                // zero-length allocation (see alloc.h).
                CALICODB_EXPECT_NE(new_len, 0);
                return -1;
            }
            // Allocate pages if growing the file.
            for (size_t i = old_len; i < new_len; ++i) {
                auto *&page = pages.ptr()[i];
                if (auto *ptr = Alloc::allocate(kPageSize)) {
                    std::memset(ptr, 0, kPageSize);
                    page = static_cast<char *>(ptr);
                } else {
                    // Clear the rest of the pointers so the destructor doesn't mess up.
                    std::memset(page, 0, (new_len - i) * sizeof(char *));
                    return -1;
                }
            }
            return 0;
        }

        [[nodiscard]] auto ensure_large_enough(size_t len) -> int
        {
            const auto num_pages = pages.len();
            if (len > num_pages && resize(len)) {
                return -1;
            }
            CALICODB_EXPECT_LE(len, pages.len());
            return 0;
        }

        [[nodiscard]] auto fetch_page(Id id) -> char *
        {
            if (id.value > pages.len()) {
                return nullptr;
            }
            return pages.ptr()[id.as_index()];
        }
    } m_file;
};

// In-memory WAL
// This WAL implementation only needs to save the most-recent version of each page (we
// can only rollback once, and there are no other connections).
class TempWal : public Wal
{
public:
    [[nodiscard]] static auto create(const Wal::Parameters &param) -> TempWal *
    {
        auto *wal = Alloc::new_object<TempWal>(
            reinterpret_cast<TempEnv &>(*param.env), *param.stat);
        if (wal->m_table.grow()) {
            Alloc::deallocate(wal);
            return nullptr;
        }
        return wal;
    }

    explicit TempWal(TempEnv &env, Stat &stat)
        : m_env(&env),
          m_stat(&stat)
    {
    }

    ~TempWal() override
    {
        m_table.clear();
    }

    auto start_reader(bool &changed) -> Status override
    {
        changed = false;
        return Status::ok();
    }

    auto read(Id page_id, char *&page) -> Status override
    {
        const auto itr = m_table.find(page_id.value);
        if (itr != m_table.end() && *itr) {
            std::memcpy(page, (*itr)->page, kPageSize);
            m_stat->counters[Stat::kReadWal] += kPageSize;
        } else {
            page = nullptr;
        }
        return Status::ok();
    }

    auto start_writer() -> Status override
    {
        return Status::ok();
    }

    auto write(PageRef *first_ref, size_t db_size) -> Status override
    {
        auto *dirty = &first_ref->dirty_hdr;
        for (auto *p = dirty; p; p = p->dirty) {
            if (m_table.occupied * 2 >= m_table.data.len()) {
                if (m_table.grow()) {
                    return Status::no_memory();
                }
            }
            auto *ref = p->get_page_ref();
            auto **itr = m_table.find(ref->page_id.value);
            if (*itr == nullptr) {
                *itr = PageEntry::create(ref->page_id.value);
                if (*itr == nullptr) {
                    return Status::no_memory();
                }
                ++m_table.occupied;
            }
            std::memcpy((*itr)->page, ref->data, kPageSize);
            m_stat->counters[Stat::kWriteWal] += kPageSize;
        }
        if (db_size && commit(db_size)) {
            return Status::no_memory();
        }
        return Status::ok();
    }

    auto rollback(const Undo &undo, void *object) -> void override
    {
        // This routine will call undo() on frames in a different order than the normal WAL
        // class. This shouldn't make a difference to the pager (the only caller).
        m_table.for_each([undo, object](auto &page) {
            undo(object, Id(page->key));
            return 0;
        });
    }

    auto finish_writer() -> void override
    {
        m_table.clear();
    }

    auto finish_reader() -> void override
    {
        CALICODB_EXPECT_EQ(m_table.occupied, 0);
    }

    auto close() -> Status override
    {
        return Status::ok();
    }

    auto checkpoint(bool) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto last_frame_count() const -> size_t override
    {
        return 0;
    }

    [[nodiscard]] auto db_size() const -> uint32_t override
    {
        return static_cast<uint32_t>(m_env->m_file.pages.len());
    }

private:
    [[nodiscard]] auto commit(size_t db_size) -> int
    {
        const auto rc = m_table.for_each([this](auto *page) {
            if (m_env->m_file.ensure_large_enough(page->key)) {
                return -1;
            }
            auto *dst = m_env->m_file.fetch_page(Id(page->key));
            std::memcpy(dst, page->page, kPageSize);
            m_stat->counters[Stat::kReadWal] += kPageSize;
            m_stat->counters[Stat::kWriteDB] += kPageSize;
            return 0;
        });
        if (rc || m_env->m_file.resize(db_size)) {
            return -1;
        }
        m_table.clear();
        return 0;
    }

    struct PageEntry {
        uint32_t key;
        char page[kPageSize];

        static auto create(uint32_t key) -> PageEntry *
        {
            auto *ptr = static_cast<PageEntry *>(
                Alloc::allocate(sizeof(PageEntry)));
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
                auto *&ptr = data.ptr()[i];
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
                auto *&ptr = data.ptr()[h & (data.len() - 1)];
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
            data = std::move(table.data);
            return 0;
        }

        auto clear() -> void
        {
            for_each([this](auto *&page) {
                Alloc::deallocate(page);
                page = nullptr;
                --occupied;
                return 0;
            });
        }
    } m_table;

    TempEnv *const m_env;
    Stat *const m_stat;
};

} // namespace

auto new_temp_env() -> Env *
{
    return new (std::nothrow) TempEnv;
}

auto new_temp_wal(const Wal::Parameters &param) -> Wal *
{
    return TempWal::create(param);
}

} // namespace calicodb