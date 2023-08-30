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
#include <chrono>
#include <random>
#include <thread>
#include <unordered_map>

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
        std::this_thread::sleep_for(
            std::chrono::microseconds(micros));
    }

private:
    friend class TempWal;

    std::default_random_engine m_rng;
    String m_filename;

    struct PagedFile final {
        UniqueBuffer<char *> pages;

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

class TempWal : public Wal
{
public:
    explicit TempWal(TempEnv &env, Stat &stat)
        : m_env(&env),
          m_stat(&stat)
    {
    }

    ~TempWal() override = default;

    auto start_reader(bool &changed) -> Status override
    {
        changed = false;
        return Status::ok();
    }

    auto read(Id page_id, char *&page) -> Status override
    {
        if (m_pages.empty()) {
            // This is the usual case: pages are only stored here if they were written during
            // the current read-write transaction. Otherwise, they will be located in the Env.
            page = nullptr;
            return Status::ok();
        }
        const auto itr = m_pages.find(page_id);
        if (itr != end(m_pages)) {
            std::memcpy(page, itr->second.data(), kPageSize);
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
            auto *ref = p->get_page_ref();
            m_pages.insert_or_assign(ref->page_id, std::string(ref->data, kPageSize));
            m_stat->counters[Stat::kWriteWal] += kPageSize;
        }
        if (db_size > 0) {
            if (write_back_committed() ||
                m_env->m_file.resize(db_size)) {
                return Status::no_memory();
            }
            m_pages.clear();
        }
        return Status::ok();
    }

    auto rollback(const Undo &undo, void *object) -> void override
    {
        // This routine will call undo() on frames in a different order than the normal WAL
        // class. This shouldn't make a difference to the pager (the only caller).
        for (const auto &[id, _] : m_pages) {
            undo(object, id);
        }
        m_pages.clear();
    }

    auto finish_writer() -> void override
    {
        // Caller should have called commit(), if not rollback().
        CALICODB_EXPECT_TRUE(m_pages.empty());
        m_pages.clear(); // Just to make sure.
    }

    auto finish_reader() -> void override {}

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
    [[nodiscard]] auto write_back_committed() -> int
    {
        for (const auto &[id, src] : m_pages) {
            if (m_env->m_file.ensure_large_enough(id.value)) {
                return -1;
            }
            auto *dst = m_env->m_file.fetch_page(id);
            std::memcpy(dst, src.data(), kPageSize);
            m_stat->counters[Stat::kReadWal] += kPageSize;
            m_stat->counters[Stat::kWriteDB] += kPageSize;
        }
        return 0;
    }

    std::unordered_map<Id, std::string, Id::Hash> m_pages;
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
    return Alloc::new_object<TempWal>(reinterpret_cast<TempEnv &>(*param.env), *param.stat);
}

} // namespace calicodb