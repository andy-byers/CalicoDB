// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "common.h"
#include "logging.h"
#include "test.h"

// TODO
#include "db_impl.h"
#include "pager.h"

namespace calicodb::test
{

auto print_database_overview(std::ostream &os, Pager &pager) -> void
{
#define SEP "|-----------|-----------|----------------|---------------------------------|\n"

    if (pager.page_count() == 0) {
        os << "DB is empty\n";
        return;
    }
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        if (page_id.as_index() % 32 == 0) {
            os << SEP "|    PageID |  ParentID | PageType       | Info                            |\n" SEP;
        }
        Id parent_id;
        std::string info, type;
        if (PointerMap::is_map(page_id)) {
            const auto first = page_id.value + 1;
            append_fmt_string(info, "Range=[%u,%u]", first, first + kPageSize / 5 - 1);
            type = "<PtrMap>";
        } else {
            PointerMap::Entry entry;
            if (page_id.is_root()) {
                entry.type = PointerMap::kTreeRoot;
            } else {
                ASSERT_OK(PointerMap::read_entry(pager, page_id, entry));
                parent_id = entry.back_ptr;
            }
            Page page;
            ASSERT_OK(pager.acquire(page_id, page));

            switch (entry.type) {
                case PointerMap::kTreeRoot:
                    type = "TreeRoot";
                    [[fallthrough]];
                case PointerMap::kTreeNode: {
                    NodeHeader hdr;
                    hdr.read(page.constant_ptr() + page_id.is_root() * FileHeader::kSize);
                    auto n = hdr.cell_count;
                    if (hdr.is_external) {
                        append_fmt_string(info, "Ex,N=%u,Sib=(%u,%u)", n, hdr.prev_id.value, hdr.next_id.value);
                    } else {
                        info = "In,N=";
                        append_number(info, n);
                        ++n;
                    }
                    if (type.empty()) {
                        type = "TreeNode";
                    }
                    break;
                }
                case PointerMap::kFreelistLeaf:
                    type = "Unused";
                    break;
                case PointerMap::kFreelistTrunk:
                    append_fmt_string(
                        info, "N=%u,Next=%u", get_u32(page.constant_ptr() + 4), get_u32(page.constant_ptr()));
                    type = "Freelist";
                    break;
                case PointerMap::kOverflowHead:
                    append_fmt_string(info, "Next=%u", get_u32(page.constant_ptr()));
                    type = "OvflHead";
                    break;
                case PointerMap::kOverflowLink:
                    append_fmt_string(info, "Next=%u", get_u32(page.constant_ptr()));
                    type = "OvflLink";
                    break;
                default:
                    type = "<BadType>";
            }
            pager.release(std::move(page));
        }
        std::string line;
        append_fmt_string(
            line,
            "|%10u |%10u | %-15s| %-32s|\n",
            page_id.value,
            parent_id.value,
            type.c_str(),
            info.c_str());
        os << line;
    }
    os << SEP;
#undef SEP
}

#define MAYBE_CRASH(target)                              \
    do {                                                 \
        if ((target)->should_next_syscall_fail()) {      \
            return Status::io_error("<injected_fault>"); \
        }                                                \
    } while (0)

class CrashEnv : public EnvWrapper
{
public:
    mutable int m_max_num = 0;
    mutable int m_num = 0;
    bool m_enabled = false;

    explicit CrashEnv(Env &env)
        : EnvWrapper(env)
    {
    }

    ~CrashEnv() override = default;

    [[nodiscard]] auto should_next_syscall_fail() const -> bool
    {
        if (m_enabled && m_num++ >= m_max_num) {
            m_num = 0;
            ++m_max_num;
            return true;
        }
        return false;
    }

    auto remove_file(const std::string &filename) -> Status override
    {
        MAYBE_CRASH(this);
        return target()->remove_file(filename);
    }

    auto resize_file(const std::string &filename, std::size_t file_size) -> Status override
    {
        MAYBE_CRASH(this);
        return target()->resize_file(filename, file_size);
    }

    auto new_file(const std::string &filename, OpenMode mode, File *&file_out) -> Status override
    {
        MAYBE_CRASH(this);

        class CrashFile : public FileWrapper
        {
            CrashEnv *m_env;

        public:
            explicit CrashFile(CrashEnv &env, File &base)
                : FileWrapper(base),
                  m_env(&env)
            {
            }

            ~CrashFile() override
            {
                delete m_target;
            }

            auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::read(offset, size, scratch, out);
            }

            auto write(std::size_t offset, const Slice &in) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::write(offset, in);
            }

            auto sync() -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::sync();
            }

            auto file_lock(FileLockMode mode) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::file_lock(mode);
            }

            auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override
            {
                MAYBE_CRASH(m_env);
                return FileWrapper::shm_map(r, extend, out);
            }

            auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status override
            {
                if (flags & kShmLock) {
                    MAYBE_CRASH(m_env);
                }
                return FileWrapper::shm_lock(r, n, flags);
            }
        };
        auto s = target()->new_file(filename, mode, file_out);
        if (s.is_ok()) {
            file_out = new CrashFile(*this, *file_out);
        }
        return s;
    }
};

#undef MAYBE_CRASH

class TestCrashes : public testing::Test
{
protected:
    std::string m_filename;
    CrashEnv *m_env;

    explicit TestCrashes()
        : m_filename(testing::TempDir() + "crashes"),
          m_env(new CrashEnv(*Env::default_env()))
    {
    }

    ~TestCrashes() override = default;

    static constexpr std::size_t kNumRecords = 512;
    static constexpr std::size_t kNumIterations = 3;
    [[nodiscard]] static auto make_key(std::size_t n) -> Slice
    {
        static std::string s_keys[kNumRecords];
        if (s_keys[n].empty()) {
            s_keys[n] = numeric_key(n) + "::";
            // Let the keys get increasingly long so that the overflow chain code gets tested.
            s_keys[n].resize(s_keys[n].size() + n, '0');
        }
        return s_keys[n];
    }

    // Check if a fault was intentional
    [[nodiscard]] static auto is_injected_fault(const Status &s) -> bool
    {
        return s.to_string() == "I/O error: <injected_fault>";
    }
    // Ensure that all statuses contain injected faults
    template <class... S>
    static auto assert_injected_fault(const S &...s) -> void
    {
        ASSERT_TRUE((is_injected_fault(s) && ...));
    }

    [[nodiscard]] static auto writer_task(Tx &tx, std::size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        Status s;
        Bucket b1, b2;
        const auto name1 = std::to_string(iteration);
        const auto name2 = std::to_string((iteration + 1) % kNumIterations);

        s = tx.open_bucket(name1, b1);
        if (s.is_invalid_argument()) {
            BucketOptions options;
            options.error_if_exists = true;
            s = tx.create_bucket(options, name1, &b1);
            for (std::size_t i = 0; s.is_ok() && i < kNumRecords; ++i) {
                const auto key = make_key(i);
                s = tx.put(b1, key, key);
            }
        }
        if (!s.is_ok()) {
            assert_injected_fault(s, tx.status());
            return s;
        }
        s = tx.create_bucket(BucketOptions(), name2, &b2);
        if (!s.is_ok()) {
            assert_injected_fault(s, tx.status());
            return s;
        }

        auto *c = tx.new_cursor(b1);
        c->seek_first();
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            if (c->is_valid()) {
                EXPECT_EQ(c->key(), make_key(i));
                EXPECT_EQ(c->key(), c->value());
                s = tx.put(b2, c->key(), c->value());

                if (!s.is_ok()) {
                    break;
                }
                c->next();
            } else {
                s = c->status();
                break;
            }
        }
        delete c;

        if (s.is_ok()) {
            s = tx.drop_bucket(name1);
        }
        if (s.is_ok()) {
            s = tx.vacuum();
        }
        if (s.is_ok()) {
            EXPECT_OK(tx.status());
        } else {
            assert_injected_fault(s, tx.status());
        }
        return s;
    }

    static auto reader_task(const Tx &tx, std::size_t iteration) -> Status
    {
        EXPECT_OK(tx.status());

        std::string b_name;
        auto &schema = tx.schema();
        schema.seek_first();
        if (schema.is_valid()) {
            b_name = schema.key().to_string();
            EXPECT_EQ(b_name, std::to_string((iteration + 1) % kNumIterations));
        } else {
            assert_injected_fault(schema.status());
            return schema.status();
        }

        Bucket b;
        auto s = tx.open_bucket(b_name, b);
        if (!s.is_ok()) {
            assert_injected_fault(s);
            return s;
        }
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            const auto key = make_key(i);
            std::string value;
            s = tx.get(b, key, &value);

            if (s.is_ok()) {
                EXPECT_EQ(key, value);
            } else {
                assert_injected_fault(s);
                return s;
            }
        }
        auto *c = tx.new_cursor(b);
        c->seek_first();
        for (std::size_t i = 0; i < kNumRecords; ++i) {
            if (c->is_valid()) {
                EXPECT_EQ(c->key(), make_key(i));
            } else {
                s = c->status();
                break;
            }
            c->next();
        }
        EXPECT_FALSE(c->is_valid()) << "key = \"" << c->key().to_string() << '\"';
        delete c;

        if (!s.is_ok()) {
            assert_injected_fault(s);
        }
        return s;
    }

    auto run_until_completion(const std::function<Status()> &task) -> void
    {
        m_env->m_max_num = 0;
        while (!task().is_ok()) {
        }
    }

    static auto validate(DB &db)
    {
        db_impl(&db)->TEST_pager().assert_state();
    }

    struct Parameters {
        bool inject_faults = false;
        bool test_checkpoint = false;
    };
    auto run_operations_test(const Parameters &param) -> void
    {
        enum SourceLocation {
            kSrcOpen,
            kSrcUpdate,
            kSrcView,
            kSrcCheckpoint,
            kNumSrcLocations,
        };
        std::size_t src_counters[kNumSrcLocations] = {};

        std::cout << "TestCrashes::Operations({\n  .inject_faults = " << std::boolalpha << param.inject_faults
                  << ",\n  .test_checkpoint = " << param.test_checkpoint << ",\n})\n\n";

        Options options;
        options.env = m_env;

        (void)DB::destroy(options, m_filename);

        for (std::size_t i = 0; i < kNumIterations; ++i) {
            m_env->m_enabled = param.inject_faults;

            DB *db;
            run_until_completion([this, &options, &db, &src_counters] {
                ++src_counters[kSrcOpen];
                auto s = DB::open(options, m_filename, db);
                if (!s.is_ok()) {
                    assert_injected_fault(s);
                }
                return s;
            });
            validate(*db);

            run_until_completion([i, &db, &src_counters] {
                ++src_counters[kSrcUpdate];
                return db->update([i](auto &tx) {
                    return writer_task(tx, i);
                });
            });
            validate(*db);

            run_until_completion([i, &db, &src_counters] {
                ++src_counters[kSrcView];
                return db->view([i](const auto &tx) {
                    return reader_task(tx, i);
                });
            });
            validate(*db);

            if (param.test_checkpoint) {
                run_until_completion([&db, &src_counters] {
                    ++src_counters[kSrcCheckpoint];
                    return db->checkpoint(true);
                });
                validate(*db);
            }

            m_env->m_enabled = false;
            delete db;
        }

        std::cout << " Location       | Hits per iteration\n";
        std::cout << "----------------|--------------------\n";
        std::cout << " kOpenDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcOpen]) / kNumIterations << '\n';
        std::cout << " kUpdateDB      | " << std::setw(18) << static_cast<double>(src_counters[kSrcUpdate]) / kNumIterations << '\n';
        std::cout << " kViewDB        | " << std::setw(18) << static_cast<double>(src_counters[kSrcView]) / kNumIterations << '\n';
        std::cout << " kSrcCheckpoint | " << std::setw(18) << static_cast<double>(src_counters[kSrcCheckpoint]) / kNumIterations << '\n';
        std::cout << '\n';

        std::memset(src_counters, 0, sizeof(src_counters));
    }
};

TEST_F(TestCrashes, Operations)
{
    // Sanity check. No faults.
    run_operations_test({false, false});
    run_operations_test({false, true});

    // Run with fault injection.
    run_operations_test({true, false});
    run_operations_test({true, true});
}

} // namespace calicodb::test