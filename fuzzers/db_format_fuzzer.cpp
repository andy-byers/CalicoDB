// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// db_format_fuzzer: Fuzz the database file format using libFuzzer
//
// This fuzzer takes a CalicoDB database file as input. The database is opened,
// and some queries and modifications are performed. The fuzzer expects 2 buckets
// to be present: "b1" and "b2". Seed inputs should contain at least these 2
// buckets.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "fake_env.h"
#include "fuzzer.h"
#include "mem.h"
#include <memory>

namespace calicodb
{

class Fuzzer
{
    static constexpr auto *kFilename = "./MemDB";
    Options m_options;

public:
    explicit Fuzzer(Env &env)
    {
        env.srand(42);
        m_options.env = &env;
        m_options.page_size = kMinPageSize;
        CHECK_OK(default_env().new_logger("/tmp/format_log", m_options.info_log));
    }

    ~Fuzzer()
    {
        delete m_options.info_log;
    }

    auto consume_input(const Slice &data) -> void
    {
        // Write the fuzzer input to a file.
        File *file;
        CHECK_OK(m_options.env->new_file(kFilename, Env::kCreate, file));
        CHECK_OK(file->resize(data.size()));
        CHECK_OK(file->write(0, data));
        delete file;

        // Attempt to open the file as a database.
        DB *db;
        auto s = DB::open(m_options, kFilename, db);

        if (s.is_ok()) {
            s = db->update([](auto &tx) {
                TestBucket b1, b2;
                auto s = test_open_bucket(tx, "b1", b1);
                if (s.is_ok()) {
                    s = test_open_bucket(tx, "b2", b2);
                }
                if (s.is_ok()) {
                    auto c1 = test_new_cursor(*b1);
                    auto c2 = test_new_cursor(*b2);
                    // Copy all records from b1 to b2.
                    c1->seek_last();
                    while (c1->is_valid() && s.is_ok()) {
                        s = b2->put(c1->key(), c1->value());
                        if (s.is_ok()) {
                            c1->previous();
                        }
                    }
                    if (s.is_ok()) {
                        s = c1->status();
                    }
                    if (s.is_ok()) {
                        // Copy reverse mapping from b2 to b1.
                        c2->seek_first();
                        while (c2->is_valid() && s.is_ok()) {
                            s = b1->put(c2->value(), c2->key());
                            c2->next();
                        }
                        if (s.is_ok()) {
                            s = c2->status();
                        }
                        // Erase some records from b2.
                        c2->seek_first();
                        while (c2->is_valid() && s.is_ok()) {
                            if (c2->key() < c2->value()) {
                                s = b2->erase(*c2);
                                CHECK_TRUE(s == c2->status());
                            } else {
                                c2->next();
                            }
                        }
                        if (s.is_ok()) {
                            s = c2->status();
                        }
                    }
                    c1.reset();
                    c2.reset();

                    if (s.is_ok()) {
                        s = tx.main_bucket().drop_bucket("b2");
                    }
                    if (s.is_ok()) {
                        s = tx.vacuum();
                    }
                }
                return s;
            });

            if (s.is_ok()) {
                s = db->checkpoint(kCheckpointRestart, nullptr);
            }

            delete db;

            if (s.is_corruption()) {
#ifdef INTEGRITY_CHECK
                CHECK_OK(s);
#endif
            }
        }

        CHECK_TRUE(
            s.is_ok() ||               // Database is valid (or corruption was not detected)
            s.is_invalid_argument() || // Not a CalicoDB database
            s.is_no_memory() ||        // Key or value larger than kMaxAllocation
            s.is_corruption());        // Corruption was detected
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    CHECK_OK(configure(kSetAllocator, DebugAllocator::config()));
    {
        FakeEnv env;
        Fuzzer fuzzer(env);
        fuzzer.consume_input(Slice(reinterpret_cast<const char *>(data), size));
    }
    CHECK_EQ(DebugAllocator::bytes_used(), 0);
    return 0;
}

} // namespace calicodb
