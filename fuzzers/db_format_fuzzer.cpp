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
            s = db->run(WriteOptions(), [](auto &tx) {
                TestCursor c1;
                TestCursor c2;
                auto s = test_open_bucket(tx, "b1", c1);
                if (s.is_ok()) {
                    s = test_open_bucket(tx, "b2", c2);
                }
                if (s.is_ok()) {
                    // Copy all records from b1 to b2.
                    c1->seek_last();
                    while (c1->is_valid() && s.is_ok()) {
                        s = tx.put(*c2, c1->key(), c1->value());
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
                            s = tx.put(*c1, c2->value(), c2->key());
                            c2->next();
                        }
                        if (s.is_ok()) {
                            s = c2->status();
                        }
                        // Erase some records from c2.
                        c2->seek_first();
                        while (c2->is_valid() && s.is_ok()) {
                            if (c2->key() < c2->value()) {
                                s = tx.erase(*c2);
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
                        s = tx.drop_bucket("b2");
                    }
                    if (s.is_ok()) {
                        s = tx.vacuum();
                    }
                }
                return s;
            });

            if (s.is_ok()) {
                s = db->checkpoint(true);
            }

            delete db;
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
