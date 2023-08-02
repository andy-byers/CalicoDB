// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "fake_env.h"
#include "fuzzer.h"
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
    }

    auto fuzz(const Slice &data) -> void
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
                Bucket b1, b2;
                auto s = tx.open_bucket("b1", b1);
                if (s.is_ok()) {
                    s = tx.open_bucket("b2", b2);
                }
                if (s.is_ok()) {
                    // Copy all records from b1 to b2.
                    std::unique_ptr<Cursor> c(tx.new_cursor(b1));
                    c->seek_last();
                    while (c->is_valid() && s.is_ok()) {
                        s = tx.put(b2, c->key(), c->value());
                        if (s.is_ok()) {
                            c->previous();
                        }
                    }
                    if (s.is_ok()) {
                        s = c->status();
                    }
                    if (s.is_ok()) {
                        // Copy reverse mapping from b2 to b1.
                        c.reset(tx.new_cursor(b2));
                        c->seek_first();
                        while (c->is_valid() && s.is_ok()) {
                            s = tx.put(b1, c->value(), c->key());
                            c->next();
                        }
                        if (s.is_ok()) {
                            s = c->status();
                        }
                        // Erase some records from b2.
                        c->seek_first();
                        while (c->is_valid() && s.is_ok()) {
                            if (c->key() < c->value()) {
                                s = tx.erase(*c);
                                CHECK_TRUE(s == c->status());
                            } else {
                                c->next();
                            }
                        }
                        if (s.is_ok()) {
                            s = c->status();
                        }
                    }
                    c.reset();

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
            s.is_corruption());        // Corruption was detected
    }
};

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    FakeEnv env;
    Fuzzer fuzzer(env);
    fuzzer.fuzz(Slice(reinterpret_cast<const char *>(data), size));
    return 0;
}

} // namespace calicodb
