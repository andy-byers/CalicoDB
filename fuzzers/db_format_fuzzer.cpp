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
    Options m_options;
    std::string m_filename;

public:
    explicit Fuzzer(std::string filename)
        : m_filename(std::move(filename))
    {
        Env::default_env().srand(42);
    }

    auto fuzz(const Slice &data) -> void
    {
        // Write the fuzzer input to a file.
        File *file;
        CHECK_OK(Env::default_env().new_file(m_filename, Env::kCreate, file));
        CHECK_OK(file->write(0, data));
        CHECK_OK(Env::default_env().resize_file(m_filename, data.size()));
        delete file;

        // Attempt to open the file as a database.
        DB *db;
        auto s = DB::open(Options(), m_filename, db);

        if (s.is_ok()) {
            // Scan the whole database.
            s = db->view([](const auto &tx) {
                Status s;
                auto &schema = tx.schema();
                schema.seek_first();
                while (s.is_ok() && schema.is_valid()) {
                    Bucket b;
                    s = tx.open_bucket(schema.key(), b);
                    if (s.is_ok()) {
                        auto *c = tx.new_cursor(b);
                        c->seek_first();
                        while (c->is_valid()) {
                            c->next();
                        }
                        s = c->status();
                        delete c;
                        schema.next();
                    }
                    if (s.is_ok()) {
                        s = schema.status();
                    }
                }
                return s;
            });

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
    Fuzzer fuzzer("/tmp/calicodb_db_format_fuzzer");
    fuzzer.fuzz(Slice(reinterpret_cast<const char *>(data), size));
    return 0;
}

} // namespace calicodb