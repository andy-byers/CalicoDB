// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include <gtest/gtest.h>

namespace calicodb::test
{

TransferBatch::TransferBatch(std::size_t ntab, std::size_t nrec)
    : num_records(nrec),
      num_tables(ntab)
{
    EXPECT_LT(0, num_records);
    EXPECT_LT(0, num_tables);
}

auto TransferBatch::make_record(std::size_t id) -> std::pair<std::string, std::string>
{
    const auto num_pages = id % 2;
    const auto extra = id % kPageSize;
    const auto key = tools::integral_key(id);
    auto value = key;
    value.resize(num_pages * kPageSize + extra, '*');
    return {key, value};
}

auto TransferBatch::run(Txn &txn) -> Status
{
    Status s;
    for (std::size_t t = 0; t <= num_tables; ++t) {
        if (s.is_ok()) {
            Table *current = nullptr;
            TableOptions tbopt{true, false};
            s = txn.new_table(tbopt, tools::integral_key(t % num_tables), current);
            for (std::size_t i = 0; s.is_ok() && i < num_records; ++i) {
                const auto [key, value] = make_record(round * num_records + i);
                s = current->put(key, value);
            }
            delete current;
        }
        if (t) {
            Table *previous;
            TableOptions tbopt{false, false};
            const auto tbname = tools::integral_key(t - 1);
            s = txn.new_table(tbopt, tbname, previous);
            if (s.is_ok()) {
                auto *c = previous->new_cursor();
                for (std::size_t i = 0; s.is_ok() && i < num_records; ++i) {
                    c->seek_last();
                    if (c->is_valid()) {
                        s = previous->erase(c->key());
                    } else {
                        s = c->status();
                    }
                }
                delete c;
                delete previous;
            }
            if (s.is_ok()) {
                s = txn.drop_table(tbname);
            }
        }
        if (s.is_ok()) {
            s = txn.vacuum();
        }
    }
    if (s.is_ok()) {
        s = check(txn);
    }
    return s;
}

auto TransferBatch::check(Txn &txn) -> Status
{
    txn_impl(&txn)->TEST_validate();

    Table *table = nullptr;
    TableOptions tbopt{false, false};
    auto s = txn.new_table(tbopt, tools::integral_key(0), table);
    if (s.is_ok()) {
        table_impl(table)->tree()->TEST_validate();
    }
    if (s.is_ok()) {
        auto *c = table->new_cursor();
        c->seek_first();
        for (std::size_t i = 0; i < num_records * (round + 1); ++i) {
            const auto [key, value] = make_record(i);
            EXPECT_TRUE(c->is_valid());
            EXPECT_EQ(key, c->key().to_string());
            EXPECT_EQ(value, c->value().to_string());
            c->next();
        }
        EXPECT_FALSE(c->is_valid());
        delete c;
    }
    delete table;
    return s;
}

} // namespace calicodb::test
