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

auto TransferBatch::run(Txn &txn) -> Status
{
    Status s;
    for (std::size_t t = 0; s.is_ok() && t < num_tables; ++t) {
        if (s.is_ok()) {
            s = put_random(txn, tools::integral_key(t), num_records, round);
        }
        if (t) {
            s = erase_all(txn, tools::integral_key(t - 1), true);
        }
        if (s.is_ok()) {
            s = txn.vacuum();
        }
    }
    if (s.is_ok()) {
        s = check(txn, false);
    }
    return s;
}

auto TransferBatch::check(Txn &txn, bool validate) -> Status
{

    Table *table = nullptr;
    TableOptions tbopt{false, false};
    auto s = txn.new_table(tbopt, tools::integral_key(num_tables - 1), table);
    if (s.is_ok()) {
        s = check_records(*table, num_records, round);
    }
    if (validate && s.is_ok()) {
        txn_impl(&txn)->TEST_validate();
        table_impl(table)->tree()->TEST_validate();
    }
    delete table;
    return s;
}

auto make_record(U32 id, U32 iteration) -> std::pair<std::string, std::string>
{
    const auto num_pages = id % 2;
    const auto extra = id % kPageSize;
    auto key = tools::integral_key<32>(id);
    auto value = key;
    for (U32 i = 0; i < iteration; ++i) {
        key.append(key);
    }
    value.resize(num_pages * kPageSize + extra, '*');
    return {key, value};
}

auto put_random(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration) -> Status
{
    return with_table(txn, tbname, [num_records, iteration, &txn](auto &table) {
        return put_random(table, num_records, iteration);
    });
}
auto put_random(Table &table, std::size_t num_records, U32 iteration) -> Status
{
    std::default_random_engine rng(iteration);
    std::vector<U32> keys(num_records);
    std::iota(begin(keys), end(keys), 0);
    std::shuffle(begin(keys), end(keys), rng);

    for (auto k : keys) {
        const auto [key, value] = make_record(k, iteration);
        CALICODB_TRY(table.put(key, value));
    }
    return Status::ok();
}

auto put_sequential(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration) -> Status
{
    return with_table(txn, tbname, [iteration, num_records, &txn](auto &table) {
        return put_sequential(table, num_records, iteration);
    });
}
auto put_sequential(Table &table, std::size_t num_records, U32 iteration) -> Status
{
    for (U32 k = 0; k < num_records; ++k) {
        const auto [key, value] = make_record(
            iteration & 1 ? k : num_records - k - 1, iteration);
        CALICODB_TRY(table.put(key, value));
    }
    return Status::ok();
}

auto erase_all(Txn &txn, const std::string &tbname, bool drop_table) -> Status
{
    CALICODB_TRY(with_table(txn, tbname, [](auto &table) {
        return erase_all(table);
    }));
    if (drop_table) {
        CALICODB_TRY(txn.drop_table(tbname));
    }
    return Status::ok();
}
auto erase_all(Table &table) -> Status
{
    Status s;
    auto *c = table.new_cursor();
    for (U32 i = 0; s.is_ok(); ++i) {
        if (i & 1) {
            c->seek_first();
        } else {
            c->seek_last();
        }
        if (c->is_valid()) {
            s = table.erase(c->key());
        } else {
            s = c->status();
        }
    }
    delete c;
    if (s.is_not_found()) {
        return Status::ok();
    }
    return s;
}

auto check_records(Txn &txn, const std::string &tbname, std::size_t num_records, U32 iteration) -> Status
{
    return with_table(txn, tbname, [iteration, num_records](auto &table) {
        return check_records(table, num_records, iteration);
    });
}
auto check_records(Table &table, std::size_t num_records, U32 iteration) -> Status
{
    for (U32 k = 0; k < num_records; ++k) {
        std::string result;
        const auto [key, value] = make_record(
            iteration & 1 ? k : num_records - k - 1, iteration);
        CALICODB_TRY(table.get(key, &result));
        EXPECT_EQ(result, value);
    }
    return Status::ok();
}

auto is_empty(Txn &txn, const std::string &tbname) -> bool
{
    bool nonempty;
    EXPECT_OK(with_table(txn, tbname, [&nonempty](auto &table) {
        nonempty = is_empty(table);
        return Status::ok();
    }));
    return !nonempty;
}
auto is_empty(Table &table) -> bool
{
    auto *c = table.new_cursor();
    c->seek_first();
    const auto nonempty = c->is_valid();
    delete c;
    return !nonempty;
}

} // namespace calicodb::test
