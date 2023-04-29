// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tools.h"
#include "encoding.h"
#include "env_posix.h"
#include "logging.h"
#include <algorithm>
#include <iomanip>
#include <iostream>

namespace calicodb::tools
{

RandomGenerator::RandomGenerator(std::size_t size)
    : m_data(size, '\0'),
      m_rng(42)
{
    std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine(m_rng);
    std::generate(begin(m_data), end(m_data), std::ref(engine));
}

auto RandomGenerator::Generate(std::size_t len) const -> Slice
{
    if (m_pos + len > m_data.size()) {
        m_pos = 0;
        CALICODB_EXPECT_LT(len, m_data.size());
    }
    m_pos += len;
    return {m_data.data() + m_pos - len, static_cast<std::size_t>(len)};
}

auto print_references(Pager &pager) -> void
{
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        std::cerr << std::setw(6) << page_id.value << ": ";
        if (PointerMap::lookup(page_id) == page_id) {
            std::cerr << "pointer map\n";
            continue;
        }
        if (page_id.is_root()) {
            std::cerr << "NULL <- node -> ...\n";
            continue;
        }
        PointerMap::Entry entry;
        CHECK_OK(PointerMap::read_entry(pager, page_id, entry));

        Page page;
        CHECK_OK(pager.acquire(page_id, page));

        switch (entry.type) {
            case PointerMap::kTreeNode:
                std::cerr << entry.back_ptr.value << " -> node -> ...\n";
                break;
            case PointerMap::kTreeRoot:
                std::cerr << "1 -> root for table " << entry.back_ptr.value << " -> ...\n";
                break;
            case PointerMap::kFreelistLink:
                std::cerr << entry.back_ptr.value << " -> freelist link -> " << get_u32(page.data()) << '\n';
                break;
            case PointerMap::kOverflowHead:
                std::cerr << entry.back_ptr.value << " -> overflow head -> " << get_u32(page.data()) << '\n';
                break;
            case PointerMap::kOverflowLink:
                std::cerr << entry.back_ptr.value << " -> overflow link -> " << get_u32(page.data()) << '\n';
                break;
        }
        pager.release(std::move(page));
    }
}

#undef TRY_INTERCEPT_FROM

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    const auto s = env.file_size(filename, file_size);
    if (s.is_not_found()) {
        // File was unlinked.
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    CHECK_OK(env.new_file(filename, Env::kCreate, file));
    CHECK_OK(file->read_exact(0, file_size, buffer.data()));
    delete file;

    return buffer;
}

auto write_string_to_file(Env &env, const std::string &filename, const std::string &buffer, long offset) -> void
{
    File *file;
    CHECK_OK(env.new_file(filename, Env::kCreate, file));

    std::size_t write_pos;
    if (offset < 0) {
        CHECK_OK(env.file_size(filename, write_pos));
    } else {
        write_pos = offset;
    }
    CHECK_OK(file->write(write_pos, buffer));
    CHECK_OK(file->sync());
    delete file;
}

auto assign_file_contents(Env &env, const std::string &filename, const std::string &contents) -> void
{
    CHECK_OK(env.resize_file(filename, 0));
    write_string_to_file(env, filename, contents, 0);
}

auto hexdump_page(const Page &page) -> void
{
    std::fprintf(stderr, "%u:\n", page.id().value);
    for (std::size_t i = 0; i < kPageSize / 16; ++i) {
        std::fputs("    ", stderr);
        for (std::size_t j = 0; j < 16; ++j) {
            const auto c = page.data()[i * 16 + j];
            if (std::isprint(c)) {
                std::fprintf(stderr, "%2c ", c);
            } else {
                std::fprintf(stderr, "%02X ", std::uint8_t(c));
            }
        }
        std::fputc('\n', stderr);
    }
}

auto fill_db(DB &db, const std::string &tablename, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Txn *txn;
    CHECK_OK(db.start(true, txn));
    auto records = fill_db(*txn, tablename, random, num_records, max_payload_size);
    CHECK_OK(txn->commit());
    db.finish(txn);
    return records;
}

auto fill_db(Txn &txn, const std::string &tablename, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Table *table;
    CHECK_OK(txn.new_table(TableOptions(), tablename, table));
    auto records = fill_db(*table, random, num_records, max_payload_size);
    delete table;
    return records;
}

auto fill_db(Table &table, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    CHECK_TRUE(max_payload_size > 0);
    std::map<std::string, std::string> records;

    for (std::size_t i = 0; i < num_records; ++i) {
        const auto ksize = random.Next(1, max_payload_size);
        const auto vsize = random.Next(max_payload_size - ksize);
        const auto k = random.Generate(ksize);
        const auto v = random.Generate(vsize);
        CHECK_OK(table.put(k, v));
        records[k.to_string()] = v.to_string();
    }
    return records;
}

auto expect_db_contains(DB &db, const std::string &tablename, const std::map<std::string, std::string> &map) -> void
{
    Txn *txn;
    CHECK_OK(db.start(false, txn));
    expect_db_contains(*txn, tablename, map);
    db.finish(txn);
}

auto expect_db_contains(Txn &txn, const std::string &tablename, const std::map<std::string, std::string> &map) -> void
{
    Table *table;
    CHECK_OK(txn.new_table(TableOptions(), tablename, table));
    expect_db_contains(*table, map);
    delete table;
}

auto expect_db_contains(const Table &table, const std::map<std::string, std::string> &map) -> void
{
    for (const auto &[key, value] : map) {
        std::string result;
        CHECK_OK(table.get(key, &result));
        CHECK_EQ(result, value);
    }
}

FakeWal::FakeWal(const Parameters &param)
    : m_param(param),
      m_db_file(param.db_file)
{
}

auto FakeWal::read(Id page_id, char *&out) -> Status
{
    for (const auto &map : {m_pending, m_committed}) {
        const auto itr = map.find(page_id);
        if (itr != end(map)) {
            std::memcpy(out, itr->second.c_str(), kPageSize);
            return Status::ok();
        }
    }
    out = nullptr;
    return Status::ok();
}

auto FakeWal::write(const PageRef *dirty, std::size_t db_size) -> Status
{
    for (auto *p = dirty; p; p = p->next) {
        m_pending.insert_or_assign(p->page_id, std::string(p->page, kPageSize));
    }
    if (db_size) {
        for (const auto &[k, v] : m_pending) {
            m_committed.insert_or_assign(k, v);
        }
        m_pending.clear();
        m_db_size = db_size;
    }
    return Status::ok();
}

auto FakeWal::checkpoint(bool, std::size_t *db_size) -> Status
{
    // TODO: Need the env to resize the file.
    CALICODB_EXPECT_TRUE(m_pending.empty());
    // Write back to the DB sequentially.
    for (const auto &[page_id, page] : m_committed) {
        const auto offset = page_id.as_index() * kPageSize;
        CALICODB_TRY(m_db_file->write(offset, page));
    }
    if (db_size != nullptr) {
        *db_size = m_db_size;
    }
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::rollback() -> void
{
    m_pending.clear();
}

auto FakeWal::close(std::size_t &) -> Status
{
    m_pending.clear();
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::statistics() const -> WalStatistics
{
    return WalStatistics{};
}

auto open_with_txn(const TxnOpenOptions &options, TxnOpenResult &out) -> Status
{
    DB *db;
    CALICODB_TRY(DB::open(options.options, options.filename, db));

    const auto is_writer = options.mode >= TxnOpenOptions::kWriter;
    const auto needs_wait = options.mode == TxnOpenOptions::kWriterWait;

    Txn *txn;
    Status s;
    do {
        s = db->start(is_writer, txn);
    } while (needs_wait && s.is_busy());

    if (s.is_ok()) {
        out.db = db;
        out.txn = txn;
    } else {
        delete db;
    }
    return s;
}

} // namespace calicodb::tools
