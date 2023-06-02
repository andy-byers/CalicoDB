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
                CHECK_OK(PointerMap::read_entry(pager, page_id, entry));
                parent_id = entry.back_ptr;
            }
            Page page;
            CHECK_OK(pager.acquire(page_id, page));

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

#undef TRY_INTERCEPT_FROM

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    const auto s = env.file_size(filename, file_size);
    if (s.is_io_error()) {
        // File was unlinked.
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    CHECK_OK(env.new_file(filename, Env::kReadOnly, file));
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
            const auto c = page.constant_ptr()[i * 16 + j];
            if (std::isprint(c)) {
                std::fprintf(stderr, "%2c ", c);
            } else {
                std::fprintf(stderr, "%02X ", std::uint8_t(c));
            }
        }
        std::fputc('\n', stderr);
    }
}

auto fill_db(DB &db, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Tx *tx;
    CHECK_OK(db.new_tx(WriteTag{}, tx));
    auto records = fill_db(*tx, bname, random, num_records, max_payload_size);
    CHECK_OK(tx->commit());
    delete tx;
    return records;
}

auto fill_db(Tx &tx, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Bucket b;
    CHECK_OK(tx.create_bucket(BucketOptions(), bname, &b));
    auto records = fill_db(tx, b, random, num_records, max_payload_size);
    return records;
}

auto fill_db(Tx &tx, const Bucket &b, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    CHECK_TRUE(max_payload_size > 0);
    std::map<std::string, std::string> records;

    for (std::size_t i = 0; i < num_records; ++i) {
        const auto ksize = random.Next(1, max_payload_size);
        const auto vsize = random.Next(max_payload_size - ksize);
        const auto k = random.Generate(ksize);
        const auto v = random.Generate(vsize);
        CHECK_OK(tx.put(b, k, v));
        records[k.to_string()] = v.to_string();
    }
    return records;
}

auto expect_db_contains(DB &db, const std::string &bname, const std::map<std::string, std::string> &map) -> void
{
    const Tx *tx;
    CHECK_OK(db.new_tx(tx));
    expect_db_contains(*tx, bname, map);
    delete tx;
}

auto expect_db_contains(const Tx &tx, const std::string &bname, const std::map<std::string, std::string> &map) -> void
{
    Bucket b;
    CHECK_OK(tx.open_bucket(bname, b));
    expect_db_contains(tx, b, map);
}

auto expect_db_contains(const Tx &tx, const Bucket &b, const std::map<std::string, std::string> &map) -> void
{
    for (const auto &[key, value] : map) {
        std::string result;
        CHECK_OK(tx.get(b, key, &result));
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

auto FakeWal::write(PageRef *dirty, std::size_t db_size) -> Status
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

auto FakeWal::checkpoint(bool reset) -> Status
{
    // TODO: Need the env to resize the file.
    CALICODB_EXPECT_TRUE(m_pending.empty());
    // Write back to the DB sequentially.
    for (const auto &[page_id, page] : m_committed) {
        const auto offset = page_id.as_index() * kPageSize;
        CALICODB_TRY(m_db_file->write(offset, page));
    }
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::rollback(const Undo &undo) -> void
{
    for (const auto &[id, page] : m_pending) {
        undo(id);
    }
    m_pending.clear();
}

auto FakeWal::close() -> Status
{
    m_pending.clear();
    m_committed.clear();
    return Status::ok();
}

} // namespace calicodb::tools
