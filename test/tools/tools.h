
#ifndef CUB_TEST_TOOLS_TOOLS_H
#define CUB_TEST_TOOLS_TOOLS_H

#include <iomanip>
#include <iostream>
#include <vector>
#include "cub/common.h"
#include "random.h"
#include "cub/bytes.h"
#include "utils/identifier.h"
#include "utils/utils.h"
#include "wal/wal_record.h"

namespace cub {

class ITree;
class Node;

template<std::size_t Length = 6> auto make_key(Index key) -> std::string
{
    auto key_string = std::to_string(key);
    return std::string(Length - key_string.size(), '0') + key_string;
}

class TreeValidator {
public:
    explicit TreeValidator(ITree&);
    auto validate() -> void;

private:
    auto validate_sibling_connections() -> void;
    auto validate_parent_child_connections() -> void;
    auto validate_ordering() -> void;
    auto collect_keys() -> std::vector<std::string>;
    auto traverse_inorder(const std::function<void(Node&, Index)>&) -> void;
    auto traverse_inorder_helper(Node, const std::function<void(Node&, Index)>&) -> void;
    auto is_reachable(std::string) -> bool;

    ITree &m_tree;
};

class TreePrinter {
public:
    explicit TreePrinter(ITree&);
    auto print() -> void;

private:
    auto add_spaces_to_level(Size, Index) -> void;
    auto add_spaces_to_other_levels(Size, Index) -> void;
    auto print_aux(Node, Index) -> void;
    auto add_key_to_level(BytesView, Index) -> void;
    auto add_key_separator_to_level(Index) -> void;
    auto add_node_start_to_level(Index, Index) -> void;
    auto add_node_end_to_level(Index) -> void;
    auto make_key_token(BytesView) -> std::string;
    static auto make_key_separator_token() -> std::string;
    static auto make_node_start_token(Index) -> std::string;
    static auto make_node_end_token() -> std::string;
    auto ensure_level_exists(Index) -> void;

    std::vector<std::string> m_levels;
    ITree &m_tree;
};

class WALReader;
class WALRecord;
class SharedMemory;

class WALPrinter {
public:
    WALPrinter() = default;
    virtual ~WALPrinter() = default;

    auto print(const WALRecord&) -> void;
    auto print(WALReader&) -> void;
    auto print(const std::string&, Size) -> void;
    auto print(SharedMemory, Size) -> void;
};

[[maybe_unused]] inline auto hexdump(const Byte *data, Size size, Size indent = 0) -> void
{
    constexpr auto chunk_size{0x10UL};
    const auto chunk_count{size / chunk_size};
    const auto rest_size{size % chunk_size};
    const std::string spaces(static_cast<Size>(indent), ' ');

    auto emit_line = [data, spaces](Size i, Size line_size) {
        if (!line_size)
            return;
        auto offset{i * chunk_size};

        std::cout
            << spaces
            << std::hex
            << std::setw(8)
            << std::setfill('0')
            << std::uppercase
            << offset
            << ": ";

        for (Index j{}; j < line_size; ++j) {
            const auto byte{static_cast<uint8_t>(data[offset+j])};
            std::cout
                << std::hex
                << std::setw(2)
                << std::setfill('0')
                << std::uppercase
                << static_cast<uint32_t>(byte)
                << ' ';
        }
        std::cout << '\n';
    };
    Index i{};
    for (; i < chunk_count; ++i)
        emit_line(i, chunk_size);
    // Last line may be partially filled.
    emit_line(i, rest_size);
}

class RecordGenerator {
public:
    static unsigned default_seed;

    struct Parameters {
        Size min_key_size {5};
        Size max_key_size {10};
        Size min_value_size {5};
        Size max_value_size {10};
        unsigned seed {};
        bool are_batches_sorted {};
    };

    static auto generate(Size, Parameters) -> std::vector<Record>;
    static auto generate_unique(Size) -> std::vector<Record>;
};

template<class Db> auto insert_random_records(Db &db, Size n, RecordGenerator::Parameters param)
{
    for (const auto &[key, value]: RecordGenerator::generate(n, param))
        db.insert(_b(key), _b(value));
}

template<class Db> auto insert_random_unique_records(Db &db, Size n)
{
    while (n) {
        Size num_inserted {};
        for (const auto &[key, value]: RecordGenerator::generate_unique(n)) {
            if (!db.lookup(_b(key), true)) {
                db.insert(_b(key), _b(value));
                num_inserted++;
            }
        }
        n -= num_inserted;
    }
}

template<class Db, class F> auto traverse_db(Db &db, F &&f)
{
    if (auto cursor = db.get_cursor(); cursor.has_record()) {
        cursor.find_minimum();
        do {
            f(_s(cursor.key()), cursor.value());
        } while (cursor.increment());
    }
}

template<class Db> auto collect_records(Db &db) -> std::vector<Record>
{
    std::vector<Record> out;
    traverse_db(db, [&out](const std::string &key, const std::string &value) {
        out.emplace_back(Record {key, value});
    });
    return out;
}

class WALRecordGenerator {
public:
    explicit WALRecordGenerator(Size block_size)
        : m_block_size {block_size}
    {
        CUB_EXPECT_TRUE(is_power_of_two(block_size));
    }

    auto generate_small() -> WALRecord
    {
        const auto small_size = m_block_size / 0x10;
        const auto total_update_size = random.next_int(small_size, small_size * 2);
        const auto update_count = random.next_int(1UL, 5UL);
        const auto mean_update_size = total_update_size / update_count;
        return generate(mean_update_size, update_count);
    }

    auto generate_large() -> WALRecord
    {
        const auto large_size = m_block_size / 3 * 2;
        const auto total_update_size = random.next_int(large_size, large_size * 2);
        const auto update_count = random.next_int(1UL, 5UL);
        const auto mean_update_size = total_update_size / update_count;
        return generate(mean_update_size, update_count);
    }

    auto generate(Size mean_update_size, Size update_count) -> WALRecord
    {
        CUB_EXPECT_GT(mean_update_size, 0);
        constexpr Size page_count = 0x1000;
        const auto lower_bound = mean_update_size - mean_update_size/3;
        const auto upper_bound = mean_update_size + mean_update_size/3;
        const auto page_size = upper_bound;
        CUB_EXPECT_LE(page_size, std::numeric_limits<uint16_t>::max());

        m_snapshots_before.emplace_back(random.next_string(page_size));
        m_snapshots_after.emplace_back(random.next_string(page_size));
        std::vector<ChangedRegion> update {};

        for (Index i {}; i < update_count; ++i) {
            const auto size = random.next_int(lower_bound, upper_bound);
            const auto offset = random.next_int(page_size - size);

            update.emplace_back();
            update.back().offset = offset;
            update.back().before = _b(m_snapshots_before.back()).range(offset, size);
            update.back().after = _b(m_snapshots_after.back()).range(offset, size);
        }
        WALRecord record {{
            std::move(update),
            PID {static_cast<uint32_t>(random.next_int(page_count))},
            LSN::null(),
            LSN {static_cast<uint32_t>(m_payloads.size() + ROOT_ID_VALUE)},
        }};
        m_payloads.push_back(_s(record.payload().data()));
        return record;
    }

    auto validate_record(const WALRecord &record, LSN target_lsn) const -> void
    {
        CUB_EXPECT_EQ(record.lsn(), target_lsn);
        const auto payload = retrieve_payload(target_lsn);
        CUB_EXPECT_EQ(record.type(), WALRecord::Type::FULL);
        CUB_EXPECT_TRUE(record.payload().data() == _b(payload));
        CUB_EXPECT_TRUE(record.is_consistent());
    }

    [[nodiscard]] auto retrieve_payload(LSN lsn) const -> const std::string&
    {
        return m_payloads.at(lsn.as_index());
    }

    Random random {0};

private:
    std::vector<std::string> m_payloads;
    std::vector<std::string> m_snapshots_before;
    std::vector<std::string> m_snapshots_after;
    Size m_block_size;
};

} // cub

#endif // CUB_TEST_TOOLS_TOOLS_H
