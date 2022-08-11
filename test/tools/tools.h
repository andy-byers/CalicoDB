
#ifndef CCO_TEST_TOOLS_TOOLS_H
#define CCO_TEST_TOOLS_TOOLS_H

#include <iomanip>
#include <iostream>
#include <vector>
#include "random.h"
#include "calico/calico.h"
#include "page/node.h"
#include "utils/identifier.h"
#include "utils/utils.h"
#include "wal/wal_record.h"

namespace cco {

class BPlusTree;

namespace tools {

    template<class T>
    auto find_exact(T &t, const std::string &key) -> Cursor
    {
        return t.find_exact(stob(key));
    }

    template<class T>
    auto find(T &t, const std::string &key) -> Cursor
    {
        return t.find(stob(key));
    }

    template<class T>
    auto contains(T &t, const std::string &key) -> bool
    {
        return find_exact(t, key).is_valid();
    }

    template<class T>
    auto contains(T &t, const std::string &key, const std::string &value) -> bool
    {
        if (auto c = find_exact(t, key); c.is_valid())
            return c.value() == value;
        return false;
    }

    template<class T>
    auto insert(T &t, const std::string &key, const std::string &value) -> bool
    {
        auto was_inserted = t.insert(stob(key), stob(value));
        CCO_EXPECT_TRUE(was_inserted.has_value());
        return was_inserted.value();
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(find_exact(t, key));
        CCO_EXPECT_TRUE(was_erased.has_value());
        return was_erased.value();
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(find_exact(t, key));
        CCO_EXPECT_TRUE(was_erased.has_value());
        if (was_erased.value())
            return true;
        auto cursor = t.find_minimum();
        CCO_EXPECT_EQ(cursor.error(), std::nullopt);
        if (!cursor.is_valid())
            return false;
        was_erased = t.erase(cursor);
        CCO_EXPECT_TRUE(was_erased.value());
        return true;
    }

} // tools

template<std::size_t Length = 20> auto make_key(Index key) -> std::string
{
    auto key_string = std::to_string(key);
    return std::string(Length - key_string.size(), '0') + key_string;
}

class TreeValidator {
public:
    explicit TreeValidator(BPlusTree &);
    auto validate() -> void;

private:
    auto validate_sibling_connections() -> void;
    auto validate_parent_child_connections() -> void;
    auto validate_ordering() -> void;
    auto collect_keys() -> std::vector<std::string>;
    auto traverse_inorder(const std::function<void(Node&, Index)>&) -> void;
    auto traverse_inorder_helper(Node, const std::function<void(Node&, Index)>&) -> void;
    auto is_reachable(std::string) -> bool;

    BPlusTree &m_tree;
};

class TreePrinter {
public:
    explicit TreePrinter(BPlusTree &, bool = true);
    auto print(Size = 0) -> void;

private:
    auto add_spaces_to_level(Size, Index) -> void;
    auto add_spaces_to_other_levels(Size, Index) -> void;
    auto print_aux(Node, Index) -> void;
    auto add_key_to_level(BytesView, Index, bool) -> void;
    auto add_key_separator_to_level(Index) -> void;
    auto add_node_start_to_level(Index, Index) -> void;
    auto add_node_end_to_level(Index) -> void;
    auto make_key_token(BytesView) -> std::string;
    static auto make_key_separator_token() -> std::string;
    static auto make_node_start_token(Index) -> std::string;
    static auto make_node_end_token() -> std::string;
    auto ensure_level_exists(Index) -> void;

    std::vector<std::string> m_levels;
    BPlusTree &m_tree;
    bool m_has_integer_keys {};
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
        Size mean_key_size {12};
        Size mean_value_size {18};
        Size spread {4};
        bool is_sequential {};
        bool is_unique {};
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(Random&, Size) -> std::vector<Record>;

private:
    Parameters m_param;
};
//
//class WALRecordGenerator {
//public:
//    explicit WALRecordGenerator(Size page_size):
//          m_page_size {page_size}
//    {
//        CCO_EXPECT_TRUE(is_power_of_two(page_size));
//    }
//
//    auto generate_small() -> WALRecord
//    {
//        const auto small_size = m_page_size / 0x10;
//        const auto total_update_size = random.next_int(small_size, small_size * 2);
//        const auto update_count = random.next_int(1UL, 5UL);
//        const auto mean_update_size = total_update_size / update_count;
//        return generate(mean_update_size, update_count);
//    }
//
//    auto generate_large() -> WALRecord
//    {
//        const auto large_size = m_page_size / 3 * 2;
//        const auto total_update_size = random.next_int(large_size, large_size * 2);
//        const auto update_count = random.next_int(1UL, 5UL);
//        const auto mean_update_size = total_update_size / update_count;
//        return generate(mean_update_size, update_count);
//    }
//
//    auto generate(Size mean_update_size, Size update_count) -> WALRecord
//    {
//        CCO_EXPECT_GT(mean_update_size, 0);
//        constexpr Size page_count = 0x1000;
//        const auto lower_bound = mean_update_size - mean_update_size/3;
//        const auto upper_bound = mean_update_size + mean_update_size/3;
//        const auto page_size = upper_bound;
//        CCO_EXPECT_LE(page_size, std::numeric_limits<uint16_t>::max());
//
//        m_snapshots_before.emplace_back(random.next_string(page_size));
//        m_snapshots_after.emplace_back(random.next_string(page_size));
//        std::vector<ChangedRegion> update {};
//        auto payload_size = WALPayload::HEADER_SIZE;
//
//        for (Index i {}; i < update_count; ++i) {
//            const auto size = random.next_int(lower_bound, upper_bound);
//            const auto offset = random.next_int(page_size - size);
//
//            update.emplace_back();
//            update.back().offset = offset;
//            update.back().before = stob(m_snapshots_before.back()).range(offset, size);
//            update.back().after = stob(m_snapshots_after.back()).range(offset, size);
//            payload_size += WALPayload::UPDATE_HEADER_SIZE + 2 * size;
//            CCO_EXPECT_LT(payload_size, 4 * m_page_size);
//        }
//        m_scratches.emplace(m_last_lsn, std::string(4 * m_page_size, '\x00'));
//        WALRecord record {{
//            std::move(update),
//            PageId {static_cast<uint32_t>(random.next_int(page_count))},
//            SequenceNumber::null(),
//            SequenceNumber {static_cast<uint32_t>(m_payloads.size() + ROOT_ID_VALUE)},
//        }, stob(m_scratches[m_last_lsn])};
//        m_last_lsn++;
//        m_payloads.push_back(btos(record.payload().data()));
//        return record;
//    }
//
//    auto validate_record(const WALRecord &record, SequenceNumber target_lsn) const -> void
//    {
//        (void)record;
//        CCO_EXPECT_EQ(record.lsn(), target_lsn);
//        const auto payload = retrieve_payload(target_lsn);
//        CCO_EXPECT_EQ(record.type(), WALRecord::Type::FULL);
//        CCO_EXPECT_TRUE(record.payload().data() == stob(payload));
//        CCO_EXPECT_TRUE(record.is_consistent());
//    }
//
//    [[nodiscard]] auto retrieve_payload(SequenceNumber lsn) const -> const std::string&
//    {
//        return m_payloads.at(lsn.as_index());
//    }
//
//    Random random {0};
//
//private:
//    std::vector<std::string> m_payloads;
//    std::vector<std::string> m_snapshots_before;
//    std::vector<std::string> m_snapshots_after;
//    std::unordered_map<SequenceNumber, std::string, SequenceNumber::Hash> m_scratches;
//    SequenceNumber m_last_lsn;
//    Size m_page_size;
//};

} // cco

#endif // CCO_TEST_TOOLS_TOOLS_H
