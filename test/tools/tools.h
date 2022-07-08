
#ifndef CALICO_TEST_TOOLS_TOOLS_H
#define CALICO_TEST_TOOLS_TOOLS_H

#include <iomanip>
#include <iostream>
#include <vector>
#include "random.h"
#include "calico/calico.h"
#include "utils/identifier.h"
#include "utils/utils.h"
#include "wal/wal_record.h"

namespace calico {

class ITree;
class Node;

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
        return t.insert(stob(key), stob(value));
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        return t.erase(find_exact(t, key));
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        if (t.erase(find_exact(t, key)))
            return true;
        return t.erase(t.find_minimum());
    }

} // tools



template<std::size_t Length = 20> auto make_key(Index key) -> std::string
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
    explicit TreePrinter(ITree&, bool = true);
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
    ITree &m_tree;
    bool m_has_integer_keys {};
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
        Size mean_key_size {12};
        Size mean_value_size {18};
        Size spread {4};
        bool is_sequential {};
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(Random&, Size) -> std::vector<Record>;

private:
    Parameters m_param;
};

template<class Db, class F> auto traverse_db(Db &db, F &&f)
{
    for (auto c = db.find_minimum(); c.is_valid(); c++)
        f(c.record());
}

template<class Db> auto collect_records(Db &db) -> std::vector<Record>
{
    std::vector<Record> out;
    traverse_db(db, [&out](const Record &record) {
        out.emplace_back(record);
    });
    return out;
}

class WALRecordGenerator {
public:
    explicit WALRecordGenerator(Size block_size)
        : m_block_size {block_size}
    {
        CALICO_EXPECT_TRUE(is_power_of_two(block_size));
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
        CALICO_EXPECT_GT(mean_update_size, 0);
        constexpr Size page_count = 0x1000;
        const auto lower_bound = mean_update_size - mean_update_size/3;
        const auto upper_bound = mean_update_size + mean_update_size/3;
        const auto page_size = upper_bound;
        CALICO_EXPECT_LE(page_size, std::numeric_limits<uint16_t>::max());

        m_snapshots_before.emplace_back(random.next_string(page_size));
        m_snapshots_after.emplace_back(random.next_string(page_size));
        std::vector<ChangedRegion> update {};

        for (Index i {}; i < update_count; ++i) {
            const auto size = random.next_int(lower_bound, upper_bound);
            const auto offset = random.next_int(page_size - size);

            update.emplace_back();
            update.back().offset = offset;
            update.back().before = stob(m_snapshots_before.back()).range(offset, size);
            update.back().after = stob(m_snapshots_after.back()).range(offset, size);
        }
        WALRecord record {{
            std::move(update),
            PID {static_cast<uint32_t>(random.next_int(page_count))},
            LSN::null(),
            LSN {static_cast<uint32_t>(m_payloads.size() + ROOT_ID_VALUE)},
        }};
        m_payloads.push_back(btos(record.payload().data()));
        return record;
    }

    auto validate_record(const WALRecord &record, LSN target_lsn) const -> void
    {
        CALICO_EXPECT_EQ(record.lsn(), target_lsn);
        const auto payload = retrieve_payload(target_lsn);
        CALICO_EXPECT_EQ(record.type(), WALRecord::Type::FULL);
        CALICO_EXPECT_TRUE(record.payload().data() == stob(payload));
        CALICO_EXPECT_TRUE(record.is_consistent());
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

} // calico

#endif // CALICO_TEST_TOOLS_TOOLS_H
