
#include <unordered_set>

#include "fakes.h"
#include "tools.h"
#include "page/page.h"
#include "tree/tree.h"
#include "file/file.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"

namespace cub {

TreeValidator::TreeValidator(ITree &tree)
    : m_tree{tree} { }
    
auto TreeValidator::validate() -> void
{
    validate_parent_child_connections();
    validate_sibling_connections();
    validate_ordering();
}

auto TreeValidator::validate_sibling_connections() -> void
{
    // First node in the sibling chain.
    auto node = m_tree.find_root(false);
    while (!node.is_external())
        node = m_tree.acquire_node(node.child_id(0), false);
    std::optional<std::string> prev{};
    while (true) {
        for (Index cid{}; cid < node.cell_count(); ++cid) {
            const auto key = btos(node.read_key(cid));
            // Strict ordering.
            if (prev)
                CUB_EXPECT_LT(*prev, key);
            prev = key;
        }
        const auto next_id = node.right_sibling_id();
        if (next_id.is_null())
            break;
        node = m_tree.acquire_node(next_id, false);
    }
}

auto TreeValidator::validate_parent_child_connections() -> void
{
    auto check_connection = [&](Node &node, Index index) -> void {
        auto child = m_tree.acquire_node(node.child_id(index), false);
        CUB_EXPECT_EQ(child.parent_id(), node.id());
    };
    traverse_inorder([&](Node &node, Index cid) -> void {
        CUB_EXPECT_LT(cid, node.cell_count());
        if (!node.is_external()) {
            check_connection(node, cid);
            // Rightmost child.
            if (cid == node.cell_count() - 1)
                check_connection(node, cid + 1);
        }
    });
}

auto TreeValidator::validate_ordering() -> void
{
    const auto keys = collect_keys();
    auto sorted = keys;
    std::sort(sorted.begin(), sorted.end());
    CUB_EXPECT_EQ(keys, sorted);
}

auto TreeValidator::collect_keys() -> std::vector<std::string>
{
    auto keys = std::vector<std::string>{};
    traverse_inorder([&keys](Node &node, Index cid) -> void {
        keys.push_back(btos(node.read_key(cid)));
    });
    return keys;
}

auto TreeValidator::traverse_inorder(const std::function<void(Node&, Index)> &callback) -> void
{
    traverse_inorder_helper(m_tree.acquire_node(PID::root(), false), callback);
}

auto TreeValidator::traverse_inorder_helper(Node node, const std::function<void(Node&, Index)> &callback) -> void
{
    CUB_VALIDATE(node.validate());

    const auto id = node.id();
    for (Index index{}; index <= node.cell_count(); ++index) {
        std::optional<Cell> cell {};
        if (index != node.cell_count())
            cell = node.read_cell(index);
        if (!node.is_external()) {
            const auto next_id = node.child_id(index);
            traverse_inorder_helper(m_tree.acquire_node(next_id, false), callback);
            node = m_tree.acquire_node(id, false);
        }
        if (cell)
            callback(node, index);
    }
}

auto TreeValidator::is_reachable(std::string key) -> bool
{
    std::string temp;
    auto success = true;

    // Traverse down to the node containing key using the child pointers.
    auto [node, index, found_eq] = m_tree.find_ge(stob(key), false);
    if (!found_eq)
        return false;

    // Try to go back up to the root using the source pointers.
    while (!node.id().is_root()) {
        auto parent_id = node.parent_id();
        if (parent_id.is_null()) {
            success = false;
            break;
        }
        node = m_tree.acquire_node(parent_id, false);
    }
    return success;
}

TreePrinter::TreePrinter(ITree &tree)
    : m_tree{tree} {}

auto TreePrinter::print() -> void
{
    print_aux(m_tree.acquire_node(PID::root(), false), 0);

    for (auto &level: m_levels)
        std::cout << level << '\n';
}

auto TreePrinter::add_spaces_to_level(Size n, Index level) -> void
{
    m_levels.at(level) += std::string(n, ' ');
}

auto TreePrinter::add_spaces_to_other_levels(Size n, Index excluded) -> void
{
    // If excluded is equal to m_levels.size(), add spaces to all levels.
    CUB_EXPECT_LE(excluded, m_levels.size());
    for (Index level{}; level < m_levels.size(); ++level) {
        if (level != excluded)
            add_spaces_to_level(n, level);
    }
}

auto TreePrinter::print_aux(Node node, Index level) -> void
{
    ensure_level_exists(level);
    for (Index cid {}; cid < node.cell_count(); ++cid) {
        const auto is_first = cid == 0;
        const auto not_last = cid < node.cell_count() - 1;
        auto cell = node.read_cell(cid);
        if (!node.is_external())
            print_aux(m_tree.acquire_node(cell.left_child_id(), false), level + 1);
        if (is_first)
            add_node_start_to_level(node.id().value, level);

        add_key_to_level(cell.key(), level);
        if (not_last) {
            add_key_separator_to_level(level);
        } else {
            add_node_end_to_level(level);
        }
    }
    if (!node.is_external())
        print_aux(m_tree.acquire_node(node.rightmost_child_id(), false), level + 1);
}

auto TreePrinter::add_key_to_level(BytesView key, Index level) -> void
{
    const auto key_token = make_key_token(key);
    m_levels[level] += key_token;
    add_spaces_to_other_levels(key_token.size(), level);
}

auto TreePrinter::add_key_separator_to_level(Index level) -> void
{
    const auto key_separator_token = make_key_separator_token();
    m_levels[level] += key_separator_token;
    add_spaces_to_other_levels(key_separator_token.size(), level);
}

auto TreePrinter::add_node_start_to_level(Index id, Index level) -> void
{
    const auto node_start_token = make_node_start_token(id);
    m_levels[level] += node_start_token;
    add_spaces_to_other_levels(node_start_token.size(), level);
}

auto TreePrinter::add_node_end_to_level(Index level) -> void
{
    const auto node_end_token = make_node_end_token();
    m_levels[level] += node_end_token;
    add_spaces_to_other_levels(node_end_token.size(), level);
}

auto TreePrinter::make_key_token(BytesView key) -> std::string
{
    return btos(key);
}

auto TreePrinter::make_key_separator_token() -> std::string
{
    return ",";
}

auto TreePrinter::make_node_start_token(Index id) -> std::string
{
    return std::to_string(id) + ":[";
}

auto TreePrinter::make_node_end_token() -> std::string
{
    return "]";
}

auto TreePrinter::ensure_level_exists(Index level) -> void
{
    while (level >= m_levels.size())
        m_levels.emplace_back();
    CUB_EXPECT_GT(m_levels.size(), level);
}

unsigned RecordGenerator::default_seed = 0;

RecordGenerator::RecordGenerator(Parameters param)
    : m_param {param} {}

auto RecordGenerator::generate(Random &random, Size num_records) -> std::vector<Record>
{
    const auto [mks, mvs, spread, is_sequential] = m_param;
    std::vector<Record> records(num_records);

    const auto min_ks = mks < spread ? 1 : mks - spread;
    const auto min_vs = mvs < spread ? 0 : mvs - spread;
    const auto max_ks = mks + spread;
    const auto max_vs = mvs + spread;

    for (auto &[key, value]: records) {
        key = random_string(random, min_ks, max_ks);
        value = random_string(random, min_vs, max_vs);
    }
    if (is_sequential)
        std::sort(begin(records), end(records));
    return records;
}

auto WALPrinter::print(const WALRecord &record) -> void
{
    std::cout << "Record<LSN=" << record.lsn().value
              << ", CRC=" << record.crc() << ", ";
    if (record.is_commit()) {
        std::cout << "COMMIT";
    } else {
        const auto update = record.payload().decode();
        std::cout << "UPDATE(pid:" << update.page_id.value << ", n=" << update.changes.size() << ')';
    }
    std::cout << ">\n";
}


auto WALPrinter::print(const std::string &path, Size block_size) -> void
{
    auto file = std::make_unique<ReadOnlyFile>(path, Mode::DIRECT, 0666);
    WALReader reader {std::move(file), block_size};
    print(reader);
}

auto WALPrinter::print(SharedMemory data, Size block_size) -> void
{
    auto file = std::make_unique<ReadOnlyMemory>(std::move(data));
    WALReader reader {std::move(file), block_size};
    print(reader);
}

auto WALPrinter::print(WALReader &reader) -> void
{
    reader.reset();
    do {
        CUB_EXPECT_NE(reader.record(), std::nullopt);
        print(*reader.record());
    } while (reader.increment());
}

} // cub