
#include "tools.h"
#include "fakes.h"
#include "page/node.h"
#include "tree/node_manager.h"
#include "tree/tree.h"
#include "utils/system.h"
#include <unordered_set>

namespace Calico {

unsigned RecordGenerator::default_seed = 0;

RecordGenerator::RecordGenerator(Parameters param)
    : m_param {param} {}

auto RecordGenerator::generate(Random &random, Size num_records) -> std::vector<Record>
{
    const auto [mks, mvs, spread, is_sequential, is_unique] = m_param;
    std::vector<Record> records(num_records);

    const auto min_ks = mks < spread ? 1 : mks - spread;
    const auto min_vs = mvs < spread ? 0 : mvs - spread;
    const auto max_ks = mks + spread;
    const auto max_vs = mvs + spread;
    auto itr = records.begin();
    std::unordered_set<std::string> set;
    Size num_collisions {};

    while (itr != records.end()) {
        const auto ks = random.get(min_ks, max_ks);
        auto key = random.get<std::string>('\x00', '\xFF', ks);
        if (is_sequential) {
            if (set.find(key) != end(set)) {
                CALICO_EXPECT_LT(num_collisions, num_records);
                num_collisions++;
                continue;
            }
            set.emplace(key);
        }
        const auto vs = random.get(min_vs, max_vs);
        itr->key = std::move(key);
        itr->value = random.get<std::string>('\x00', '\xFF', vs);
        itr++;
    }
    if (is_sequential)
        std::sort(begin(records), end(records));
    return records;
}


//TreePrinter::TreePrinter(Tree &tree, bool has_integer_keys)
//    : m_tree{tree},
//      m_has_integer_keys {has_integer_keys} {}
//
//auto TreePrinter::print(Size indentation) -> void
//{
//    print_aux(m_tree.pool().acquire(Id::root(), false).value(), 0);
//
//    for (auto &level: m_levels)
//        fmt::print("{}{}\n", std::string(indentation, ' '), level);
//}
//
//auto TreePrinter::add_spaces_to_level(Size n, Index level) -> void
//{
//    m_levels.at(level) += std::string(n, ' ');
//}
//
//auto TreePrinter::add_spaces_to_other_levels(Size n, Index excluded) -> void
//{
//    // If excluded is equal to m_levels.size(), add spaces to all levels.
//    CALICO_EXPECT_LE(excluded, m_levels.size());
//    for (Index level{}; level < m_levels.size(); ++level) {
//        if (level != excluded)
//            add_spaces_to_level(n, level);
//    }
//}
//
//auto TreePrinter::print_aux(Node node, Index level) -> void
//{
//    ensure_level_exists(level);
//    for (Index cid {}; cid < node.cell_count(); ++cid) {
//        const auto is_first = cid == 0;
//        const auto not_last = cid < node.cell_count() - 1;
//        auto cell = node.read_cell(cid);
//        if (!node.is_external())
//            print_aux(m_tree.pool().acquire(cell.left_child_id(), false).value(), level + 1);
//        if (is_first)
//            add_node_start_to_level(node.id().value, level);
//        auto key = btos(cell.key());
//        if (m_has_integer_keys) {
//            key = std::to_string(std::stoi(key));
//        } else if (key.size() > 5) {
//            key = key.substr(0, 3) + "..";
//        }
//        add_key_to_level(Slice {key), level, node.is_external()};
//        if (not_last) {
//            add_key_separator_to_level(level);
//        } else {
//            add_node_end_to_level(level);
//        }
//    }
//    if (!node.is_external())
//        print_aux(m_tree.pool().acquire(node.rightmost_child_id(), false).value(), level + 1);
//}
//
//auto TreePrinter::add_key_to_level(Slice key, Index level, bool has_value) -> void
//{
//    const auto key_token = make_key_token(key);
//    const std::string value_token {has_value ? "*" : ""};
//    m_levels[level] += key_token + value_token;
//    add_spaces_to_other_levels(key_token.size() + value_token.size(), level);
//}
//
//auto TreePrinter::add_key_separator_to_level(Index level) -> void
//{
//    const auto key_separator_token = make_key_separator_token();
//    m_levels[level] += key_separator_token;
//    add_spaces_to_other_levels(key_separator_token.size(), level);
//}
//
//auto TreePrinter::add_node_start_to_level(Index id, Index level) -> void
//{
//    const auto node_start_token = make_node_start_token(id);
//    m_levels[level] += node_start_token;
//    add_spaces_to_other_levels(node_start_token.size(), level);
//}
//
//auto TreePrinter::add_node_end_to_level(Index level) -> void
//{
//    const auto node_end_token = make_node_end_token();
//    m_levels[level] += node_end_token;
//    add_spaces_to_other_levels(node_end_token.size(), level);
//}
//
//auto TreePrinter::make_key_token(Slice key) -> std::string
//{
//    return btos(key);
//}
//
//auto TreePrinter::make_key_separator_token() -> std::string
//{
//    return ",";
//}
//
//auto TreePrinter::make_node_start_token(Index id) -> std::string
//{
//    return fmt::format("{}:[", id);
//}
//
//auto TreePrinter::make_node_end_token() -> std::string
//{
//    return "]";
//}
//
//auto TreePrinter::ensure_level_exists(Index level) -> void
//{
//    while (level >= m_levels.size())
//        m_levels.emplace_back();
//    CALICO_EXPECT_GT(m_levels.size(), level);
//}

} // namespace Calico

auto operator>(const Calico::Record &lhs, const Calico::Record &rhs) -> bool
{
    return Calico::Slice {lhs.key} > Calico::Slice {rhs.key};
}

auto operator<=(const Calico::Record &lhs, const Calico::Record &rhs) -> bool
{
    return Calico::Slice {lhs.key} <= Calico::Slice {rhs.key};
}

auto operator>=(const Calico::Record &lhs, const Calico::Record &rhs) -> bool
{
    return Calico::Slice {lhs.key} >= Calico::Slice {rhs.key};
}

auto operator==(const Calico::Record &lhs, const Calico::Record &rhs) -> bool
{
    return Calico::Slice {lhs.key} == Calico::Slice {rhs.key};
}

auto operator!=(const Calico::Record &lhs, const Calico::Record &rhs) -> bool
{
    return Calico::Slice {lhs.key} != Calico::Slice {rhs.key};
}
