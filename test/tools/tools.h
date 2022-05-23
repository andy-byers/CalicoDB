
#ifndef CUB_TEST_TOOLS_TOOLS_H
#define CUB_TEST_TOOLS_TOOLS_H

#include <iomanip>
#include <iostream>
#include <vector>
#include "common.h"
#include "random.h"
#include "bytes.h"
#include "utils/utils.h"

namespace cub {

class ITree;
class Node;

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

[[maybe_unused]] inline auto hexdump(const Byte *data, Size size, Size indent = 0) -> void
{
    CUB_EXPECT_GE(size, 0);
    CUB_EXPECT_GE(indent, 0);
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

struct MoveOnly {
    MoveOnly() = default;

    MoveOnly(const MoveOnly&) = delete;
    auto operator=(const MoveOnly&) -> MoveOnly& = delete;

    MoveOnly(MoveOnly&&) = default;
    auto operator=(MoveOnly&&) -> MoveOnly& = default;
};

struct Record {
    auto operator<(const Record &rhs) const -> bool
    {
        // NOTE: Could probably just use std::string::operator<(), but this works for strings containing
        //       weird data, like '\0' in the middle.
        return compare_three_way(_b(key), _b(rhs.key)) == ThreeWayComparison::LT;
    }

    std::string key;
    std::string value;
};

class RecordGenerator {
public:
    struct Parameters {
        Size min_key_size {5};
        Size max_key_size {10};
        Size min_value_size {5};
        Size max_value_size {10};
        unsigned seed {};
        bool are_batches_sorted {};
    };

    static auto generate(Size, Parameters) -> std::vector<Record>;
};

} // db

#endif // CUB_TEST_TOOLS_TOOLS_H
