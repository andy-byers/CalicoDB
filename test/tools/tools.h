
#ifndef CCO_TEST_TOOLS_TOOLS_H
#define CCO_TEST_TOOLS_TOOLS_H

#include <iomanip>
#include <iostream>
#include <vector>
#include "random.h"
#include "calico/calico.h"
#include "utils/identifier.h"
#include "utils/utils.h"

namespace cco {

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

} // calico

#endif // CCO_TEST_TOOLS_TOOLS_H
