// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "internal.h"
#include "internal_vector.h"
#include "json.h"

namespace calicodb::json
{

struct Aggregate {
    size_t size;
    Node *begin;
};

struct Node {
    Type type;
    Node *next;
    union V {
        bool boolean = false;
        int64_t integer;
        double real;
        Slice string;
        Aggregate aggregate;
    } v;
};

namespace
{

[[nodiscard]] constexpr auto is_scalar(Type t) -> bool
{
    return t != Type::kObject && t != Type::kArray;
}

// Interned string format:
//  Size | Field
// ------|-----------
//  1    | Flag = 0
//  N    | Data
//
// Externed string format:
//  Size | Field
// ------|-----------
//  1    | Flag = 1
//  N    | Data
[[nodiscard]] static auto string_data(char *ptr) -> char *
{
    CALICODB_EXPECT_NE(ptr, nullptr);
    return ptr + 1;
}

class Allocator final
{
public:
    explicit Allocator() = default;

    ~Allocator()
    {
        for (auto *a : m_node_arenas) {
            Mem::deallocate(a);
        }
        for (auto *a : m_char_arenas) {
            Mem::deallocate(a);
        }
        CALICODB_EXPECT_EQ(m_string_refs, 0);
    }

    [[nodiscard]] auto allocate_string(size_t size) -> char *
    {
        if (size > kMaxAllocation) {
            return nullptr;
        }
        char *str = nullptr;
        if (size > CharArena::kSize) {
            // String is too long to be interned. Use the general-purpose allocator.
            str = static_cast<char *>(Mem::allocate(1 + size));
            init_string(str, true);
        } else if (auto *arena = find_arena(m_char_arenas, size)) {
            str = static_cast<char *>(arena->allocate(1 + size));
            init_string(str, false);
        }
        return str;
    }

    [[nodiscard]] auto allocate_node() -> Node *
    {
        if (auto *arena = find_arena(m_node_arenas, sizeof(Node) + alignof(Node) - 1)) {
            if (auto *node = static_cast<Node *>(arena->allocate(sizeof(Node), alignof(Node)))) {
                *node = {};
                return node;
            }
        }
        return nullptr;
    }

    void deallocate_string(char *str)
    {
        if (str) {
            if (str[-1]) {
                Mem::deallocate(str - 1);
            }
            --m_string_refs;
        }
    }

private:
    template <size_t Size>
    struct Arena {
        static constexpr size_t kSize = Size;

        size_t offset;

        auto allocate(size_t size) -> void *
        {
            if (offset + size > Size) {
                return nullptr;
            }
            auto *ptr = reinterpret_cast<char *>(this + 1) + offset;
            offset += size;
            return ptr;
        }

        auto allocate(size_t size, size_t align) -> void *
        {
            CALICODB_EXPECT_GT(align, 0); // Power of 2
            CALICODB_EXPECT_EQ(align & (align - 1), 0);
            // Move up to the nearest multiple of the alignment.
            const auto diff = (offset + align - 1) & align;
            auto *ptr = static_cast<char *>(allocate(diff + size));
            return ptr ? ptr + diff : nullptr;
        }
    };

    void init_string(char *str, bool is_extern)
    {
        *str = static_cast<char>(is_extern);
        ++m_string_refs;
    }

    template <class Arena>
    [[nodiscard]] auto find_arena(Vector<Arena *> &arenas, size_t size) -> Arena *
    {
        CALICODB_EXPECT_LE(size, Arena::kSize);
        // Find an arena that has enough memory left to satisfy the request.
        for (auto *a : arenas) {
            CALICODB_EXPECT_GE(Arena::kSize, a->offset);
            if (a->offset + size <= Arena::kSize) {
                return a;
            }
        }

        // If no such arena exists, add a new one. The new arena is guaranteed
        // to have enough memory.
        auto *arena = static_cast<Arena *>(
            Mem::allocate(sizeof(Arena) + // Arena metadata
                          Arena::kSize)); // Arena buffer
        if (arena && arenas.push_back(arena) == 0) {
            *arena = Arena{};
        }
        return arena;
    }

    using NodeArena = Arena<sizeof(Node) * 64>;
    using CharArena = Arena<sizeof(char) * 4'096>;
    Vector<NodeArena *> m_node_arenas;
    Vector<CharArena *> m_char_arenas;
    size_t m_string_refs = 0;
};

struct Context {
    Allocator a;
    Node *root;
    size_t n;
};

class DocumentHandler : public Handler
{
    Vector<Node *> m_stack;
    Context *const m_ctx;
    Node *m_cursor = nullptr;

    [[nodiscard]] auto make_node(Type type) -> Node *
    {
        CALICODB_EXPECT_FALSE(oom);
        auto *node = m_ctx->a.allocate_node();
        if (node) {
            node->type = type;
            if (!m_ctx->root) {
                // Just allocated the root node.
                m_ctx->root = node;
                m_cursor = node;
            }
        } else {
            oom = true;
        }
        return node;
    }

    void add_node(Node &node)
    {
        if (m_cursor == &node) {
            CALICODB_EXPECT_EQ(m_cursor, m_ctx->root);
            return;
        }
        if (is_scalar(m_cursor->type)) {
            m_cursor->next = &node;
        } else if (m_cursor->v.aggregate.begin) {
            // Aggregate already has a child, so this must be its sibling. end_structure() detects
            // empty objects/arrays and sets the child pointer to the node's own address.
            m_cursor->next = &node;
        } else {
            m_cursor->v.aggregate.begin = &node;
            m_cursor->v.aggregate.size = 0;
        }
        if (!m_stack.is_empty()) {
            CALICODB_EXPECT_FALSE(is_scalar(m_stack.back()->type));
            m_stack.back()->v.aggregate.size += node.type != Type::kKey;
        }
        m_cursor = &node;
    }

    auto begin_structure() -> int
    {
        if (m_stack.push_back(m_cursor)) {
            oom = true;
            return -1;
        }
        return 0;
    }

    void end_structure()
    {
        CALICODB_EXPECT_FALSE(m_stack.is_empty());
        if (!is_scalar(m_cursor->type)) {
            if (m_cursor->v.aggregate.size == 0) {
                // Empty object/array. Set the child pointer so that add_node() doesn't attempt to
                // set the next node as the first child.
                m_cursor->v.aggregate.begin = m_cursor;
            }
        }
        m_cursor->next = nullptr;
        m_cursor = m_stack.back();
        m_stack.pop_back();
    }

    [[nodiscard]] auto accept_any_string(const Slice &value, bool is_key) -> bool
    {
        if (auto *str = m_ctx->a.allocate_string(value.size())) {
            if (auto *node = make_node(is_key ? Type::kKey : Type::kString)) {
                std::memcpy(string_data(str), value.data(), value.size());
                node->v.string = Slice(string_data(str), value.size());
                add_node(*node);
                return true;
            }
        }
        return false;
    }

public:
    // Set to true by the DocumentHandler instance if it runs out of memory.
    bool oom = false;

    explicit DocumentHandler(Context &ctx)
        : m_ctx(&ctx)
    {
    }

    ~DocumentHandler() override = default;

    [[nodiscard]] auto accept_key(const Slice &value) -> bool override
    {
        return accept_any_string(value, true);
    }

    [[nodiscard]] auto accept_string(const Slice &value) -> bool override
    {
        return accept_any_string(value, false);
    }

    [[nodiscard]] auto accept_integer(int64_t value) -> bool override
    {
        if (auto *node = make_node(Type::kInteger)) {
            node->v.integer = value;
            add_node(*node);
            return true;
        }
        return false;
    }

    [[nodiscard]] auto accept_real(double value) -> bool override
    {
        if (auto *node = make_node(Type::kReal)) {
            node->v.real = value;
            add_node(*node);
            return true;
        }
        return false;
    }

    [[nodiscard]] auto accept_boolean(bool value) -> bool override
    {
        if (auto *node = make_node(Type::kBoolean)) {
            node->v.boolean = value;
            add_node(*node);
            return true;
        }
        return false;
    }

    [[nodiscard]] auto accept_null() -> bool override
    {
        if (auto *node = make_node(Type::kNull)) {
            add_node(*node);
            return true;
        }
        return false;
    }

    [[nodiscard]] auto begin_object() -> bool override
    {
        if (auto *node = make_node(Type::kObject)) {
            add_node(*node);
            if (begin_structure()) {
                oom = true;
            } else {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto end_object() -> bool override
    {
        end_structure();
        return true;
    }

    [[nodiscard]] auto begin_array() -> bool override
    {
        if (auto *node = make_node(Type::kArray)) {
            add_node(*node);
            if (begin_structure()) {
                oom = true;
            } else {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] auto end_array() -> bool override
    {
        end_structure();
        return true;
    }
};

auto build_from_text(const Slice &input, Context &ctx)
{
    DocumentHandler handler(ctx);
    Reader reader(handler);
    auto r = reader.read(input);
    if (r && handler.oom) {
        // The handler may have returned false from an accept_*() because it ran out of
        // memory. The result will indicate success in this case, so make sure it says
        // that we have run out of memory.
        r.error = Error::kNoMemory;
    }
    return r;
}

} // namespace

class Document::DocumentImpl
{
public:
    Context ctx;

    explicit DocumentImpl() = default;
    ~DocumentImpl() = default;
};

auto new_document(const Slice &input, Document *&doc_out) -> Result
{
    Result result = {};
    doc_out = nullptr;
    if (auto *impl = Mem::new_object<Document::DocumentImpl>()) {
        doc_out = new (std::nothrow) Document(*impl);
        if (doc_out) {
            result = build_from_text(input, impl->ctx);
            if (result) {
                return result;
            }
            delete doc_out;
        }
        Mem::delete_object(impl);
    }
    result.error = Error::kNoMemory;
    return result;
}

auto new_document() -> Document *
{
    if (auto *impl = Mem::new_object<Document::DocumentImpl>()) {
        return new (std::nothrow) Document(*impl);
    }
    return nullptr;
}

Document::Document(Document &&rhs) noexcept
    : m_impl(exchange(rhs.m_impl, nullptr))
{
}

Document::~Document()
{
    Mem::delete_object(m_impl);
}

auto Document::operator=(Document &&rhs) noexcept -> Document &
{
    if (this != &rhs) {
        Mem::delete_object(m_impl);
        m_impl = exchange(rhs.m_impl, nullptr);
    }
    return *this;
}

auto Document::TODO_render_to_std_string() const -> std::string
{
    std::string string;
    Vector<const Node *> stack;
    auto skip = false;
    const auto *node = m_impl->ctx.root;
    while (node) {
        switch (node->type) {
            case Type::kKey:
                string.append('"' + node->v.string.to_string() + "\":");
                break;
            case Type::kString:
                string.append('"' + node->v.string.to_string() + '"');
                break;
            case Type::kInteger:
                string.append(std::to_string(node->v.integer));
                break;
            case Type::kReal:
                string.append(std::to_string(node->v.real));
                break;
            case Type::kBoolean:
                string.append(node->v.boolean ? "true" : "false");
                break;
            case Type::kNull:
                string.append("null");
                break;
            default:
                if (node->v.aggregate.begin == node) {
                    // Empty object/array.
                    string.append(node->type == Type::kObject ? "{}" : "[]");
                } else {
                    if (stack.push_back(node)) {
                        std::abort();
                    }
                    string += node->type == Type::kObject ? '{' : '[';
                    node = node->v.aggregate.begin;
                    skip = true;
                }
                break;
        }
        if (skip) {
            skip = false;
        } else if (node->next) {
            if (node->type != Type::kKey) {
                string += ',';
            }
            node = node->next;
        } else if (stack.is_empty()) {
            node = nullptr;
        } else {
            if (stack.back()->type == Type::kObject) {
                string += '}';
            } else {
                string += ']';
            }
            node = stack.back()->next;
            stack.pop_back();
            if (node) {
                string += ',';
            }
        }
    }
    while (!stack.is_empty()) {
        if (stack.back()->type == Type::kObject) {
            string += '}';
        } else {
            string += ']';
        }
        stack.pop_back();
    }
    return string;
}

} // namespace calicodb::json