// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "logging.h"
#include "tree.h"

namespace calicodb
{

class SchemaCursor : public Cursor
{
    CursorImpl *const m_c;

public:
    explicit SchemaCursor(CursorImpl &c)
        : m_c(&c)
    {
    }

    ~SchemaCursor() override
    {
        delete m_c;
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_c->is_valid();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_c->status();
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        return m_c->key();
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        // TODO: Parse the options string into a human-readable format, save it internally and return a slice to it.
        //       We should skip the varint root ID or convert it into a decimal string.
        CALICODB_EXPECT_TRUE(is_valid());
        return m_c->value();
    }

    auto seek(const Slice &key) -> void override
    {
        return m_c->seek(key);
    }

    auto seek_first() -> void override
    {
        m_c->seek_first();
    }

    auto seek_last() -> void override
    {
        m_c->seek_last();
    }

    auto next() -> void override
    {
        m_c->next();
    }

    auto previous() -> void override
    {
        m_c->previous();
    }
};

Schema::Schema(Pager &pager, const Status &status, Stat &stat, char *scratch)
    : m_status(&status),
      m_pager(&pager),
      m_scratch(scratch),
      m_stat(&stat),
      m_map(pager, stat, scratch, nullptr)
{
}

auto Schema::new_cursor() -> Cursor *
{
    return new SchemaCursor(*m_map.new_cursor());
}

auto Schema::close() -> void
{
    for (const auto &[_, state] : m_trees) {
        delete state.tree;
    }
    m_map.finish_operation();
}

auto Schema::corrupted_root_id(const Slice &name, const Slice &value) -> Status
{
    std::string message("root entry for bucket \"" + name.to_string() + "\" is corrupted: ");
    message.append(escape_string(value));
    auto s = Status::corruption(message);
    m_pager->set_status(s);
    return s;
}

auto Schema::create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status
{
    Status s;
    if (m_pager->page_count() == 0) {
        // Initialize the database file header, as well as the schema tree's root page.
        m_pager->initialize_root();
    }

    Id root_id;
    std::string value;
    if (s.is_ok()) {
        s = m_map.get(name, &value);
    }

    if (s.is_not_found()) {
        s = Tree::create(*m_pager, &root_id);
        if (s.is_ok()) {
            // TODO: Encode persistent bucket options here.
            encode_root_id(root_id, value);
            s = m_map.put(name, value);
        }
    } else if (s.is_ok()) {
        if (!decode_and_check_root_id(value, root_id)) {
            s = corrupted_root_id(name.to_string(), value);
        } else if (options.error_if_exists) {
            s = Status::invalid_argument(
                "table \"" + name.to_string() + "\" already exists");
        }
    }

    if (s.is_ok() && b_out) {
        *b_out = construct_bucket_state(root_id);
    }
    return s;
}

auto Schema::open_bucket(const Slice &name, Bucket &b_out) -> Status
{
    Status s;
    if (m_pager->page_count() == 0) {
        return Status::invalid_argument();
    }

    std::string value;
    if (s.is_ok()) {
        s = m_map.get(name, &value);
    }

    Id root_id;
    if (s.is_ok()) {
        if (!decode_and_check_root_id(value, root_id)) {
            return corrupted_root_id(name.to_string(), value);
        }
    } else if (s.is_not_found()) {
        return Status::invalid_argument(
            "table \"" + name.to_string() + "\" does not exist");
    } else {
        return s;
    }
    b_out = construct_bucket_state(root_id);
    return s;
}

auto Schema::decode_root_id(const Slice &data, Id &out) -> bool
{
    U32 num;
    if (decode_varint(data.data(), data.data() + data.size(), num)) {
        out.value = static_cast<U32>(num);
        return true;
    }
    return false;
}

auto Schema::decode_and_check_root_id(const Slice &data, Id &out) -> bool
{
    if (!decode_root_id(data, out) || out.value > m_pager->page_count()) {
        return false;
    }
    return true;
}

auto Schema::encode_root_id(Id id, std::string &root_id_out) -> void
{
    if (root_id_out.size() < kVarintMaxLength) {
        // More than enough for a U32.
        root_id_out.resize(kVarintMaxLength);
    }
    const auto *end = encode_varint(root_id_out.data(), id.value);
    root_id_out.resize(static_cast<std::uintptr_t>(end - root_id_out.data()));
}

auto Schema::construct_bucket_state(Id root_id) -> Bucket
{
    auto itr = m_trees.find(root_id);
    if (itr == end(m_trees) || !itr->second.tree) {
        itr = m_trees.insert(itr, {root_id, {}});
        itr->second.root = root_id;
        itr->second.tree = new Tree(
            *m_pager,
            *m_stat,
            m_scratch,
            &itr->second.root);
    }
    return Bucket{itr->second.tree};
}

auto Schema::use_bucket(const Bucket &b) -> void
{
    CALICODB_EXPECT_NE(b.state, nullptr);
    if (m_recent && m_recent != b.state) {
        m_recent->finish_operation();
    }
    m_recent = reinterpret_cast<const Tree *>(b.state);
}

auto Schema::drop_bucket(const Slice &name) -> Status
{
    std::string value;
    auto s = m_map.get(name, &value);
    if (s.is_not_found()) {
        return Status::invalid_argument(
            "table \"" + name.to_string() + "\" does not exist");
    } else if (!s.is_ok()) {
        return s;
    }

    Id root_id;
    if (!decode_and_check_root_id(value, root_id)) {
        return corrupted_root_id(name, value);
    }
    auto itr = m_trees.find(root_id);
    if (itr != end(m_trees)) {
        delete itr->second.tree;
        m_trees.erase(root_id);
    }
    Tree drop(*m_pager, *m_stat, m_scratch, &root_id);
    s = Tree::destroy(drop);
    if (s.is_ok()) {
        s = m_map.erase(name);
    }
    return s;
}

auto Schema::vacuum_reroot(Id old_id, Id new_id) -> void
{
    Id original_id;
    auto itr = m_trees.find(old_id);
    if (itr == end(m_trees)) {
        RootedTree reroot;
        reroot.root = old_id;
        // This tree isn't open right now (and it hasn't been rerooted yet), but we need to keep track of the
        // new root mapping. Create a dummy tree entry for this purpose.
        m_trees.insert(itr, {new_id, reroot});
        original_id = old_id;
    } else {
        // Reroot the tree. This tree is either open, or it has already been rerooted. Leave the root ID stored
        // in the tree state set to the original root.
        auto node = m_trees.extract(itr);
        CALICODB_EXPECT_FALSE(!node);
        node.key() = new_id;
        original_id = node.mapped().root;
        m_trees.insert(std::move(node));
    }
    // Map the original root ID to the newest root ID.
    m_reroot.insert_or_assign(original_id, new_id);
}

auto Schema::vacuum_finish() -> Status
{
    auto *c = new_cursor();
    c->seek_first();

    Status s;
    while (c->is_valid()) {
        Id old_id;
        if (!decode_and_check_root_id(c->value(), old_id)) {
            s = corrupted_root_id(c->key().to_string(), c->value());
            break;
        }
        const auto root = m_reroot.find(old_id);
        if (root != end(m_reroot)) {
            std::string value;
            encode_root_id(root->second, value);
            // Update the database schema with the new root page ID for this tree.
            s = m_map.put(c->key(), value);
            if (!s.is_ok()) {
                break;
            }
            // Update the in-memory tree root stored by each bucket.
            auto tree = m_trees.find(root->second);
            CALICODB_EXPECT_NE(tree, end(m_trees));
            if (tree->second.tree) {
                tree->second.root = root->second;
            } else {
                // This tree is not actually open. The RootedTree entry exists so that vacuum_reroot() could
                // find the original root ID.
                m_trees.erase(tree);
            }
            m_reroot.erase(root);
        }
        c->next();
    }
    if (s.is_ok() && !m_reroot.empty()) {
        std::string message("missed ");
        append_number(message, m_reroot.size());
        message.append(" bucket(s)");
        s = Status::corruption(message);
    }
    m_reroot.clear();
    delete c;
    return s;
}

auto Schema::vacuum() -> Status
{
    if (m_recent) {
        m_recent->finish_operation();
        m_recent = nullptr;
    }
    return m_map.vacuum(*this);
}

auto Schema::TEST_validate() const -> void
{
    for (const auto &[_, bucket] : m_trees) {
        if (bucket.tree) {
            bucket.tree->TEST_validate();
        }
    }
}

} // namespace calicodb
