// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "logging.h"
#include "scope_guard.h"
#include "table_impl.h"
#include "tree.h"
#include "txn_impl.h"

namespace calicodb
{

auto Schema::corrupted_root_id(const Slice &name, const Slice &value) -> Status
{
    std::string message("root entry for table \"" + name.to_string() + "\" is corrupted: ");
    message.append(escape_string(value));
    auto s = Status::corruption(message);
    if (m_status->is_ok()) {
        *m_status = s;
    }
    return s;
}

auto Schema::create_table(const TableOptions &options, const Slice &name, bool readonly, Table **out) -> Status
{
    Status s;
    if (m_pager->page_count() == 0) {
        if (options.create_if_missing) {
            // Initialize the database file header, as well as the schema tree's root page.
            m_pager->initialize_root();
        } else {
            s = Status::not_found();
        }
    }

    std::string value;
    if (s.is_ok()) {
        s = m_map.get(name, &value);
    }

    Id root_id;
    if (s.is_ok()) {
        if (options.error_if_exists) {
            return Status::invalid_argument("table \"" + name.to_string() + "\" already exists");
        }
        if (!decode_root_id(value, root_id)) {
            return corrupted_root_id(name.to_string(), value);
        }
    } else if (s.is_not_found()) {
        if (!options.create_if_missing) {
            return Status::invalid_argument("table \"" + name.to_string() + "\" does not exist");
        }
        s = Tree::create(*m_pager, false, &root_id);
        if (s.is_ok()) {
            encode_root_id(root_id, value);
            s = m_map.put(name, value);
        }
    }
    if (s.is_ok()) {
        auto *tb = construct_table_state(root_id, readonly);
        if (out) {
            *out = tb;
        }
    }
    return s;
}

auto Schema::decode_root_id(const Slice &data, Id &out) -> bool
{
    U64 num;
    if (decode_varint(data.data(), data.data() + data.size(), num)) {
        if (num <= m_pager->page_count()) {
            out.value = static_cast<U32>(num);
            return true;
        }
    }
    return false;
}

auto Schema::encode_root_id(Id id, std::string &out) -> void
{
    if (out.size() < kVarintMaxLength) {
        // More than enough for a U32.
        out.resize(kVarintMaxLength);
    }
    const auto *end = encode_varint(out.data(), id.value);
    out.resize(static_cast<std::uintptr_t>(end - out.data()));
}

auto Schema::construct_table_state(Id root_id, bool readonly) -> Table *
{
    auto itr = m_tables.find(root_id);
    if (itr == end(m_tables) || !itr->second.table) {
        itr = m_tables.insert(itr, {root_id, {}});
        itr->second.root = root_id;
        itr->second.table = new TableImpl(
            *m_pager, *m_status, &itr->second.root, readonly);
    }
    return itr->second.table;
}

auto Schema::drop_table(const Slice &name) -> Status
{
    std::string value;
    auto s = m_map.get(name, &value);
    if (!s.is_ok()) {
        return s;
    }

    Id root_id;
    if (!decode_root_id(value, root_id)) {
        return corrupted_root_id(name, value);
    }
    auto itr = m_tables.find(root_id);
    if (itr != end(m_tables)) {
        delete itr->second.table;
        m_tables.erase(root_id);
    }
    Tree drop(*m_pager, &root_id);
    s = Tree::destroy(drop);
    if (s.is_ok()) {
        s = m_map.erase(name);
    }
    return s;
}

auto Schema::vacuum_reroot(Id old_id, Id new_id) -> void
{
    auto tree = m_tables.find(old_id);
    if (tree == end(m_tables)) {
        RootedTable rooted;
        rooted.root = old_id;
        m_tables.insert(tree, {old_id, rooted});
    }
    // Rekey the tree. Leave the root ID stored in the tree state set
    // to the original root.
    auto node = m_tables.extract(old_id);
    CALICODB_EXPECT_FALSE(!node);
    node.key() = new_id;
    const auto root_id = node.mapped().root;
    m_tables.insert(std::move(node));

    // Map the original root ID to the newest root ID.
    m_reroot.insert_or_assign(root_id, new_id);
}

auto Schema::vacuum_finish() -> Status
{
    auto *c = new_cursor();
    c->seek_first();
    ScopeGuard guard = [c] {
        delete c;
    };

    Status s;
    while (c->is_valid()) {
        Id old_id;
        if (!decode_root_id(c->value(), old_id)) {
            return corrupted_root_id(c->key().to_string(), c->value());
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
            // Update the in-memory tree root stored by each table.
            auto tb = m_tables.find(root->second);
            CALICODB_EXPECT_NE(tb, end(m_tables));
            if (tb->second.table) {
                tb->second.root = root->second;
            } else {
                // This table is not actually open. The RootedTable entry exists
                // so that vacuum_reroot() could find the original root ID.
                m_tables.erase(tb);
            }
            m_reroot.erase(root);
        }
        c->next();
    }
    const auto missed_roots = m_reroot.size();
    m_reroot.clear();

    if (s.is_ok() && missed_roots) {
        std::string message("missing ");
        append_number(message, missed_roots);
        message.append(" root entr");
        message.append(missed_roots == 1 ? "y" : "ies");
        return Status::corruption(message);
    }
    return s;
}

auto Schema::vacuum_page(Id page_id, bool &success) -> Status
{
    return m_map.vacuum_one(page_id, *this, &success);
}

auto Schema::TEST_validate() const -> void
{
    for (const auto &[_, table] : m_tables) {
        if (table.table) {
            table_impl(table.table)->TEST_tree().TEST_validate();
        }
    }
}

SchemaCursor::~SchemaCursor()
{
    delete m_impl;
}

auto SchemaCursor::move_to_impl() -> void
{
    if (m_impl->is_valid()) {
        m_key = m_impl->key().to_string();
        m_value = m_impl->value().to_string();
    }
    m_status = m_impl->status();
    m_impl->clear();
}

auto SchemaCursor::next() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    m_impl->seek(m_key);
    if (m_impl->is_valid()) {
        m_impl->next();
    }
    move_to_impl();
}

auto SchemaCursor::previous() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    m_impl->seek(m_key);
    if (m_impl->is_valid()) {
        m_impl->previous();
    }
    move_to_impl();
}

} // namespace calicodb
