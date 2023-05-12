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

Schema::Schema(Pager &pager, Status &status, bool write)
    : m_status(&status),
      m_pager(&pager),
      m_map(new Tree(pager, nullptr)),
      m_write(write)
{
}

Schema::~Schema()
{
    delete m_map;
}

auto Schema::corrupted_root_id(const std::string &table_name, const Slice &value) -> Status
{
    std::string message("root entry for table \"" + table_name + "\" is corrupted: ");
    message.append(escape_string(value));
    auto s = Status::corruption(message);
    if (m_status->is_ok()) {
        *m_status = s;
    }
    return s;
}

auto Schema::new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    CALICODB_EXPECT_FALSE(out);

    Status s;
    if (m_pager->page_count() == 0) {
        if (m_write) {
            m_pager->initialize_root();
        } else {
            // Read-only transaction cannot create a new table, and there are no tables
            // created yet in this database (the schema table hasn't even been created).
            // This code path ends up returning an invalid argument status.
            s = Status::not_found();
        }
    }

    std::string value;
    if (s.is_ok()) {
        s = m_map->get(name, &value);
    }

    Id root_id;
    if (s.is_ok()) {
        if (options.error_if_exists) {
            return Status::invalid_argument("table \"" + name + "\" already exists");
        }
        if (!decode_root_id(value, root_id)) {
            return corrupted_root_id(name, value);
        }
    } else if (s.is_not_found()) {
        if (!m_write || !options.create_if_missing) {
            return Status::invalid_argument("table \"" + name + "\" does not exist");
        }
        CALICODB_TRY(Tree::create(*m_pager, false, &root_id));
        encode_root_id(root_id, value);
        CALICODB_TRY(m_map->put(name, value));
    }
    return construct_table_state(name, root_id, out);
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

auto Schema::construct_table_state(const std::string &name, Id root_id, Table *&out) -> Status
{
    auto itr = m_trees.find(root_id);
    if (itr != end(m_trees) && itr->second.tree) {
        return Status::invalid_argument("table \"" + name + "\" is already open");
    }
    itr = m_trees.insert(itr, {root_id, {}});
    itr->second.root = root_id;
    itr->second.tree = new Tree(*m_pager, &itr->second.root);
    out = new TableImpl(itr->second.tree, *m_status, m_write);
    return Status::ok();
}

auto Schema::drop_table(const std::string &name) -> Status
{
    if (!m_write) {
        return Status::readonly();
    }
    std::string value;
    CALICODB_TRY(m_map->get(name, &value));

    Id root_id;
    if (!decode_root_id(value, root_id)) {
        return corrupted_root_id(name, value);
    }
    auto itr = m_trees.find(root_id);
    if (itr != end(m_trees) && itr->second.tree) {
        return Status::invalid_argument(
            "table \"" + name + "\" is still open");
    }
    Tree drop(*m_pager, &root_id);
    CALICODB_TRY(Tree::destroy(drop));
    CALICODB_TRY(m_map->erase(name));
    m_trees.erase(root_id);
    return Status::ok();
}

auto Schema::vacuum_reroot(Id old_id, Id new_id) -> void
{
    auto tree = m_trees.find(old_id);
    if (tree == end(m_trees)) {
        RootedTree rooted;
        rooted.root = old_id;
        m_trees.insert(tree, {old_id, rooted});
    }
    // Rekey the tree. Leave the root ID stored in the tree state set
    // to the original root.
    auto node = m_trees.extract(old_id);
    CALICODB_EXPECT_FALSE(!node);
    node.key() = new_id;
    const auto root_id = node.mapped().root;
    m_trees.insert(std::move(node));

    // Map the original root ID to the newest root ID.
    m_reroot.insert_or_assign(root_id, new_id);
}

auto Schema::vacuum_finish() -> Status
{
    auto *cursor = CursorInternal::make_cursor(*m_map);
    cursor->seek_first();
    ScopeGuard guard = [cursor] {
        delete cursor;
    };

    Status s;
    while (cursor->is_valid()) {
        Id old_id;
        if (!decode_root_id(cursor->value(), old_id)) {
            return corrupted_root_id(cursor->key().to_string(), cursor->value());
        }
        const auto root = m_reroot.find(old_id);
        if (root != end(m_reroot)) {
            std::string value;
            encode_root_id(root->second, value);
            // Update the database schema with the new root page ID for this tree.
            s = m_map->put(cursor->key(), value);
            if (!s.is_ok()) {
                break;
            }
            // Update the in-memory root stored by each Tree.
            auto tree = m_trees.find(root->second);
            CALICODB_EXPECT_NE(tree, end(m_trees));
            if (tree->second.tree) {
                tree->second.root = root->second;
            } else {
                // This tree is not actually open. The RootedTree entry exists
                // so that vacuum_reroot() could find the original root ID (or
                // the table was just closed by the user, either way, the entry
                // should be removed).
                m_trees.erase(tree);
            }
            m_reroot.erase(root);
        }
        cursor->next();
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
    inform_live_cursors();
    return s;
}

auto Schema::vacuum_page(Id page_id, bool &success) -> Status
{
    return m_map->vacuum_one(page_id, *this, &success);
}

auto Schema::inform_live_cursors() -> void
{
    for (auto &[_, tree] : m_trees) {
        if (tree.tree) {
            tree.tree->inform_cursors();
        }
    }
}

auto Schema::TEST_validate() const -> void
{
    for (const auto &[_, tree] : m_trees) {
        if (tree.tree) {
            tree.tree->TEST_validate();

            // Make sure the last vacuum didn't miss any roots.
            CALICODB_EXPECT_EQ(tree.root, tree.tree->root());
        }
    }
}

} // namespace calicodb
