// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "table_impl.h"
#include "tree.h"

namespace calicodb
{

Schema::Schema(Pager &pager)
    : m_map(new Tree(pager, nullptr))
{
}

Schema::~Schema()
{
    delete m_map;
}

auto Schema::new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status
{
    std::string value;
    auto s = m_map->get(name, &value);

    Id root_id;
    if (s.is_ok()) {
        if (options.error_if_exists) {
            return Status::invalid_argument("table exists");
        }
        if (value.size() != Id::kSize) {
            return Status::corruption("root ID is corrupted");
        }
        root_id.value = get_u32(value);
    } else if (s.is_not_found()) {
        if (!options.create_if_missing) {
            return Status::invalid_argument("table does not exist");
        }
        CALICODB_TRY(Tree::create(*m_pager, false, &root_id));
        value.resize(Id::kSize);
        put_u32(value.data(), root_id.value);
        CALICODB_TRY(m_map->put(name, value));
    }
    auto itr = m_trees.find(root_id);
    if (itr == end(m_trees) || itr->second.tree == nullptr) {
        itr = m_trees.insert(itr, {root_id, {}});
        itr->second.root = root_id;
        itr->second.tree = new Tree(*m_pager, &itr->second.root);
    }
    out = new TableImpl(itr->second.tree);
    return Status::ok();
}

auto Schema::drop_table(const std::string &name) -> Status
{
    std::string value;
    CALICODB_TRY(m_map->get(name, &value));
    if (value.size() != Id::kSize) {
        return Status::corruption("root ID is corrupted");
    }
    // TODO: Check if the tree is empty, if not, empty it. I was just iterating through
    //       with a cursor and erasing all the records, but it would be better to do an
    //       inorder traversal and move all the pages to the freelist.
    m_trees.erase(Id(get_u32(value)));
    return m_map->erase(name);
}

auto Schema::vacuum_reroot(Id old_id, Id new_id) -> void
{
    auto node = m_trees.extract(old_id);
    CALICODB_EXPECT_FALSE(!node);
    node.key() = new_id;
    m_trees.insert(std::move(node));
}

auto Schema::vacuum_finish() -> Status
{
    return Status::not_supported("not implemented...");
}

} // namespace calicodb
