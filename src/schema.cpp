// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "bucket_impl.h"
#include "calicodb/bucket.h"
#include "encoding.h"
#include "status_internal.h"

namespace calicodb
{

Schema::Schema(Pager &pager, Stats &stat)
    : m_pager(&pager),
      m_stat(&stat),
      m_main(pager, stat, pager.scratch(), Id::root()),
      m_trees{&m_main, nullptr, nullptr}
{
    IntrusiveList::initialize(m_trees);
}

auto Schema::find_parent_id(Id root_id, Id &parent_id_out) -> Status
{
    PointerMap::Entry entry;
    auto s = PointerMap::read_entry(*m_pager, root_id, entry);
    if (s.is_ok()) {
        if (entry.type != kTreeNode &&
            entry.type != kTreeRoot) {
            return Status::corruption();
        }
        parent_id_out = entry.back_ptr;
    }
    return s;
}

auto Schema::close_trees() -> void
{
    map_trees(false, [](auto &t) {
        Mem::delete_object(t.tree);
        return true;
    });
    IntrusiveList::initialize(m_trees);
    m_main.deactivate_cursors(nullptr);
}

auto Schema::create_tree(Id &root_id_out) -> Status
{
    CALICODB_EXPECT_GT(m_pager->page_count(), 0);
    use_tree(&m_main);
    return m_main.create(root_id_out);
}

auto Schema::find_open_tree(Id root_id) -> Tree *
{
    Tree *target = nullptr;
    map_trees(false, [root_id, &target](auto &t) {
        if (root_id == t.tree->root()) {
            target = t.tree;
            return false;
        }
        return true;
    });
    return target;
}

auto Schema::open_tree(Id root_id) -> Tree *
{
    CALICODB_EXPECT_GT(m_pager->page_count(), 0);
    if (auto *already_open = find_open_tree(root_id)) {
        return already_open;
    }
    auto *tree = Mem::new_object<Tree>(*m_pager, *m_stat, m_pager->scratch(), root_id);
    if (tree) {
        IntrusiveList::add_tail(tree->list_entry, m_trees);
    }
    return tree;
}

auto Schema::use_tree(Tree *tree) -> void
{
    map_trees(true, [tree](auto &t) {
        if (t.tree != tree) {
            t.tree->deactivate_cursors(nullptr);
        }
        return true;
    });
}

auto Schema::drop_tree(Id root_id) -> Status
{
    use_tree(nullptr);

    ObjectPtr<Tree> drop(open_tree(root_id));
    if (!drop) {
        return Status::no_memory();
    }
    IntrusiveList::remove(drop->list_entry);
    drop->deactivate_cursors(nullptr);

    Tree::Reroot rr;
    auto s = drop->destroy(rr);
    if (s.is_ok() && rr.before != rr.after) {
        // Update the in-memory root ID.
        map_trees(false, [rr](auto &t) {
            if (t.tree->m_root_id == rr.before) {
                t.tree->m_root_id = rr.after;
                return false;
            }
            return true;
        });
        // Update the on-disk root ID.
        // TODO: Use back pointer in pointer map for root_id to find target node. Search that node for the
        //       record with value root_id. Could also follow back pointers to the root of the parent bucket,
        //       then open that tree. Then, use the tree object to modify the record.
    }
    return s;
}

auto Schema::vacuum() -> Status
{
    use_tree(nullptr);
    return m_main.vacuum();
}

auto Schema::TEST_validate() const -> void
{
    map_trees(true, [](auto &t) {
        t.tree->TEST_validate();
        return true;
    });
}

} // namespace calicodb
