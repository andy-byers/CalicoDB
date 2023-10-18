// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCHEMA_H
#define CALICODB_SCHEMA_H

#include "calicodb/db.h"
#include "cursor_impl.h"
#include "internal.h"
#include "tree.h"

namespace calicodb
{

class Pager;
struct Stats;

class Schema final
{
public:
    explicit Schema(Pager &pager, Stats &stat);
    ~Schema();

    auto main_tree() -> Tree &
    {
        return m_main;
    }

    auto pager() const -> Pager &
    {
        return *m_pager;
    }

    auto use_tree(Tree *tree) -> void;
    auto create_tree(Id parent_id, Id &root_id_out) -> Status;
    auto open_tree(Id root_id) -> Tree *;

    // Remove a tree from the database
    // This routine will not drop a tree if it is currently open. Instead, a flag is
    // set on the bucket structure that contains the tree object, and the work is
    // deferred until the bucket is closed. It is the responsibility of the caller to
    // remove the referring bucket record from the parent.
    // Sub-buckets are dropped recursively, with each traversal stopping at the first
    // open bucket. For example, if an open bucket b needs to be dropped, and it has
    // sub-buckets s1,s2,... of its own, then only b is flagged to be dropped,
    // even if one or more of the s* are open. It doesn't matter if the open s* are
    // closed before b, since they are only accessible through b itself, and the
    // reference to b no longer exists in its parent. Furthermore, the s* have not
    // been dropped, so they should be locatable in b. When b is closed, the dropping
    // process is continued. b is no longer open, so its pages are put on the
    // freelist, and drop_tree() is called on the s*.
    auto drop_tree(Id root_id) -> Status;
    auto close_trees() -> void;
    auto find_open_tree(Id root_id) -> Tree *;
    auto vacuum() -> Status;

    // Locate the page containing the reference to the sub-bucket rooted at `root_id`
    auto find_parent_id(Id root_id, Id &parent_id_out) -> Status;

    auto TEST_validate() const -> void;

private:
    template <class Action>
    auto map_trees(bool include_main, Action &&action) const -> void
    {
        auto *t = &m_trees;
        do {
            // Don't access t after action() is called: it might have been destroyed.
            auto *next_t = t->next_entry;
            if (t->tree != &m_main || include_main) {
                if (!action(*t)) {
                    break;
                }
            }
            t = next_t;
        } while (t != &m_trees);
    }

    Pager *const m_pager;
    Stats *const m_stat;

    Tree m_main;

    // List containing a tree for each open bucket (including m_main).
    mutable Tree::ListEntry m_trees = {};
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H
