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

    auto main_tree() -> Tree &
    {
        return m_main;
    }

    auto pager() const -> Pager &
    {
        return *m_pager;
    }

    auto create_tree(Id &root_id_out) -> Status;
    auto drop_tree(Id root_id) -> Status;
    auto open_tree(Id root_id) -> Tree *;
    auto use_tree(Tree *tree) -> void;
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
