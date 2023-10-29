// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "calicodb/bucket.h"
#include "encoding.h"
#include "status_internal.h"

namespace calicodb
{

Schema::Schema(Pager &pager, Stats &stat)
    : m_pager(&pager),
      m_stat(&stat),
      m_main(pager, stat, Id::root()),
      m_trees{&m_main, nullptr, nullptr}
{
    IntrusiveList::initialize(m_trees);
}

Schema::~Schema()
{
    // Tree destructor asserts that the refcount is 0 to catch leaked bucket handles.
    m_main.m_refcount = 0;
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

auto Schema::create_tree(Id parent_id, Id &root_id_out) -> Status
{
    CALICODB_EXPECT_GT(m_pager->page_count(), 0);
    use_tree(nullptr);
    return m_main.create(parent_id, root_id_out);
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
    if (auto *tree = find_open_tree(root_id)) {
        return tree;
    }
    if (auto *tree = Mem::new_object<Tree>(*m_pager, *m_stat, root_id)) {
        IntrusiveList::add_tail(tree->list_entry, m_trees);
        return tree;
    }
    return nullptr;
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

    Status s;
    Vector<Id> children;
    auto *next = drop.get();
    for (size_t nc = 0; next && s.is_ok(); ++nc) {
        if (drop->m_refcount) {
            // Trees that the user has a handle to cannot be dropped. Set the "dropped" flag instead.
            // Tree pages will be removed when the tree is closed.
            drop->m_dropped = true;
            drop.release();
        } else {
            IntrusiveList::remove(drop->list_entry);
            drop->deactivate_cursors(nullptr);

            Tree::Reroot rr;
            s = drop->destroy(rr, children);
            if (!s.is_ok()) {
                break;
            }
            if (rr.before != rr.after) {
                map_trees(false, [rr](auto &t) {
                    if (t.tree->root() == rr.before) {
                        t.tree->set_root(rr.after);
                        return false;
                    }
                    return true;
                });
                for (size_t i = nc; i < children.size(); ++i) {
                    if (children[i] == rr.before) {
                        children[i] = rr.after;
                        break;
                    }
                }
            }
        }

        next = nullptr;
        if (nc < children.size()) {
            drop.reset(open_tree(children[nc]));
            if (drop) {
                next = drop.get();
            } else {
                s = Status::no_memory();
            }
        }
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
    map_trees(true, []([[maybe_unused]] auto &t) {
        CALICODB_EXPECT_TRUE(t.tree->check_integrity().is_ok());
        return true;
    });
}

} // namespace calicodb
