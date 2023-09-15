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
struct Stat;

// Representation of the database schema
// NOTE: The routines *_bucket(), where * is "create" or "open", do not set their
//       `c_out` parameter to nullptr on failure. It is the responsibility of the
//       caller to do so.
class Schema final
{
public:
    explicit Schema(Pager &pager, const Status &status, Stat &stat, char *scratch);

    [[nodiscard]] auto cursor() -> Cursor *
    {
        return m_schema;
    }

    auto close() -> void;
    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status;
    auto open_bucket(const Slice &name, Cursor *&c_out) -> Status;
    auto drop_bucket(const Slice &name) -> Status;

    struct UnpackedCursor {
        Tree *tree;
        CursorImpl *c;
    };
    auto unpack_and_use(Cursor &c) -> UnpackedCursor;
    auto use_tree(Tree *tree) -> void;

    auto vacuum() -> Status;

    [[nodiscard]] static auto decode_root_id(const Slice &data, Id &out) -> bool;
    [[nodiscard]] static auto encode_root_id(Id id, char *root_id_out) -> size_t;

    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto decode_and_check_root_id(const Slice &data, Id &out) -> bool;
    auto open_cursor(const Slice &name, Id root_id, Cursor *&c_out) -> Status;
    auto corrupted_root_id() -> Status;
    auto construct_or_reference_tree(const Slice &name, Id root_id) -> Tree *;
    auto find_open_tree(const Slice &name) -> Tree *;

    template <class Action>
    auto map_trees(bool include_schema, Action &&action) const -> void
    {
        auto *t = &m_trees;
        do {
            // Don't access t after action() is called: it may get destroyed.
            auto *next_t = t->next_entry;
            if (t->tree != &m_map || include_schema) {
                if (!action(*t)) {
                    break;
                }
            }
            t = next_t;
        } while (t != &m_trees);
    }

    friend class Tree;

    const Status *const m_status;
    Pager *const m_pager;
    char *const m_scratch;
    Stat *const m_stat;

    // The schema tree and a cursor over its contents. When m_cursor.is_valid(),
    // m_cursor.value() returns a non-human-readable string containing information
    // about the bucket that m_cursor is positioned on.
    Tree m_map;
    CursorImpl m_cursor;

    // List containing a tree for each open bucket (including the schema tree).
    mutable Tree::ListEntry m_trees = {};

    // Wrapper over m_cursor, returns a human-readable string for Cursor::value().
    Cursor *const m_schema;
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H
