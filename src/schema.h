// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCHEMA_H
#define CALICODB_SCHEMA_H

#include "calicodb/db.h"
#include "cursor_impl.h"
#include "tree.h"
#include "utils.h"
#include <unordered_map>

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

    [[nodiscard]] auto cursor() -> Cursor &
    {
        return *m_schema;
    }

    auto close() -> void;
    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status;
    auto open_bucket(const Slice &name, Cursor *&c_out) -> Status;
    auto drop_bucket(const Slice &name) -> Status;

    auto unpack_and_use(Cursor &c) -> std::pair<Tree &, CursorImpl &>;
    auto use_tree(Tree &tree) -> void;

    auto vacuum() -> Status;

    [[nodiscard]] static auto decode_root_id(const Slice &data, Id &out) -> bool;
    [[nodiscard]] static auto encode_root_id(Id id, char *root_id_out) -> size_t;

    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto decode_and_check_root_id(const Slice &data, Id &out) -> bool;
    auto corrupted_root_id() -> Status;
    auto construct_or_reference_tree(Id root_id) -> Tree *;

    template <class T>
    using HashMap = std::unordered_map<Id, T, Id::Hash>;

    friend class Tree;

    // Change the root page of a bucket from `old_id` to `new_id` during vacuum
    auto vacuum_reroot(Id old_id, Id new_id) -> void;

    // Write updated root page IDs for buckets that were closed during vacuum, if any
    // buckets were rerooted
    auto vacuum_finish() -> Status;

    struct RootedTree {
        Tree *tree = nullptr;
        Id root;
    };

    HashMap<RootedTree> m_trees;
    HashMap<Id> m_reroot;

    const Status *const m_status;
    Pager *const m_pager;
    char *const m_scratch;
    Stat *const m_stat;

    // The schema tree and a cursor over its contents. When m_cursor.is_valid(),
    // m_cursor.value() returns a non-human-readable string containing information
    // about the bucket that m_cursor is positioned on.
    Tree m_map;
    CursorImpl m_cursor;

    // Wrapper over m_cursor, returns a human-readable string for Cursor::value().
    Cursor *const m_schema;

    // Pointer to the most-recently-accessed tree.
    const Tree *m_recent = nullptr;
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H
