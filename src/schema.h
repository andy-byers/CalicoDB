// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCHEMA_H
#define CALICODB_SCHEMA_H

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "tree.h"
#include "utils.h"
#include <unordered_map>

namespace calicodb
{

class Pager;
struct Stat;

// Representation of the database schema
class Schema final
{
public:
    explicit Schema(Pager &pager, const Status &status, Stat &stat, char *scratch);
    ~Schema() = default;

    [[nodiscard]] auto new_cursor() -> Cursor *;

    auto close() -> void;
    auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status;
    auto open_bucket(const Slice &name, Bucket &b_out) -> Status;
    auto drop_bucket(const Slice &name) -> Status;

    // Set a bucket `b` as the most-recently-used bucket
    // This class keeps the most-recently-used bucket's internal tree cursor active. A
    // root-to-leaf traversal can be avoided if the cursor is already on the correct node.
    // Other buckets should have their cursors cleared so that the system doesn't run out
    // of pager buffers (each live cursor holds onto a page reference).
    auto use_bucket(const Bucket &b) -> void;

    auto vacuum() -> Status;

    [[nodiscard]] static auto decode_root_id(const Slice &data, Id &out) -> bool;
    static auto encode_root_id(Id id, std::string &out) -> void;

    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto decode_and_check_root_id(const Slice &data, Id &out) -> bool;
    auto corrupted_root_id(const Slice &name, const Slice &value) -> Status;
    auto construct_bucket_state(Id root_id) -> Bucket;

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
    Tree m_map;

    // Pointer to the most-recently-accessed tree.
    const Tree *m_recent = nullptr;
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H
