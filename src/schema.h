// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCHEMA_H
#define CALICODB_SCHEMA_H

#include "calicodb/db.h"
#include "page.h"
#include "tree.h"
#include <unordered_map>

namespace calicodb
{

class SchemaCursor : public Cursor
{
    mutable Status m_status;
    std::string m_key;
    std::string m_value;
    CursorImpl *m_impl;

    auto move_to_impl() -> void;

public:
    explicit SchemaCursor(Tree &tree)
        : m_impl(new CursorImpl(tree))
    {
    }

    ~SchemaCursor() override;

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_status.is_ok() && !m_key.empty();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_status;
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_key;
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_value;
    }

    auto seek(const Slice &key) -> void override
    {
        m_impl->seek(key);
        move_to_impl();
    }

    auto seek_first() -> void override
    {
        m_impl->seek_first();
        move_to_impl();
    }

    auto seek_last() -> void override
    {
        m_impl->seek_last();
        move_to_impl();
    }

    auto next() -> void override;
    auto previous() -> void override;
};

// Representation of the database schema
class Schema final
{
public:
    explicit Schema(Pager &pager, Status &status, char *scratch)
        : m_status(&status),
          m_pager(&pager),
          m_scratch(scratch),
          m_map(pager, scratch, nullptr)
    {
    }

    [[nodiscard]] auto new_cursor() -> Cursor *
    {
        return new SchemaCursor(m_map);
    }

    auto close() -> void
    {
        for (const auto &[_, state] : m_trees) {
            delete state.tree;
        }
        m_map.m_cursor.clear();
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status;
    auto open_bucket(const Slice &name, Bucket &b_out) -> Status;
    auto drop_bucket(const Slice &name) -> Status;
    auto vacuum_freelist() -> Status;

    // Write updated root page IDs for buckets that were closed during vacuum, if any
    // buckets were rerooted
    auto vacuum_finish() -> Status;

    [[nodiscard]] static auto decode_root_id(const Slice &data, Id &out) -> bool;
    static auto encode_root_id(Id id, std::string &out) -> void;

    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto decode_and_check_root_id(const Slice &data, Id &out) -> bool;
    auto corrupted_root_id(const Slice &name, const Slice &value) -> Status;
    auto construct_bucket_state(Id root_id) -> Bucket;

    template <class T>
    using HashMap = std::unordered_map<Id, T, Id::Hash>;

    // Change the root page of a bucket from `old_id` to `new_id` during vacuum
    friend class Tree;
    auto vacuum_reroot(Id old_id, Id new_id) -> void;

    struct RootedTree {
        Tree *tree = nullptr;
        Id root;
    };

    HashMap<RootedTree> m_trees;
    HashMap<Id> m_reroot;
    Status *m_status;
    Pager *m_pager;
    char *m_scratch;
    Tree m_map;
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H
