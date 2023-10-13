// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bucket_impl.h"
#include "encoding.h"
#include "schema.h"
#include "status_internal.h"

namespace calicodb
{

namespace
{

auto no_bucket(const Slice &name) -> Status
{
    return StatusBuilder(Status::kInvalidArgument)
        .append("bucket \"")
        .append_escaped(name)
        .append("\" does not exist")
        .build();
}

auto decode_root_id(const Slice &data, Id &root_id_out) -> int
{
    if (data.size() == sizeof(uint32_t)) {
        root_id_out.value = get_u32(data);
        return 0;
    }
    return -1;
}

auto encode_root_id(Id id, char *root_id_out) -> size_t
{
    put_u32(root_id_out, id.value);
    return sizeof(uint32_t);
}

} // namespace

#define TREE_CURSOR(c) (static_cast<TreeCursor *>((c).handle()))

BucketImpl::BucketImpl(Schema &schema, Tree &tree)
    : m_cursor(tree),
      m_schema(&schema),
      m_tree(&tree)
{
}

BucketImpl::~BucketImpl()
{
    m_tree->deactivate_cursors(nullptr);
}

auto BucketImpl::new_cursor() const -> Cursor *
{
    return new (std::nothrow) CursorImpl(*m_tree);
}

auto BucketImpl::create_bucket(const Slice &key, Bucket **b_out) -> Status
{
    if (b_out) {
        *b_out = nullptr;
    }
    return pager_write(m_schema->pager(), [this, key, b_out] {
        m_cursor.find(key);
        auto s = m_cursor.status();
        if (m_cursor.is_valid()) {
            return Status::invalid_argument("bucket already exists");
        } else if (!s.is_ok()) {
            return s;
        }

        Id root_id;
        s = m_schema->create_tree(root_id);
        if (!s.is_ok()) {
            return s;
        }

        char buf[kVarintMaxLength];
        const size_t len = encode_root_id(root_id, buf);
        s = m_tree->put(*TREE_CURSOR(m_cursor), key, Slice(buf, len), true);
        if (s.is_ok() && b_out) {
            s = Status::no_memory();
            if (auto *tree = m_schema->open_tree(root_id)) {
                *b_out = new (std::nothrow) BucketImpl(*m_schema, *tree);
                s = *b_out ? Status::ok() : s;
            }
        }
        return s;
    });
}

auto BucketImpl::open_bucket(const Slice &key, Bucket *&b_out) const -> Status
{
    b_out = nullptr;
    return pager_read(m_schema->pager(), [this, key, &b_out] {
        m_cursor.find(key);
        auto s = m_cursor.status();
        if (!m_cursor.is_valid()) {
            return s.is_ok() ? no_bucket(key) : s;
        }

        CALICODB_EXPECT_TRUE(s.is_ok()); // Cursor invariant
        const auto root_id = TREE_CURSOR(m_cursor)->bucket_root_id();

        s = Status::no_memory();
        if (auto *tree = m_schema->open_tree(root_id)) {
            b_out = new (std::nothrow) BucketImpl(*m_schema, *tree);
            s = b_out ? Status::ok() : s;
        }
        return s;
    });
}

auto BucketImpl::drop_bucket(const Slice &key) -> Status
{
    return pager_write(m_schema->pager(), [this, key] {
        m_cursor.find(key);
        auto s = m_cursor.status();
        if (!m_cursor.is_valid()) {
            return s.is_ok() ? no_bucket(key) : s;
        }

        CALICODB_EXPECT_TRUE(s.is_ok()); // Cursor invariant
        const auto root_id = TREE_CURSOR(m_cursor)->bucket_root_id();

        s = m_tree->erase(*TREE_CURSOR(m_cursor));
        if (!s.is_ok()) {
            return s;
        }

        return m_schema->drop_tree(root_id);
    });
}

auto BucketImpl::put(const Slice &key, const Slice &value) -> Status
{
    return pager_write(m_schema->pager(), [this, key, value] {
        return m_tree->put(*TREE_CURSOR(m_cursor), key, value, false);
    });
}

auto BucketImpl::erase(const Slice &key) -> Status
{
    return pager_write(m_schema->pager(), [this, key] {
        m_cursor.find(key);
        if (m_cursor.is_valid()) {
            if (m_cursor.is_bucket()) {
                return Status::invalid_argument("cannot erase bucket record");
            }
            return m_tree->erase(*TREE_CURSOR(m_cursor));
        }
        return m_cursor.status();
    });
}

auto BucketImpl::put(Cursor &c, const Slice &value) -> Status
{
    CALICODB_EXPECT_EQ(&TREE_CURSOR(c)->tree(), m_tree);
    return pager_write(m_schema->pager(), [this, &c, value] {
        return m_tree->put(*TREE_CURSOR(c), c.key(), value, false);
    });
}

auto BucketImpl::erase(Cursor &c) -> Status
{
    CALICODB_EXPECT_EQ(&TREE_CURSOR(c)->tree(), m_tree);
    return pager_write(m_schema->pager(), [this, &c] {
        return m_tree->erase(*TREE_CURSOR(c));
    });
}

auto BucketImpl::TEST_validate() const -> void
{
    m_tree->TEST_validate();
}

auto BucketImpl::decode_and_check_root_id(const Slice &data, Id &root_id_out) const -> Status
{
    Status s;
    auto &pager = m_schema->pager();
    if (decode_root_id(data.data(), root_id_out)) {
        s = Status::corruption("root ID is corrupted");
    } else if (root_id_out.value > pager.page_count()) {
        s = StatusBuilder::corruption("root ID %u is out of bounds (page count is %u)",
                                      root_id_out.value, pager.page_count());
        pager.set_status(s);
    }
    return s;
}

} // namespace calicodb
