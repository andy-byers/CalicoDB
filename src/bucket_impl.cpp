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
    if (m_tree->m_refcount-- == 1) {
        m_tree->deactivate_cursors(nullptr);
        IntrusiveList::remove(m_tree->list_entry);
        if (m_tree->m_dropped) {
            // This bucket was dropped earlier, but its pages could not be recycled because the user
            // still had this object alive. Recycle the tree pages now. Schema will set the pager
            // status if an error occurs.
            (void)m_schema->drop_tree(m_tree->root());
        }
        Mem::delete_object(m_tree);
    }
}

auto BucketImpl::new_cursor() const -> Cursor *
{
    return new (std::nothrow) CursorImpl(*m_tree);
}

auto BucketImpl::create_bucket(const Slice &key, Bucket **b_out) -> Status
{
    return create_bucket_impl(key, true, b_out);
}

auto BucketImpl::create_bucket_if_missing(const Slice &key, Bucket **b_out) -> Status
{
    return create_bucket_impl(key, false, b_out);
}

auto BucketImpl::create_bucket_impl(const Slice &key, bool error_if_exists, Bucket **b_out) -> Status
{
    if (b_out) {
        *b_out = nullptr;
    }
    return pager_write(m_schema->pager(), [this, key, error_if_exists, b_out] {
        Id root_id;
        m_cursor.find(key);
        auto s = m_cursor.status();
        if (!s.is_ok()) {
            return s;
        } else if (!m_cursor.is_valid()) {
            s = m_schema->create_tree(m_tree->root(), root_id);
            if (s.is_ok()) {
                char buf[sizeof(uint32_t)];
                put_u32(buf, root_id.value); // Root ID encoded as record value
                s = m_tree->put(*TREE_CURSOR(m_cursor), key, Slice(buf, sizeof(buf)), true);
            }
        } else if (error_if_exists) {
            s = Status::invalid_argument("bucket already exists");
        } else {
            root_id = TREE_CURSOR(m_cursor)->bucket_root_id();
        }

        if (s.is_ok() && b_out) {
            if (open_bucket_impl(root_id, *b_out)) {
                s = Status::no_memory();
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

        if (open_bucket_impl(root_id, b_out)) {
            s = Status::no_memory();
        }
        return s;
    });
}

auto BucketImpl::open_bucket_impl(Id root_id, Bucket *&b_out) const -> int
{
    if (auto *tree = m_schema->open_tree(root_id)) {
        b_out = new (std::nothrow) BucketImpl(*m_schema, *tree);
        if (b_out) {
            ++tree->m_refcount;
            return 0;
        } else if (tree->m_refcount == 0) {
            Mem::delete_object(tree);
        }
    }
    return -1;
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

        s = m_tree->erase(*TREE_CURSOR(m_cursor), true);
        if (!s.is_ok()) {
            return s;
        }

        return m_schema->drop_tree(root_id);
    });
}

auto BucketImpl::get(const Slice &key, CALICODB_STRING *value_out) const -> Status
{
    auto s = pager_read(m_schema->pager(), [this, key, value_out] {
        m_cursor.find(key);
        auto s = m_cursor.status();
        if (!m_cursor.is_valid()) {
            return s.is_ok() ? Status::not_found() : s;
        } else if (m_cursor.is_bucket()) {
            return Status::incompatible_value();
        }

        CALICODB_EXPECT_TRUE(s.is_ok()); // Cursor invariant
        if (value_out) {
            const auto value = m_cursor.value();
            // CALICODB_STRING must return a valid address from its data() method, even if it has 0 length. This
            // is how std::string behaves (see https://en.cppreference.com/w/cpp/string/basic_string/data).
            value_out->resize(value.size());
            if (value_out->size() == value.size()) {
                std::memcpy(value_out->data(), value.data(), value.size());
            } else {
                // CALICODB_STRING was not able to get enough memory to resize. This will never happen when using
                // the default of std::string (std::string throws std::bad_alloc).
                return Status::no_memory();
            }
        }
        return s;
    });
    if (value_out && !s.is_ok()) {
        value_out->clear();
    }
    return s;
}

auto BucketImpl::put(const Slice &key, const Slice &value) -> Status
{
    return pager_write(m_schema->pager(), [this, key, value] {
        return m_tree->put(*TREE_CURSOR(m_cursor), key, value, false);
    });
}

auto BucketImpl::put(Cursor &c, const Slice &value) -> Status
{
    CALICODB_EXPECT_EQ(&TREE_CURSOR(c)->tree(), m_tree);
    return pager_write(m_schema->pager(), [this, &c, value] {
        return m_tree->put(*TREE_CURSOR(c), c.key(), value, false);
    });
}

auto BucketImpl::erase(const Slice &key) -> Status
{
    return pager_write(m_schema->pager(), [this, key] {
        m_cursor.find(key);
        if (m_cursor.is_valid()) {
            if (m_cursor.is_bucket()) {
                return Status::incompatible_value();
            }
            return m_tree->erase(*TREE_CURSOR(m_cursor), false);
        }
        return m_cursor.status();
    });
}

auto BucketImpl::erase(Cursor &c) -> Status
{
    CALICODB_EXPECT_EQ(&TREE_CURSOR(c)->tree(), m_tree);
    return pager_write(m_schema->pager(), [this, &c] {
        if (c.is_valid()) {
            return m_tree->erase(*TREE_CURSOR(c), false);
        } else if (c.status().is_ok()) {
            return Status::invalid_argument();
        } else {
            return c.status();
        }
    });
}

auto BucketImpl::TEST_validate() const -> void
{
    m_tree->TEST_validate();
}

} // namespace calicodb
