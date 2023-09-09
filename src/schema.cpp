// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "logging.h"

namespace calicodb
{

namespace
{

constexpr size_t kMaxRootEntryLen = kVarintMaxLength;

auto no_bucket(const Slice &name) -> Status
{
    return StatusBuilder(Status::kInvalidArgument)
        .append("bucket \"")
        .append_escaped(name)
        .append("\" does not exist")
        .build();
}

class SchemaCursor : public Cursor
{
    Cursor *const m_c;

public:
    explicit SchemaCursor(Cursor &c)
        : m_c(&c)
    {
    }

    ~SchemaCursor() override = default;

    [[nodiscard]] auto handle() -> void * override
    {
        return m_c->handle();
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_c->is_valid();
    }

    auto status() const -> Status override
    {
        return m_c->status();
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        return m_c->key();
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        // TODO: Parse the options string into a human-readable format, save it internally and return a slice to it.
        //       We should skip the varint root ID or convert it into a decimal string.
        CALICODB_EXPECT_TRUE(is_valid());
        // return m_c->value();
        return "";
    }

    auto find(const Slice &key) -> void override
    {
        return m_c->find(key);
    }

    auto seek(const Slice &key) -> void override
    {
        return m_c->seek(key);
    }

    auto seek_first() -> void override
    {
        m_c->seek_first();
    }

    auto seek_last() -> void override
    {
        m_c->seek_last();
    }

    auto next() -> void override
    {
        m_c->next();
    }

    auto previous() -> void override
    {
        m_c->previous();
    }
};

} // namespace

Schema::Schema(Pager &pager, const Status &status, Stat &stat, char *scratch)
    : m_status(&status),
      m_pager(&pager),
      m_scratch(scratch),
      m_stat(&stat),
      m_map(pager, stat, scratch, Id::root(), String()),
      m_cursor(m_map),
      m_trees{"", &m_map, nullptr, nullptr},
      m_schema(Mem::new_object<SchemaCursor>(m_cursor))
{
    IntrusiveList::initialize(m_trees);
}

auto Schema::close() -> void
{
    map_trees(false, [](auto &t) {
        Mem::delete_object(t.tree);
        return true;
    });
    IntrusiveList::initialize(m_trees);

    m_cursor.reset();
    Mem::delete_object(m_schema);
    m_map.save_all_cursors();
}

auto Schema::corrupted_root_id() -> Status
{
    auto s = Status::corruption("bucket root page ID is corrupted");
    if (m_pager->mode() > Pager::kRead) {
        m_pager->set_status(s);
    }
    return s;
}

auto Schema::open_cursor(const Slice &name, Id root_id, Cursor *&c_out) -> Status
{
    if (auto *tree = construct_or_reference_tree(name, root_id)) {
        c_out = new (std::nothrow) CursorImpl(*tree);
    }
    return c_out ? Status::ok() : Status::no_memory();
}

auto Schema::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    if (m_pager->page_count() == 0) {
        // Initialize the database file header, as well as the schema tree's root page.
        m_pager->initialize_root();
    }
    Id root_id;
    use_tree(&m_map);
    const auto bucket_exists = m_cursor.seek_to_leaf(
        name, CursorImpl::kSeekNormal);
    auto s = m_cursor.status();
    if (bucket_exists) {
        CALICODB_EXPECT_TRUE(s.is_ok());
        s = m_cursor.fetch_user_payload();
        if (!s.is_ok()) {
            return s; // Unable to read the existing schema record.
        } else if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
            s = corrupted_root_id();
        } else if (options.error_if_exists) {
            s = Status::invalid_argument("bucket already exists");
        }
    } else if (s.is_ok()) {
        s = m_map.create(&root_id);
        if (s.is_ok()) {
            char buf[kMaxRootEntryLen];
            const auto len = encode_root_id(root_id, buf);
            // On success, this call will leave m_cursor is on the schema record of the bucket that we
            // need to open a cursor on below. It may invalidate `name`, so m_cursor.key() is used
            // instead. The 2 should be equivalent.
            s = m_map.put(m_cursor, name, Slice(buf, len));
        }
    }

    if (s.is_ok() && c_out) {
        s = open_cursor(m_cursor.key(), root_id, *c_out);
    }
    return s;
}

auto Schema::open_bucket(const Slice &name, Cursor *&c_out) -> Status
{
    if (m_pager->page_count() == 0) {
        return Status::invalid_argument();
    }
    // NOTE: Cannot use `name` again: this routine may have been called like
    //       open_bucket(schema.key(), c), where "schema" is the result of
    //       Tx::schema(), which is actually m_cursor.
    m_cursor.find(name);
    auto s = m_cursor.status();

    Id root_id;
    if (!m_cursor.is_valid()) {
        return s.is_ok() ? no_bucket(name) : s;
    } else if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
        return corrupted_root_id();
    }
    return open_cursor(m_cursor.key(), root_id, c_out);
}

auto Schema::decode_root_id(const Slice &data, Id &out) -> bool
{
    uint32_t num;
    if (decode_varint(data.data(), data.data() + data.size(), num)) {
        out.value = static_cast<uint32_t>(num);
        return true;
    }
    return false;
}

auto Schema::decode_and_check_root_id(const Slice &data, Id &out) -> bool
{
    if (!decode_root_id(data, out) || out.value > m_pager->page_count()) {
        return false;
    }
    return true;
}

auto Schema::encode_root_id(Id id, char *root_id_out) -> size_t
{
    const auto *end = encode_varint(root_id_out, id.value);
    return static_cast<size_t>(end - root_id_out);
}

auto Schema::find_open_tree(const Slice &name) -> Tree *
{
    Tree *target = nullptr;
    map_trees(false, [name, &target](auto &t) {
        if (name == t.name) {
            target = t.tree;
            return false;
        } else if (IntrusiveList::is_empty(t.tree->m_active_list) &&
                   IntrusiveList::is_empty(t.tree->m_inactive_list)) {
            IntrusiveList::remove(t);
            Mem::delete_object(t.tree);
        }
        return true;
    });
    return target;
}

auto Schema::construct_or_reference_tree(const Slice &name, Id root_id) -> Tree *
{
    if (auto *already_open = find_open_tree(name)) {
        return already_open;
    }

    String name_str;
    if (!root_id.is_root() && append_strings(name_str, name)) {
        return nullptr;
    }

    auto *tree = Mem::new_object<Tree>(*m_pager, *m_stat, m_scratch,
                                       root_id, move(name_str));
    if (tree) {
        IntrusiveList::add_tail(tree->list_entry, m_trees);
    }
    return tree;
}

auto Schema::unpack_and_use(Cursor &c) -> UnpackedCursor
{
    auto *c_impl = static_cast<CursorImpl *>(c.handle());
    auto *tree = Tree::get_tree(*c_impl);
    use_tree(tree);
    return {tree, c_impl};
}

auto Schema::use_tree(Tree *tree) -> void
{
    map_trees(true, [tree](auto &t) {
        if (t.tree != tree) {
            t.tree->save_all_cursors();
        }
        return true;
    });
}

auto Schema::drop_bucket(const Slice &name) -> Status
{
    use_tree(&m_map);
    m_cursor.find(name);
    auto s = m_cursor.status();
    if (!m_cursor.is_valid()) {
        return s.is_ok() ? no_bucket(name) : s;
    }
    CALICODB_EXPECT_TRUE(s.is_ok());

    Id root_id;
    if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
        return corrupted_root_id();
    }
    ObjectPtr<Tree> drop(construct_or_reference_tree(m_cursor.key(), root_id));
    if (!drop) {
        return Status::no_memory();
    }
    IntrusiveList::remove(drop->list_entry);
    drop->save_all_cursors();
    m_map.save_all_cursors();

    Tree::Reroot rr;
    s = m_map.destroy(*drop, rr);
    if (s.is_ok()) {
        s = m_map.erase(m_cursor);
    }
    if (s.is_ok() && rr.before != rr.after) {
        map_trees(false, [rr](auto &t) {
            if (t.tree->m_root_id == rr.before) {
                t.tree->m_root_id = rr.after;
                return false;
            }
            return true;
        });
        // TODO: This is unfortunate. Keep a reverse mapping?
        auto found = false;
        m_cursor.seek_first();
        while (m_cursor.is_valid()) {
            Id found_id;
            if (!decode_and_check_root_id(m_cursor.value(), found_id)) {
                return corrupted_root_id();
            }
            if (found_id == rr.before) {
                char buf[kMaxRootEntryLen];
                const auto len = encode_root_id(rr.after, buf);
                s = m_map.put(m_cursor, m_cursor.key(), Slice(buf, len));
                found = true;
                break;
            }
            m_cursor.next();
        }
        if (s.is_ok() && !found) {
            s = Status::corruption();
        }
    }
    return s;
}

auto Schema::vacuum() -> Status
{
    use_tree(nullptr);
    return m_map.vacuum();
}

auto Schema::TEST_validate() const -> void
{
    map_trees(true, [](auto &t) {
        t.tree->TEST_validate();
        return true;
    });
}

} // namespace calicodb
