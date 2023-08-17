// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "logging.h"
#include "tree.h"

namespace calicodb
{

static constexpr size_t kMaxRootEntryLen = kVarintMaxLength;
static const Status kNoBucket = Status::invalid_argument("bucket does not exist");

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

    [[nodiscard]] auto status() const -> Status override
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

Schema::Schema(Pager &pager, const Status &status, Stat &stat, char *scratch)
    : m_status(&status),
      m_pager(&pager),
      m_scratch(scratch),
      m_stat(&stat),
      m_map(pager, stat, scratch, Id::root(), UniqueBuffer()),
      m_cursor(m_map),
      m_schema(Alloc::new_object<SchemaCursor>(m_cursor))
{
    IntrusiveList::initialize(m_trees);
}

auto Schema::close() -> void
{
    map_trees([](auto &t) {
        Alloc::delete_object(t.tree);
        return true;
    });
    IntrusiveList::initialize(m_trees);

    m_cursor.reset();
    Alloc::delete_object(m_schema);
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

auto Schema::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    if (m_pager->page_count() == 0) {
        // Initialize the database file header, as well as the schema tree's root page.
        m_pager->initialize_root();
    }

    Id root_id;
    m_cursor.find(name);
    auto s = m_cursor.status();
    if (m_cursor.is_valid()) {
        CALICODB_EXPECT_TRUE(s.is_ok());
        if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
            s = corrupted_root_id();
        } else if (options.error_if_exists) {
            s = Status::invalid_argument("bucket already exists");
        }
    } else if (s.is_ok()) {
        s = m_map.create(&root_id);
        if (s.is_ok()) {
            // TODO: Encode persistent bucket options here.
            char buf[kMaxRootEntryLen];
            const auto len = encode_root_id(root_id, buf);
            s = m_map.put(m_cursor, name, Slice(buf, len));
        }
    }

    if (s.is_ok() && c_out) {
        if (auto *tree = construct_or_reference_tree(name, root_id)) {
            *c_out = new (std::nothrow) CursorImpl(*tree);
        }
        if (*c_out == nullptr) {
            s = Status::no_memory();
        }
    }
    return s;
}

auto Schema::open_bucket(const Slice &name, Cursor *&c_out) -> Status
{
    if (m_pager->page_count() == 0) {
        return Status::invalid_argument();
    }

    m_cursor.find(name);
    auto s = m_cursor.status();

    Id root_id;
    if (!m_cursor.is_valid()) {
        return s.is_ok() ? kNoBucket : s;
    } else if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
        return corrupted_root_id();
    }
    if (auto *tree = construct_or_reference_tree(name, root_id)) {
        c_out = new (std::nothrow) CursorImpl(*tree);
    }
    if (c_out == nullptr) {
        s = Status::no_memory();
    }
    return s;
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
    for (auto *t = m_trees.next_entry; t != &m_trees; t = t->next_entry) {
        if (t->name == name) {
            return t->tree;
        }
    }
    return nullptr;
}

auto Schema::construct_or_reference_tree(const Slice &name, Id root_id) -> Tree *
{
    if (auto *already_open = find_open_tree(name)) {
        return already_open;
    }

    UniqueBuffer name_buf;
    if (!root_id.is_root()) {
        // If `name` is empty, a single byte will be allocated to store the '\0'.
        name_buf = UniqueBuffer::from_slice(name);
        if (name_buf.is_empty()) {
            return nullptr;
        }
    }

    auto *tree = Alloc::new_object<Tree>(*m_pager, *m_stat, m_scratch,
                                         root_id, std::move(name_buf));
    if (tree) {
        IntrusiveList::add_tail(tree->list_entry, m_trees);
    }
    return tree;
}

auto Schema::unpack_and_use(Cursor &c) -> std::pair<Tree &, CursorImpl &>
{
    auto *c_impl = static_cast<CursorImpl *>(c.handle());
    auto *tree = Tree::get_tree(*c_impl);
    use_tree(*tree);
    return {*tree, *c_impl};
}

auto Schema::use_tree(Tree &tree) -> void
{
    if (m_recent && m_recent != &tree) {
        m_recent->save_all_cursors();
    }
    m_recent = &tree;
}

auto Schema::drop_bucket(const Slice &name) -> Status
{
    map_trees([](auto &t) {
        t.tree->save_all_cursors();
        return true;
    });
    m_cursor.find(name);
    auto s = m_cursor.status();
    if (!m_cursor.is_valid()) {
        return s.is_ok() ? kNoBucket : s;
    }
    Id root_id;
    if (!decode_and_check_root_id(m_cursor.value(), root_id)) {
        return corrupted_root_id();
    }
    auto *drop = construct_or_reference_tree(name, root_id);
    if (drop == nullptr) {
        return Status::no_memory();
    }
    m_map.save_all_cursors();
    IntrusiveList::remove(drop->list_entry);
    drop->save_all_cursors();
    m_recent = nullptr;

    Tree::Reroot rr;
    s = m_map.destroy(*drop, rr);
    if (s.is_ok()) {
        s = m_map.erase(m_cursor, name);
    }
    if (s.is_ok() && rr.before != rr.after) {
        map_trees([rr](auto &t) {
            if (t.tree->m_root_id == rr.before) {
                t.tree->m_root_id = rr.after;
                return false;
            }
            return true;
        });
    }

    Alloc::delete_object(drop);
    return s;
}

auto Schema::vacuum() -> Status
{
    map_trees([](auto &t) {
        t.tree->save_all_cursors();
        return true;
    });
    m_map.save_all_cursors();
    m_recent = nullptr;
    return m_map.vacuum();
}

auto Schema::TEST_validate() const -> void
{
    map_trees([](auto &t) {
        t.tree->TEST_validate();
        return true;
    });
}

} // namespace calicodb
