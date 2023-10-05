// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "schema.h"
#include "encoding.h"
#include "status_internal.h"

namespace calicodb
{

namespace
{

constexpr size_t kMinRootEntryLen = 2;
constexpr size_t kMaxRootEntryLen = 1 + kVarintMaxLength;
constexpr char kUniqueFlag = '\xFF';

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
    CursorImpl *const m_c;

public:
    explicit SchemaCursor(CursorImpl &c)
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

Schema::Schema(Pager &pager, const Status &status, Stats &stat)
    : m_status(&status),
      m_pager(&pager),
      m_scratch(pager.scratch()),
      m_stat(&stat),
      m_map(pager, stat, pager.scratch(), Id::root(), String(), true),
      m_internal(m_map),
      m_exposed(m_map),
      m_trees{"", &m_map, nullptr, nullptr},
      m_schema(Mem::new_object<SchemaCursor>(m_exposed))
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

    Mem::delete_object(m_schema);
    m_map.deactivate_cursors(nullptr);
}

auto Schema::corrupted_root_id() -> Status
{
    auto s = Status::corruption("bucket root page ID is corrupted");
    if (m_pager->mode() > Pager::kRead) {
        m_pager->set_status(s);
    }
    return s;
}

auto Schema::open_cursor(const Slice &name, const RootInfo &info, Cursor *&c_out) -> Status
{
    if (auto *tree = construct_or_reference_tree(name, info)) {
        c_out = new (std::nothrow) CursorImpl(*tree);
    }
    return c_out ? Status::ok() : Status::no_memory();
}

auto Schema::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    CALICODB_EXPECT_GT(m_pager->page_count(), 0);

    RootInfo info;
    auto [_, c] = unpack_cursor(m_internal);
    m_internal.find(name);
    auto s = m_internal.status();
    if (m_internal.is_valid()) {
        CALICODB_EXPECT_TRUE(s.is_ok());
        if (!decode_and_check_root_info(m_internal.value(), info)) {
            s = corrupted_root_id();
        } else if (options.error_if_exists) {
            s = Status::invalid_argument("bucket already exists");
        }
    } else if (s.is_ok()) {
        use_tree(&m_map);
        s = m_map.create(info.root_id);
        if (s.is_ok()) {
            char buf[kMaxRootEntryLen];
            info.unique = options.unique;
            const auto len = encode_root_info(info, buf);
            // On success, this call will leave c on the schema record of the bucket that we need to
            // open a cursor on below. It may invalidate `name`, so m_cursor.key() is used instead.
            // The 2 should be equivalent.
            s = m_map.put(*c, name, Slice(buf, len));
        }
    }

    if (s.is_ok() && c_out) {
        s = open_cursor(m_internal.key(), info, *c_out);
    }
    return s;
}

auto Schema::open_bucket(const Slice &name, Cursor *&c_out) -> Status
{
    if (m_pager->page_count() == 0) {
        return Status::invalid_argument();
    }
    m_internal.find(name);
    auto s = m_internal.status();

    RootInfo info;
    if (!m_internal.is_valid()) {
        return s.is_ok() ? no_bucket(name) : s;
    } else if (!decode_and_check_root_info(m_internal.value(), info)) {
        return corrupted_root_id();
    }
    return open_cursor(m_internal.key(), info, c_out);
}

auto Schema::decode_root_info(const Slice &data, RootInfo &info_out) -> bool
{
    if (data.size() < kMinRootEntryLen) {
        return false;
    }
    info_out.unique = data[0] == kUniqueFlag;
    if (!info_out.unique && data[0]) {
        return false; // Invalid "unique flag"
    }
    uint32_t num;
    if (decode_varint(data.data() + 1, data.data() + data.size(), num)) {
        info_out.root_id.value = num;
        return true;
    }
    return false;
}

auto Schema::decode_and_check_root_info(const Slice &data, RootInfo &info_out) -> bool
{
    return decode_root_info(data, info_out) &&
           info_out.root_id.value <= m_pager->page_count();
}

auto Schema::encode_root_info(const RootInfo &info, char *root_id_out) -> size_t
{
    root_id_out[0] = info.unique ? kUniqueFlag : '\0';
    const auto *end = encode_varint(root_id_out + 1, info.root_id.value);
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

auto Schema::construct_or_reference_tree(const Slice &name, const RootInfo &info) -> Tree *
{
    if (auto *already_open = find_open_tree(name)) {
        return already_open;
    }

    String name_str;
    if (!info.root_id.is_root() && append_strings(name_str, name)) {
        return nullptr;
    }

    auto *tree = Mem::new_object<Tree>(*m_pager, *m_stat, m_scratch, info.root_id,
                                       move(name_str), info.unique);
    if (tree) {
        IntrusiveList::add_tail(tree->list_entry, m_trees);
    }
    return tree;
}

auto Schema::unpack_cursor(Cursor &c) -> UnpackedCursor
{
    auto *c_impl = static_cast<TreeCursor *>(c.handle());
    return {Tree::get_tree(*c_impl), c_impl};
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

auto Schema::drop_bucket(const Slice &name) -> Status
{
    auto [_, c] = unpack_cursor(m_internal);

    m_internal.find(name);
    auto s = m_internal.status();
    if (!m_internal.is_valid()) {
        return s.is_ok() ? no_bucket(name) : s;
    }
    CALICODB_EXPECT_TRUE(s.is_ok());
    use_tree(&m_map);

    RootInfo info;
    if (!decode_and_check_root_info(m_internal.value(), info)) {
        return corrupted_root_id();
    }
    ObjectPtr<Tree> drop(construct_or_reference_tree(m_internal.key(), info));
    if (!drop) {
        return Status::no_memory();
    }
    IntrusiveList::remove(drop->list_entry);
    drop->deactivate_cursors(nullptr);
    m_map.deactivate_cursors(nullptr);

    Tree::Reroot rr;
    s = m_map.destroy(*drop, rr);
    if (s.is_ok()) {
        s = m_map.erase(*c);
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
        m_internal.seek_first();
        s = m_internal.status();
        while (m_internal.is_valid()) {
            if (!decode_and_check_root_info(m_internal.value(), info)) {
                return corrupted_root_id();
            }
            if (info.root_id == rr.before) {
                info.root_id = rr.after;
                char buf[kMaxRootEntryLen];
                const auto len = encode_root_info(info, buf);
                s = m_map.put(*c, m_internal.key(), Slice(buf, len));
                found = true;
                break;
            }
            m_internal.next();
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
