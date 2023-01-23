#include "cell.h"

#include "node.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/layout.h"

namespace Calico {

static constexpr auto extra_size_internal() -> Size
{
    return sizeof(Id) + sizeof(std::uint16_t);
}

static constexpr auto extra_size_external(bool has_overflow) -> Size
{
    return sizeof(std::uint16_t) + sizeof(std::uint32_t) + sizeof(Id)*has_overflow;
}

LocalValueSizeGetter::LocalValueSizeGetter(Size page_size)
    : m_min_local {get_min_local(page_size)},
      m_max_local {get_max_local(page_size)}
{}

auto LocalValueSizeGetter::operator()(Size key_size, Size value_size) const -> Size
{
    return get_local_value_size(key_size, value_size, m_min_local, m_max_local);
}

auto Cell::make_external(Byte *buffer, const Slice &key, const Slice &value, const LocalValueSizeGetter &lvs_getter) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());
    const auto lvs = lvs_getter(key.size(), value.size());

    Cell cell;
    cell.m_data = buffer;
    cell.m_key_ptr = key.data();
    cell.m_val_ptr = value.data();

    Size offset {};

    put_u16(buffer + offset, static_cast<std::uint16_t>(key.size()));
    offset += sizeof(std::uint16_t);

    put_u32(buffer + offset, static_cast<std::uint32_t>(value.size()));
    offset += sizeof(std::uint32_t) + key.size() + lvs;

    if (lvs != value.size()) {
        CALICO_EXPECT_LT(lvs, value.size());
        offset += sizeof(Id);
    }
    cell.m_size = offset;
    return cell;
}

auto Cell::make_internal(Byte *buffer, const Slice &key) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());

    Cell cell;
    cell.m_data = buffer;
    cell.m_key_ptr = key.data();
    cell.m_val_ptr = nullptr;

    put_u16(buffer + sizeof(Id), static_cast<std::uint16_t>(key.size()));
    cell.m_size = extra_size_internal() + key.size();
    return cell;
}

auto Cell::read_external(Byte *data, const LocalValueSizeGetter &lvs_getter) -> Cell
{
    Cell cell;
    cell.m_data = data;

    const auto key_size = get_u16(data);
    const auto val_size = get_u32(data + sizeof(std::uint16_t));
    const auto lvs = lvs_getter(key_size, val_size);

    CALICO_EXPECT_LE(lvs, val_size);
    cell.m_size = extra_size_external(lvs != val_size);
    return cell;
}

auto Cell::read_internal(Byte *data) -> Cell
{
    Cell cell;
    cell.m_data = data;
    cell.m_size = extra_size_internal() + get_u16(data + sizeof(Id));
    return cell;
}

auto Cell::read_at(Slice in, Size page_size, bool is_external) -> Cell
{
    Cell cell;
    cell.m_page_size = page_size;

    if (!is_external) {
        cell.m_child_id.value = get_u64(in);
        in.advance(PAGE_ID_SIZE);
    }
    const auto key_size = get_u16(in);
    in.advance(sizeof(std::uint16_t));

    if (is_external) {
        cell.m_value_size = get_u32(in);
        in.advance(sizeof(std::uint32_t));
    }

    cell.m_key = in;
    cell.m_key.truncate(key_size);

    if (is_external) {
        in.advance(cell.m_key.size());
        const auto local_value_size = get_local_value_size(key_size, cell.m_value_size, page_size);
        cell.m_local_value = in;
        cell.m_local_value.truncate(local_value_size);

        if (local_value_size < cell.m_value_size) {
            in.advance(local_value_size);
            cell.m_overflow_id.value = get_u64(in);
        }
    }
    cell.m_is_external = is_external;
    return cell;
}

auto Cell::read_at(const Node &node, Size offset) -> Cell
{
    return read_at(node.page().view(offset), node.size(), node.is_external());
}

Cell::Cell(const Parameters &param)
    : m_key {param.key},
      m_local_value {param.local_value},
      m_overflow_id {param.overflow_id},
      m_value_size {param.value_size},
      m_page_size {param.page_size},
      m_data {param.buffer},
      m_is_external {param.is_external}
{}

auto Cell::copy() const -> Cell
{
    return *this;
}

auto Cell::size() const -> Size
{
    const auto is_internal = !m_is_external;
    const auto size_fields {sizeof(std::uint16_t) + sizeof(std::uint32_t) * m_is_external};
    const auto has_overflow_id = !m_overflow_id.is_null();
    return PAGE_ID_SIZE * static_cast<Size>(is_internal + has_overflow_id) +
           size_fields + m_key.size() + m_local_value.size();
}

auto Cell::child_id() const -> Id
{
    CALICO_EXPECT_FALSE(m_is_external);
//    return {get_u64(m_data)};
    return m_child_id;
}

auto Cell::set_child_id(Id id) -> void
{
    CALICO_EXPECT_FALSE(m_is_external);
    m_child_id = id;
//    put_u64(m_data, id.value);
}

auto Cell::set_overflow_id(Id id) -> void
{
    CALICO_EXPECT_TRUE(m_is_external);
    m_overflow_id = id;
}

auto Cell::key() const -> Slice
{
    return m_key;
}

auto Cell::local_value() const -> Slice
{
    CALICO_EXPECT_TRUE(m_is_external);
    return m_local_value;
}

auto Cell::value_size() const -> Size
{
    return m_value_size;
}

auto Cell::overflow_size() const -> Size
{
    return m_value_size - m_local_value.size();
}

auto Cell::overflow_id() const -> Id
{
    // Internal cells have a zero-length value field, so they cannot overflow.
    CALICO_EXPECT_TRUE(m_is_external);
    return m_overflow_id;
}

auto Cell::write(Span out) const -> void
{
    if (!m_is_external) {
        CALICO_EXPECT_FALSE(m_child_id.is_root());
        put_u64(out, m_child_id.value);
        out.advance(PAGE_ID_SIZE);
    }
    put_u16(out, static_cast<std::uint16_t>(m_key.size()));
    out.advance(sizeof(std::uint16_t));

    if (m_is_external) {
        put_u32(out, static_cast<std::uint32_t>(m_value_size));
        out.advance(sizeof(std::uint32_t));
    }

    mem_copy(out, m_key, m_key.size());
    out.advance(m_key.size());

    if (m_is_external) {
        const auto local = local_value();
        mem_copy(out, local, local.size());

        if (!m_overflow_id.is_null()) {
            CALICO_EXPECT_FALSE(m_child_id.is_root());
            CALICO_EXPECT_LT(local.size(), m_value_size);
            out.advance(local.size());
            put_u64(out, m_overflow_id.value);
        }
    }
}

auto Cell::detach(Span scratch, bool ensure_internal) -> void
{
    if (ensure_internal && m_is_external)
        set_is_external(false);

    write(scratch);
//    if (m_is_external) {
//        const auto min_local = get_min_local(m_page_size);
//        const auto max_local = get_max_local(m_page_size);
//        *this = read_at_(scratch.data(), min_local, max_local);
//    } else {
//        *this = read_at_(scratch.data());
//    }
    *this = read_at(scratch, m_page_size, m_is_external);
    m_is_attached = false;
}

auto Cell::set_is_external(bool is_external) -> void
{
    m_is_external = is_external;

    if (!m_is_external) {
        m_local_value.clear();
        m_value_size = 0;
        m_overflow_id = Id::null();
    }
}

auto make_external_cell(Byte *buffer, const Slice &key, const Slice &value, Size page_size) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());
    const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
    Cell::Parameters param;
    param.buffer = buffer;
    param.key = key;
    param.local_value = value;
    param.value_size = value.size();
    param.page_size = page_size;
    param.is_external = true;

    if (local_value_size != value.size()) {
        CALICO_EXPECT_LT(local_value_size, value.size());
        param.local_value.truncate(local_value_size);
        // Set to an arbitrary value.
        param.overflow_id = Id::root();
    }
    return Cell {param};
}

auto make_internal_cell(Byte *buffer, const Slice &key, Size page_size) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());
    Cell::Parameters param;
    param.buffer = buffer;
    param.key = key;
    param.page_size = page_size;
    param.is_external = false;
    return Cell {param};
}


auto make_external_cell(const Slice &key, const Slice &value, Size page_size) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());
    const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
    Cell::Parameters param;
    param.key = key;
    param.local_value = value;
    param.value_size = value.size();
    param.page_size = page_size;
    param.is_external = true;

    if (local_value_size != value.size()) {
        CALICO_EXPECT_LT(local_value_size, value.size());
        param.local_value.truncate(local_value_size);
        // Set to an arbitrary value.
        param.overflow_id = Id::root();
    }
    return Cell {param};
}

auto make_internal_cell(const Slice &key, Size page_size) -> Cell
{
    CALICO_EXPECT_FALSE(key.is_empty());
    Cell::Parameters param;
    param.key = key;
    param.page_size = page_size;
    param.is_external = false;
    return Cell {param};
}

} // namespace Calico
