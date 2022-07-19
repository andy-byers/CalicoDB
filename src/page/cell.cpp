#include "cell.h"

#include "node.h"
#include "utils/expect.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cco::page {

using namespace utils;

auto Cell::read_at(BytesView in, Size page_size, bool is_external) -> Cell
{
    Cell cell;
    cell.m_page_size = page_size;

    if (!is_external) {
        cell.m_left_child_id.value = utils::get_u32(in);
        in.advance(PAGE_ID_SIZE);
    }
    const auto key_size = utils::get_u16(in);
    in.advance(sizeof(std::uint16_t));

    if (is_external) {
        cell.m_value_size = utils::get_u32(in);
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
            cell.m_overflow_id.value = utils::get_u32(in);
        }
    }
    cell.m_is_external = is_external;
    return cell;
}

auto Cell::read_at(const Node &node, Index offset) -> Cell
{
    return read_at(node.page().view(offset), node.size(), node.is_external());
}

Cell::Cell(const Parameters &param)
    : m_key {param.key},
      m_local_value {param.local_value},
      m_overflow_id {param.overflow_id},
      m_value_size {param.value_size},
      m_page_size {param.page_size},
      m_is_external {param.is_external} {}

auto Cell::copy() const -> Cell
{
    return Cell {{
        m_key,
        m_local_value,
        m_overflow_id,
        m_value_size,
        m_page_size,
        m_is_external,
    }};
}

auto Cell::size() const -> Size
{
    const auto is_internal = !m_is_external;
    const auto size_fields {sizeof(std::uint16_t) + sizeof(std::uint32_t)*m_is_external};
    const auto has_overflow_id = !m_overflow_id.is_null();
    return PAGE_ID_SIZE * static_cast<Size>(is_internal + has_overflow_id) +
           size_fields + m_key.size() + m_local_value.size();
}

auto Cell::left_child_id() const -> PID
{
    CCO_EXPECT_FALSE(m_is_external);
    return m_left_child_id;
}

auto Cell::set_left_child_id(PID left_child_id) -> void
{
    CCO_EXPECT_FALSE(m_is_external);
    m_left_child_id = left_child_id;
}

auto Cell::set_overflow_id(PID id) -> void
{
    CCO_EXPECT_TRUE(m_is_external);
    m_overflow_id = id;
}

auto Cell::key() const -> BytesView
{
    return m_key;
}

auto Cell::local_value() const -> BytesView
{
//    CCO_EXPECT_TRUE(m_is_external);
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

auto Cell::overflow_id() const -> PID
{
//    CCO_EXPECT_TRUE(m_is_external);
    return m_overflow_id;
}

auto Cell::write(Bytes out) const -> void
{
    if (!m_is_external) {
        CCO_EXPECT_FALSE(m_left_child_id.is_root());
        utils::put_u32(out, m_left_child_id.value);
        out.advance(PAGE_ID_SIZE);
    }
    utils::put_u16(out, static_cast<std::uint16_t>(m_key.size()));
    out.advance(sizeof(std::uint16_t));

    if (m_is_external) {
        utils::put_u32(out, static_cast<std::uint32_t>(m_value_size));
        out.advance(sizeof(std::uint32_t));
    }

    mem_copy(out, m_key, m_key.size());
    out.advance(m_key.size());

    if (m_is_external) {
        const auto local = local_value();
        mem_copy(out, local, local.size());

        if (!m_overflow_id.is_null()) {
            CCO_EXPECT_FALSE(m_left_child_id.is_root());
            CCO_EXPECT_LT(local.size(), m_value_size);
            out.advance(local.size());
            utils::put_u32(out, m_overflow_id.value);
        }
    }
}

auto Cell::detach(Scratch scratch, bool ensure_internal) -> void
{
    if (ensure_internal && m_is_external)
        set_is_external(false);

    auto data = scratch.data();
    write(data);
    *this = read_at(data, m_page_size, m_is_external);
    m_scratch = scratch;
}

auto Cell::set_is_external(bool is_external) -> void
{
    m_is_external = is_external;

    if (!m_is_external) {
        m_local_value.clear();
        m_value_size = 0;
        m_overflow_id = PID::null();
    }
}

auto make_external_cell(BytesView key, BytesView value, Size page_size) -> Cell
{
    CCO_EXPECT_FALSE(key.is_empty());
    const auto local_value_size = get_local_value_size(key.size(), value.size(), page_size);
    Cell::Parameters param;
    param.key = key;
    param.local_value = value;
    param.value_size = value.size();
    param.page_size = page_size;
    param.is_external = true;

    if (local_value_size != value.size()) {
        CCO_EXPECT_LT(local_value_size, value.size());
        param.local_value.truncate(local_value_size);
        // Set to an arbitrary value.
        param.overflow_id = PID::root();
    }
    return Cell {param};
}

auto make_internal_cell(BytesView key, Size page_size) -> Cell
{
    CCO_EXPECT_FALSE(key.is_empty());
    Cell::Parameters param;
    param.key = key;
    param.page_size = page_size;
    param.is_external = false;
    return Cell {param};
}

} // cco::page
