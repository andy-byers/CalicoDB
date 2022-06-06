#include "cell.h"
#include "node.h"
#include "utils/assert.h"
#include "utils/encoding.h"
#include "utils/layout.h"

namespace cub {

auto Cell::read_at(const Node &node, Index offset) -> Cell
{
    auto in = node.page().range(offset);
    Cell cell;

    if (!node.is_external()) {
        cell.m_left_child_id.value = get_uint32(in);
        in.advance(PAGE_ID_SIZE);
    }
    const auto key_size = get_uint16(in);
    in.advance(sizeof(uint16_t));

    cell.m_value_size = get_uint32(in);
    in.advance(sizeof(uint32_t));

    cell.m_key = in;
    cell.m_key.truncate(key_size);
    in.advance(cell.m_key.size());

    const auto local_value_size = get_local_value_size(key_size, cell.m_value_size, node.size());
    cell.m_local_value = in;
    cell.m_local_value.truncate(local_value_size);

    if (local_value_size < cell.m_value_size) {
        in.advance(local_value_size);
        cell.m_overflow_id.value = get_uint32(in);
    }
    return cell;
}

Cell::Cell(const Parameters &param)
    : m_key {param.key}
    , m_local_value {param.local_value}
    , m_overflow_id {param.overflow_id}
    , m_value_size {param.value_size} {}

auto Cell::size() const -> Size
{
    constexpr auto KEY_AND_VALUE_SIZES = sizeof(uint16_t) + sizeof(uint32_t);
    const auto is_internal = !m_left_child_id.is_null();
    const auto has_overflow_id = !m_overflow_id.is_null();
    return PAGE_ID_SIZE * (is_internal + has_overflow_id) +
           KEY_AND_VALUE_SIZES + m_key.size() + m_local_value.size();
}

auto Cell::left_child_id() const -> PID
{
    return m_left_child_id;
}

auto Cell::set_left_child_id(PID left_child_id) -> void
{
    m_left_child_id = left_child_id;
}

auto Cell::set_overflow_id(PID id) -> void
{
    m_overflow_id = id;
}

auto Cell::key() const -> BytesView
{
    return m_key;
}

auto Cell::local_value() const -> BytesView
{
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
    return m_overflow_id;
}

auto Cell::write(Bytes out) const -> void
{
    if (!m_left_child_id.is_null()) {
        put_uint32(out, m_left_child_id.value);
        out.advance(PAGE_ID_SIZE);
    }
    put_uint16(out, static_cast<uint16_t>(m_key.size()));
    out.advance(sizeof(uint16_t));

    put_uint32(out, static_cast<uint32_t>(m_value_size));
    out.advance(sizeof(uint32_t));

    mem_copy(out, m_key, m_key.size());
    out.advance(m_key.size());

    const auto local = local_value();
    mem_copy(out, local, local.size());

    if (!m_overflow_id.is_null()) {
        CUB_EXPECT_LT(local.size(), m_value_size);
        out.advance(local.size());
        put_uint32(out, m_overflow_id.value);
    }
}

auto Cell::detach(Scratch scratch) -> void
{
    const auto key_size = m_key.size();
    const auto local_value_size = m_local_value.size();
    auto data = scratch.data();

    mem_copy(data, m_key, key_size);
    mem_copy(data.range(key_size), m_local_value, local_value_size);

    m_key = data;
    m_key.truncate(key_size);
    m_local_value = data.range(key_size);
    m_local_value.truncate(local_value_size);
    m_scratch = std::move(scratch);
}

} // cub
