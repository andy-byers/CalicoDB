#include "cell.h"
#include "utils/assert.h"
#include "utils/encoding.h"

namespace cub {

auto Cell::size() const -> Size
{
    constexpr auto KEY_AND_VALUE_SIZES{sizeof(uint16_t) + sizeof(uint32_t)};
    const bool is_internal{!m_left_child_id.is_null()};
    const bool has_overflow_id{!m_overflow_id.is_null()};
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

auto Cell::key() const -> RefBytes
{
    return m_key;
}

auto Cell::local_value() const -> RefBytes
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

auto Cell::write(MutBytes out) const -> void
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

    const auto local{local_value()};
    mem_copy(out, local, local.size());

    if (!m_overflow_id.is_null()) {
        CUB_EXPECT_LT(local.size(), m_value_size);
        out.advance(local.size());
        put_uint32(out, m_overflow_id.value);
    }
}

auto Cell::detach(Scratch scratch) -> void
{
    const auto key_size{m_key.size()};
    const auto local_value_size{m_local_value.size()};
    auto data = scratch.data();

    mem_copy(data, m_key, key_size);
    mem_copy(data.range(key_size), m_local_value, local_value_size);

    m_key = data;
    m_key.truncate(key_size);
    m_local_value = data.range(key_size);
    m_local_value.truncate(local_value_size);
    m_scratch = std::move(scratch);
}

namespace {

    inline constexpr auto compute_lvs(Size key_size, Size value_size, Size page_size) -> Size
    {
        CUB_EXPECT_GT(key_size, 0);
        CUB_EXPECT_GT(page_size, 0);
        CUB_EXPECT_TRUE(is_power_of_two(page_size));

        /* Cases:
         *              Byte 0     min_local(...)  max_local(...)
         *                   |                  |               |
         *                   |                  |               |
         *                   v                  v               v
         *     (1)  ::H::::: ::K::::::: ::V::::::::::::::::::::::
         *     (2)  ::H::::: ::K::::::::::::::::::::::: ::V::::::
         *     (3)  ::H::::: ::K::::::: ::V::::::**************************
         *     (4)  ::H::::: ::K::::::::::::::::::::::::::::::::: **V******
         *     (5)  ::H::::: ::K::::::::::::::::::::::: **V****************
         *
         * Everything shown as a '*' is stored on an overflow page.
         *
         * In (1) and (2), the entire value is stored in the cell. In (3), (4), and (5), part of V is
         * written to an overflow page. In (3), V is truncated such that the local payload is min_local(...)
         * in length. In (4) and (5), we try to truncate the local payload to min_local(...), but we never
         * remove any of the key.
        */
        if (const auto total{key_size + value_size}; total > max_local(page_size)) {
            const auto nonlocal_value_size{total - std::max(key_size, min_local(page_size))};
            return value_size - nonlocal_value_size;
        }
        return value_size;
    }

} // <anonymous>

CellBuilder::CellBuilder(Size page_size)
    : m_page_size{page_size} {}

auto CellBuilder::build() const -> Cell
{
    CUB_EXPECT_FALSE(m_key.is_empty());
    const auto local_value_size = compute_lvs(m_key.size(), m_value.size(), m_page_size);

    Cell cell;
    cell.m_key = m_key;
    cell.m_local_value = m_value.range(0, local_value_size);
    cell.m_value_size = m_value.size();
    return cell;
}

auto CellBuilder::set_key(RefBytes key) -> CellBuilder &
{
    m_key = key;
    return *this;
}

auto CellBuilder::set_value(RefBytes value) -> CellBuilder &
{
    m_value = value;
    return *this;
}

auto CellBuilder::overflow() const -> RefBytes
{
    const auto local_value_size = compute_lvs(m_key.size(), m_value.size(), m_page_size);
    if (local_value_size < m_value.size())
        return m_value.range(local_value_size);
    return {nullptr, 0};
}

CellReader::CellReader(PageType page_type, RefBytes page)
    : m_page{page}
    , m_page_type{page_type} {}

auto CellReader::read(Index offset) const -> Cell
{
    auto in = m_page.range(offset);
    Cell cell;

    if (m_page_type == PageType::INTERNAL_NODE) {
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

    const auto local_value_size = compute_lvs(key_size, cell.m_value_size, m_page.size());
    cell.m_local_value = in;
    cell.m_local_value.truncate(local_value_size);

    if (local_value_size < cell.m_value_size) {
        in.advance(local_value_size);
        cell.m_overflow_id.value = get_uint32(in);
    }
    return cell;
}

} // cub
