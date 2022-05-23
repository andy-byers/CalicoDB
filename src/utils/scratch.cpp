#include "scratch.h"

namespace cub {

Scratch::Scratch(Index id, Bytes data, ScratchManager *source)
    : m_internal {Internal {data, source, id}} {}

Scratch::~Scratch()
{
    do_release();
}

auto Scratch::do_release() -> void
{
    if (m_internal.value.source)
        m_internal.value.source->on_scratch_release(*this);
    // TODO: NULL out m_internal, and do the same in Page.
}

auto Scratch::id() const -> Index
{
    CUB_EXPECT_NOT_NULL(m_internal.value.source);
    return m_internal.value.id;
}

auto Scratch::size() const -> Size
{
    CUB_EXPECT_NOT_NULL(m_internal.value.source);
    return m_internal.value.data.size();
}

auto Scratch::data() -> Bytes
{
    CUB_EXPECT_NOT_NULL(m_internal.value.source);
    return m_internal.value.data;
}

ScratchManager::ScratchManager(Size scratch_size)
    : m_scratch_size{scratch_size} {}

auto ScratchManager::get() -> Scratch
{
    if (m_available.empty())
        m_available.emplace_back(m_scratch_size, '\x00');

    const auto id = m_id_counter++;
    CUB_EXPECT_EQ(m_pinned.find(id), m_pinned.end());
    m_pinned.emplace(id, std::move(m_available.back()));
    m_available.pop_back();

    // NOTE: We need to look up the string to get the slice here. If the strings
    //       are small enough, the compiler will sometimes make a new string instead
    //       of moving the old one, and the slice will be invalidated. If we increase
    //       the string size, the compiler stops performing this optimization and the
    //       problem goes away.
    return {id, _b(m_pinned[id]), this};
}

auto ScratchManager::on_scratch_release(Scratch &scratch) -> void
{
    CUB_EXPECT_GE(scratch.id(), MIN_SCRATCH_ID);
    auto itr = m_pinned.find(scratch.id());
    CUB_EXPECT_NE(itr, m_pinned.end());
    m_available.emplace_back(std::move(itr->second));
    m_pinned.erase(scratch.id());
}

} // db