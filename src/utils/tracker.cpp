#include "tracker.h"
#include "page/page.h"

namespace cco {

auto Tracker::track(Page &page) -> void
{
    CCO_EXPECT_EQ(m_registry.find(page.id()), end(m_registry));
    auto [itr, was_posted] = m_registry.emplace(page.id(), ChangeManager {page.view(0), m_scratch.get(), m_scratch.get()});
    CCO_EXPECT_TRUE(was_posted);
    page.set_manager(itr->second);
}

auto Tracker::discard(Page &page) -> void
{
    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    itr->second.release_scratches(m_scratch);
    m_registry.erase(itr);
    page.clear_manager();
}

auto Tracker::collect(Page &page, SequenceNumber lsn) -> PageUpdate
{
    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    auto &manager = itr->second;

    PageUpdate update;
    if (manager.has_changes()) {
        const auto previous_lsn = page.lsn();
        page.set_lsn(lsn);

        auto changes = manager.collect_changes();
        CCO_EXPECT_FALSE(changes.empty());
        update.page_id = page.id();
        update.last_lsn = previous_lsn;
        update.page_lsn = lsn;
        update.changes = std::move(changes);
    }
    // Note that we don't release scratch memory here. We need to call cleanup() when we're done with the
    // memory used by the before and after page snapshots.
    page.clear_manager();

    std::lock_guard lock {m_mutex};
    m_processing.emplace(itr->first, std::move(itr->second));
    m_registry.erase(itr);
    return update;
}

auto Tracker::cleanup(PageId id) -> void
{
    std::lock_guard lock {m_mutex};
    auto itr = m_processing.find(id);
    CCO_EXPECT_NE(itr, end(m_processing));
    itr->second.release_scratches(m_scratch);
    m_processing.erase(itr);
}

} // namespace cco