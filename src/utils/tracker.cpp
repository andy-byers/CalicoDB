#include "tracker.h"
#include "page/page.h"

namespace cco {

using namespace page;
using namespace utils;

auto Tracker::reset() -> void
{
    CCO_EXPECT_TRUE(m_registry.empty());
    m_scratch.reset();
}

auto Tracker::track(Page &page) -> void
{
    CCO_EXPECT_EQ(m_registry.find(page.id()), end(m_registry));
    auto [itr, was_posted] = m_registry.emplace(page.id(), UpdateManager {page.view(0), m_scratch.get().data()});
    CCO_EXPECT_TRUE(was_posted);
    page.set_manager(itr->second);
}

auto Tracker::discard(Page &page) -> void
{
    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    m_registry.erase(itr);
    page.clear_manager();
}

auto Tracker::collect(Page &page, LSN lsn) -> PageUpdate
{
    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    auto &manager = itr->second;

    PageUpdate update;
    if (manager.has_updates()) {
        const auto previous_lsn = page.lsn();
        page.set_lsn(lsn);

        auto changes = manager.collect();
        CCO_EXPECT_FALSE(changes.empty());
        update.page_id = page.id();
        update.previous_lsn = previous_lsn;
        update.lsn = lsn;
        update.changes = std::move(changes);
    }
    m_registry.erase(itr);
    page.clear_manager();
    return update;
}

} // cco