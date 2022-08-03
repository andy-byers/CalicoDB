#ifndef CCO_UTILS_TRACKER_H
#define CCO_UTILS_TRACKER_H

#include "identifier.h"
#include "page/update.h"
#include "scratch.h"

namespace cco {

class Tracker final {
public:
    ~Tracker() = default;

    explicit Tracker(Size page_size)
        : m_scratch {page_size}
    {}

    [[nodiscard]] auto collect(Page &, SequenceNumber) -> PageUpdate;
    auto cleanup(PageId) -> void;
    auto discard(Page &) -> void;
    auto track(Page &) -> void;

private:
    std::unordered_map<PageId, ChangeManager, PageId::Hash> m_registry;
    std::unordered_map<PageId, ChangeManager, PageId::Hash> m_processing;
    ManualScratchManager m_scratch;
};

} // namespace cco

#endif // CCO_UTILS_TRACKER_H
