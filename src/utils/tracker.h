#ifndef CCO_UTILS_TRACKER_H
#define CCO_UTILS_TRACKER_H

#include "identifier.h"
#include "scratch.h"
#include "page/update.h"

namespace cco {

class Tracker final {
public:
    ~Tracker() = default;

    explicit Tracker(Size page_size):
          m_scratch {page_size} {}

    [[nodiscard]] auto collect(Page&, LSN) -> PageUpdate;
    auto reset() -> void;
    auto discard(Page&) -> void;
    auto track(Page&) -> void;

private:
    std::unordered_map<PID, UpdateManager, PID::Hasher> m_registry;
    ScratchManager m_scratch;
};

} // cco

#endif // CCO_UTILS_TRACKER_H
