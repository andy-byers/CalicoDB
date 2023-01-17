#ifndef CALICO_STATISTICS_H
#define CALICO_STATISTICS_H

#include "common.h"

namespace Calico {

class Core;

class Statistics final {
public:
    ~Statistics() = default;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto maximum_key_size() const -> Size;
    [[nodiscard]] auto cache_hit_ratio() const -> double;
    [[nodiscard]] auto registered_updates() const -> Size;
    [[nodiscard]] auto pager_throughput() const -> Size;
    [[nodiscard]] auto data_throughput() const -> Size;
    [[nodiscard]] auto wal_throughput() const -> Size;

private:
    friend class Core;

    explicit Statistics(Core &core)
        : m_core {&core}
    {}

    Core *m_core {};
};

} // namespace Calico

#endif // CALICO_STATISTICS_H
