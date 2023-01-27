#ifndef CALICO_STATISTICS_H
#define CALICO_STATISTICS_H

#include "common.h"

namespace Calico {

class DatabaseImpl;

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
    friend class DatabaseImpl;
    explicit Statistics(DatabaseImpl &impl);

    const DatabaseImpl *m_impl {};
};

} // namespace Calico

#endif // CALICO_STATISTICS_H
