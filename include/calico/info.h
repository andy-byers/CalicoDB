#ifndef CALICO_INFO_H
#define CALICO_INFO_H

#include "database.h"

namespace calico {

class Info final {
public:
    ~Info() = default;
    [[nodiscard]] auto record_count() const -> Size;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto maximum_key_size() const -> Size;

private:
    friend class Core;

    explicit Info(Core &core)
        : m_core {&core}
    {}

    Core *m_core {};
};

} // namespace calico

#endif // CALICO_INFO_H
