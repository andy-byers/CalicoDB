#ifndef CALICO_INFO_H
#define CALICO_INFO_H

#include "database.h"

namespace calico {

class Info final {
public:
    explicit Info(Core &core)
        : m_core {&core}
    {}

    ~Info() = default;

    /**
     * Get the number of records in the database.
     *
     * @return The number of records currently in the database.
     */
    [[nodiscard]] auto record_count() const -> Size;

    /**
     * Get the database size in pages.
     *
     * @return The database size in pages.
     */
    [[nodiscard]] auto page_count() const -> Size;

    /**
     * Get the database page size.
     *
     * @return The page size in bytes.
     */
    [[nodiscard]] auto page_size() const -> Size;

    /**
     * Get the maximum allowed key size.
     *
     * @return The maximal key length in characters.
     */
    [[nodiscard]] auto maximum_key_size() const -> Size;

private:
    Core *m_core {}; ///< Pointer to the database that this object was opened on.
};

} // namespace calico

#endif // CALICO_INFO_H
