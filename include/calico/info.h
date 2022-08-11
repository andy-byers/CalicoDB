#ifndef CCO_INFO_H
#define CCO_INFO_H

#include "database.h"

namespace cco {

class Info {
public:
    explicit Info(Database::Impl &impl)
        : m_impl {&impl}
    {}

    virtual ~Info() = default;

    /**
     * Get the hit ratio for the buffer pool page cache.
     *
     * @return A page cache hit ratio in the view 0.0 to 1.0, inclusive.
     */
    [[nodiscard]] auto cache_hit_ratio() const -> double;

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

    /**
     * Determine if the database uses transactions.
     *
     * @return True if the database uses transactions, false otherwise.
     */
    [[nodiscard]] auto uses_xact() const -> bool;

    /**
     * Determine if the database exists only in-memory.
     *
     * @return True if the database is an in-memory database, false otherwise.
     */
    [[nodiscard]] auto is_temp() const -> bool;

private:
    Database::Impl *m_impl {}; ///< Pointer to the database this object was opened on.
};

} // cco

#endif // CCO_INFO_H
