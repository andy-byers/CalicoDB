#ifndef CUB_CURSOR_H
#define CUB_CURSOR_H

#include <memory>
#include "bytes.h"

namespace cub {

/**
 * A cursor object for finding records and traversing the database.
 *
 * This class is designed such that an open cursor will always be on a record, unless the database is empty.
 * Also, instances of this class always keep a node acquired. This can lead to problems if we try to write
 * to the database while a cursor is live.
 */
class Cursor {
public:
    virtual ~Cursor();

    /**
     * Check if the cursor is positioned on a record.
     *
     * @return True if the cursor is on a record, i.e. the database is not empty, false otherwise.
     */
    [[nodiscard]] auto has_record() const -> bool;

    /**
     * Check if the cursor is positioned on the record with the smallest key.
     *
     * @return True if the cursor is on the record with the smallest key, false otherwise.
     */
    [[nodiscard]] auto is_minimum() const -> bool;

    /**
     * Check if the cursor is positioned on the record with the largest key.
     *
     * @return True if the cursor is on the record with the largest key, false otherwise.
     */
    [[nodiscard]] auto is_maximum() const -> bool;

    /**
     * Get the current record's key.
     *
     * @return The current record's key.
     */
    [[nodiscard]] auto key() const -> BytesView;

    /**
     * Get the current record's value.
     *
     * @return The current record's value.
     */
    [[nodiscard]] auto value() const -> std::string;

    /**
     * Move the cursor back to its starting position.
     */
    auto reset() -> void;

    /**
     * Move the cursor to the next record.
     *
     * @return True if the cursor was incremented, false otherwise.
     */
    auto increment() -> bool;

    /**
     * Increment the cursor repeatedly.
     *
     * @param n Number of times to increment the cursor.
     */
    auto increment(Size n) -> Size;

    /**
     * Move the cursor to the previous record.
     *
     * @return True if the cursor was decremented, false otherwise.
     */
    auto decrement() -> bool;

    /**
     * Decrement the cursor repeatedly.
     *
     * @param n Number of times to decrement the cursor.
     */
    auto decrement(Size n) -> Size;

    /**
     * Seek to the first record with a key that is greater than or equal to the given key.
     *
     * @param key The key to seek to.
     * @return True if the exact key was found, false otherwise.
     */
    auto find(BytesView key) -> bool;

    /**
     * Seek to the record with the smallest key in the database.
     */
    auto find_minimum() -> void;

    /**
     * Seek to the record with the largest key in the database.
     */
    auto find_maximum() -> void;

    Cursor(Cursor &&) noexcept;
    Cursor &operator=(Cursor &&) noexcept;

private:
    friend class Database;
    Cursor();

    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // cub

#endif // CUB_CURSOR_H
