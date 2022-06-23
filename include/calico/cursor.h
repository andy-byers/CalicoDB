#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include "bytes.h"

namespace calico {

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
     * This method should only return false if the database is empty. The getter methods (key(), value(), and
     * record()) rely on the cursor being positioned over a valid record. If this method returns false and one
     * of them is called, the result is unspecified. If compiling with assertions, an assertion will be
     * triggered, otherwise, we'll likely crash or receive garbage results.
     *
     * @return True if the cursor is on a record, false otherwise.
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
     * @see has_record()
     * @return The current record's key.
     */
    [[nodiscard]] auto key() const -> BytesView;

    /**
     * Get the current record's value.
     *
     * @see has_record()
     * @return The current record's value.
     */
    [[nodiscard]] auto value() const -> std::string;

    /**
     * Get the current record.
     *
     * @see has_record()
     * @return The current record.
     */
    [[nodiscard]] auto record() const -> Record;

    /**
     * Move the cursor back to its starting position.
     *
     * In terms of the underlying B-tree, this method moves the cursor to the first record in the root node.
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

} // calico

#endif // CALICO_CURSOR_H
