#ifndef CCO_CURSOR_H
#define CCO_CURSOR_H

#include "status.h"
#include <memory>
#include <optional>

namespace cco {

class Node;
class NodePool;
class Internal;

class Cursor final {
public:
    ~Cursor() = default;

    /**
     * Check if the cursor is on a valid record.
     *
     * This method should be called to make sure that a cursor is valid before calling any method that
     * accesses the underlying record.
     *
     * @return True if the cursor is on a valid record, false otherwise.
     */
    [[nodiscard]] auto is_valid() const -> bool;

    [[nodiscard]] auto status() const -> Status;

    /**
     * Check if the cursor is on the record with the largest key (the rightmost record).
     *
     * @return True if the cursor is on the rightmost record, false otherwise.
     */
    [[nodiscard]] auto is_maximum() const -> bool;

    /**
     * Check if the cursor is on the record with the smallest key (the leftmost record).
     *
     * @return True if the cursor is on the leftmost record, false otherwise.
     */
    [[nodiscard]] auto is_minimum() const -> bool;

    /**
     * Get the key of the current record.
     *
     * This method produces undefined behavior if the cursor is not valid.
     *
     * @return The key of the record that the cursor is positioned on.
     */
    [[nodiscard]] auto key() const -> BytesView;

    /**
     * Get the value of the current record.
     *
     * This method produces undefined behavior if the cursor is not valid.
     *
     * @return The value of the record that the cursor is positioned on.
     */
    [[nodiscard]] auto value() const -> std::string;

    /**
     * Get the current record.
     *
     * This method produces undefined behavior if the cursor is not valid.
     *
     * @return The record that the cursor is positioned on.
     */
    [[nodiscard]] auto record() const -> Record;

    /**
     * Move the cursor to the right by one position.
     *
     * The cursor will be left on the record with a key that is immediately larger than the current key
     * in the database ordering, or it will be invalidated if already on the rightmost record.
     *
     * @see is_valid()
     * @return True if the cursor changed positions, false otherwise. Note that if this method returns
     *         true, it does not mean that the cursor is valid. Essentially, any cursor that is valid
     *         can be incremented at least once.
     */
    auto increment() -> bool;

    /**
     * Move the cursor to the left by one position.
     *
     * The cursor will be left on the record with a key that is immediately smaller than the current key
     * in the database ordering, or it will be invalidated if already on the leftmost record.
     *
     * @see is_valid()
     * @return True if the cursor changed positions, false otherwise. Note that if this method returns
     *         true, it does not mean that the cursor is valid. Essentially, any cursor that is valid
     *         can be decremented at least once.
     */
    auto decrement() -> bool;

    /**
     * Move a valid cursor to the right by one position.
     *
     * @see increment()
     * @see is_valid()
     * @return This cursor.
     */
    auto operator++() -> Cursor&;

    /**
     * Move a valid cursor to the right by one position.
     *
     * @see increment()
     * @see is_valid()
     * @return This cursor.
     */
    auto operator++(int) -> Cursor;

    /**
     * Move a valid cursor to the left by one position.
     *
     * @see decrement()
     * @see is_valid()
     * @return This cursor.
     */
    auto operator--() -> Cursor&;

    /**
     * Move a valid cursor to the left by one position.
     *
     * @see decrement()
     * @see is_valid()
     * @return This cursor.
     */
    auto operator--(int) -> Cursor;

    /**
     * Determine if two cursors are on the same record.
     *
     * @return True if the cursors are on the same record, false otherwise.
     */
    auto operator==(const Cursor&) const -> bool;

    /**
     * Determine if two cursors are on different records.
     *
     * @return True if the cursors are on different records, false otherwise.
     */
    auto operator!=(const Cursor&) const -> bool;

private:

    /**
     * Representation of a cursor position in the tree.
     */
    struct Position {
        static constexpr Index LEFT {0};
        static constexpr Index CURRENT {1};
        static constexpr Index RIGHT {2};

        auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        std::uint64_t ids[3] {0, 1, 0}; ///< Page IDs of the current node and its two siblings.
        std::uint16_t cell_count {}; ///< Number of cells in the current node.
        std::uint16_t index {}; ///< Offset of the cursor in the current node.
    };

    friend class CursorInternal;
    Cursor() = default;

    mutable Status m_status {Status::not_found()};
    NodePool *m_pool {}; ///< Reference to an object that provides nodes from the buffer pool.
    Internal *m_internal {}; ///< Reference to the page internals.
    Position m_position; ///< Position of the cursor in the page.
};

} // cco

#endif // CCO_CURSOR_H
