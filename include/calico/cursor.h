#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include "status.h"
#include <memory>
#include <optional>

namespace calico {

class Node;
class NodePool;
class Internal;

class Cursor final {
public:
    Cursor() = default;

    ~Cursor() = default;

    /**
     * Determine if the cursor is valid.
     *
     * @return True if, and only if, the internal status object has an OK state, false otherwise.
     */
    [[nodiscard]] auto is_valid() const -> bool;

    /**
     * Get the cursor status.
     *
     * This method should be called to make sure that a cursor is valid before calling any method that
     * accesses the underlying record. A valid cursor will have an OK status. Any non-OK status indicates
     * an invalid cursor. For example, a "not found" status means that the record could not be located,
     * and the cursor is not safe to use.
     *
     * @return The cursor's internal status object.
     */
    [[nodiscard]] auto status() const -> Status;

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
        static constexpr Size LEFT {0};
        static constexpr Size CURRENT {1};
        static constexpr Size RIGHT {2};

        auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        std::uint64_t ids[3] {0, 1, 0}; ///< Page IDs of the current node and its two siblings.
        std::uint16_t cell_count {}; ///< Number of cells in the current node.
        std::uint16_t index {}; ///< Offset of the cursor in the current node.
    };

    friend class CursorInternal;

    mutable Status m_status {Status::not_found("not found")};
    NodePool *m_pool {}; ///< Reference to an object that provides node pages from the pager component.
    Internal *m_internal {}; ///< Reference to the tree internals.
    Position m_position; ///< Position of the cursor in the page.
};

} // namespace calico

#endif // CALICO_CURSOR_H
