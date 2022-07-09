#ifndef CALICO_TREE_FREE_LIST_H
#define CALICO_TREE_FREE_LIST_H

#include <optional>
#include "utils/identifier.h"

namespace calico {

class FileHeader;
class IBufferPool;
class Page;

/**
 * Object that manages a stack of deleted pages on disk.
 */
class FreeList {
public:

    /**
     * Parameters for constructing a free list.
     */
    struct Parameters {
        IBufferPool *buffer_pool {}; ///< Reference to the underlying buffer pool.
        PID free_start {}; ///< Page ID of the page at the top of the free list stack.
        Size free_count {}; ///< Number of pages in the free list stack.
    };

    ~FreeList() = default;

    /**
     * Create a new free list.
     *
     * @param param Initial state and dependencies.
     */
    explicit FreeList(const Parameters &param);

    /**
     * Push a page onto the free list stack.
     *
     * @param page The page to push.
     */
    auto push(Page page) -> void;

    /**
     * Pop a page off of the free list stack.
     *
     * @return The page at the top of the free list stack, or std::nullopt if the free list is empty.
     */
    auto pop() -> std::optional<Page>;

    /**
     * Save state to a file header.
     *
     * @param header The header to save state to.
     */
    auto save_header(FileHeader &header) const -> void;

    /**
     * Load state from a file header.
     *
     * @param header The header to read state from.
     */
    auto load_header(const FileHeader &header) -> void;

    /**
     * Determine if the free list is empty.
     *
     * @return True if the free list is empty, false otherwise.
     */
    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_free_count == 0;
    }

private:
    IBufferPool *m_pool; ///< Reference to the underlying buffer pool.
    PID m_free_start {}; ///< Page ID of the page at the top of the free list stack.
    Size m_free_count {}; ///< Number of pages in the free list stack.
};

} // calico

#endif // CALICO_TREE_FREE_LIST_H
