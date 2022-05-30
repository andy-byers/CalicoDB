#ifndef CUB_POOL_IN_MEMORY_H
#define CUB_POOL_IN_MEMORY_H

#include <mutex>
#include <stack>
#include <stdexcept>
#include <string>
#include "common.h"
#include "interface.h"
#include "utils/identifier.h"
#include "utils/scratch.h"

namespace cub {

class InMemory: public IBufferPool {
public:
    explicit InMemory(Size);
    virtual ~InMemory() = default;

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return 1.0;
    }

    [[nodiscard]] auto page_count() const -> Size override
    {
        return m_data.size() / m_page_size;
    }

    [[nodiscard]] auto page_size() const -> Size override
    {
        return m_page_size;
    }

    [[nodiscard]] auto block_size() const -> Size override
    {
        return m_page_size;
    }

    [[nodiscard]] auto flushed_lsn() const -> LSN override
    {
        return LSN::null();
    }

    [[nodiscard]] auto can_commit() const -> bool override
    {
        return !m_stack.empty();
    }

    [[nodiscard]] auto allocate(PageType) -> Page override;
    [[nodiscard]] auto acquire(PID, bool) -> Page override;

    auto commit() -> void override
    {
        while (!m_stack.empty())
            m_stack.pop();
    }

    auto abort() -> void override {}
    auto flush() -> void override {}
    auto purge() -> void override {}
    auto recover() -> void override {}
    auto save_header(FileHeader&) -> void override {}
    auto on_page_release(Page&) -> void override;
    auto on_page_error() -> void override;

private:
    struct UndoInfo {
        std::string before;
        PID id;
        Index offset {};
    };

    auto propagate_page_error() -> void;

    mutable std::mutex m_mutex;
    std::stack<UndoInfo> m_stack;
    std::exception_ptr m_error;
    ScratchManager m_scratch;
    std::string m_data;
    Size m_page_size {};
};

} // cub

#endif // CUB_POOL_IN_MEMORY_H
