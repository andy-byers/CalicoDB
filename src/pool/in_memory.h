#ifndef CALICO_POOL_IN_MEMORY_H
#define CALICO_POOL_IN_MEMORY_H

#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>
#include <spdlog/logger.h>
#include "interface.h"
#include "utils/identifier.h"
#include "utils/scratch.h"

namespace calico {

class InMemory: public IBufferPool {
public:
    InMemory(Size, bool, spdlog::sink_ptr);

    ~InMemory() override = default;

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return 1.0;
    }

    [[nodiscard]] auto page_count() const -> Size override
    {
        return m_frames.size();
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
        if (!m_uses_transactions)
            throw std::logic_error {"Transactions are not enabled"};

        return !m_stack.empty();
    }

    [[nodiscard]] auto uses_transactions() const -> bool override
    {
        return m_uses_transactions;
    }

    auto recover() -> bool override
    {
        return true;
    }

    auto try_flush() -> bool override
    {
        return true;
    }

    auto try_flush_wal() -> bool override
    {
        return true;
    }

    auto purge() -> void override {}

    [[nodiscard]] auto allocate(PageType) -> Page override;
    [[nodiscard]] auto acquire(PID, bool) -> Page override;
    auto commit() -> void override;
    auto abort() -> void override;
    auto save_header(FileHeader&) -> void override;
    auto load_header(const FileHeader&) -> void override;
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
    ScratchManager m_scratch;
    std::vector<UndoInfo> m_stack;
    std::vector<Frame> m_frames;
    std::shared_ptr<spdlog::logger> m_logger;
    std::exception_ptr m_error;
    Size m_page_size {};
    bool m_uses_transactions {};
};

} // calico

#endif // CALICO_POOL_IN_MEMORY_H
