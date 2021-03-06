#ifndef CCO_POOL_IN_MEMORY_H
#define CCO_POOL_IN_MEMORY_H

#include "calico/status.h"
#include "interface.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include "utils/tracker.h"
#include <mutex>
#include <optional>
#include <spdlog/logger.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace cco {

class MemoryPool : public IBufferPool {
public:
    ~MemoryPool() override = default;
    MemoryPool(Size, bool);

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return 1.0;
    }

    [[nodiscard]] auto uses_xact() const -> bool override
    {
        return m_uses_xact;
    }

    [[nodiscard]] auto page_size() const -> Size override
    {
        return m_page_size;
    }

    [[nodiscard]] auto flush() -> Result<void> override
    {
        return {};
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto recover() -> Result<void> override
    {
        return {};
    }

    [[nodiscard]] auto can_commit() const -> bool override
    {
        return !m_stack.empty();
    }

    auto clear_error() -> void override {}

    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto allocate() -> Result<Page> override;
    [[nodiscard]] auto acquire(PID, bool) -> Result<Page> override;
    [[nodiscard]] auto release(Page) -> Result<void> override;
    [[nodiscard]] auto fetch(PID id, bool is_writable) -> Result<Page> override;
    [[nodiscard]] auto commit() -> Result<void> override;
    [[nodiscard]] auto abort() -> Result<void> override;
    auto on_release(Page&) -> void override;
    auto save_header(FileHeaderWriter&) -> void override;
    auto load_header(const FileHeaderReader&) -> void override;

private:
    struct UndoInfo {
        std::string before;
        PID id;
        Index offset {};
    };

    auto do_release(Page&) -> void;

    mutable std::mutex m_mutex;
    Tracker m_tracker;
    ScratchManager m_scratch;
    std::vector<UndoInfo> m_stack;
    std::vector<Frame> m_frames;
    Size m_page_size {};
    bool m_uses_xact {};
};

} // cco

#endif // CCO_POOL_IN_MEMORY_H
