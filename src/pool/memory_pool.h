#ifndef CALICO_POOL_IN_MEMORY_H
#define CALICO_POOL_IN_MEMORY_H

#include "calico/error.h"
#include "interface.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include <mutex>
#include <optional>
#include <spdlog/logger.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace calico {

class MemoryPool : public IBufferPool {
public:
    ~MemoryPool() override = default;
    MemoryPool(Size, bool, spdlog::sink_ptr);

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return 1.0;
    }

    [[nodiscard]] auto page_size() const -> Size override
    {
        return m_page_size;
    }

    [[nodiscard]] auto page_count() const -> Size override;
    auto save_header(page::FileHeader&) -> void override;
    auto load_header(const page::FileHeader&) -> void override;

    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto allocate() -> Result<page::Page> override;
    [[nodiscard]] auto acquire(PID, bool) -> Result<page::Page> override;
    [[nodiscard]] auto release(page::Page) -> Result<void> override;
    auto on_release(page::Page&) -> void override;

    auto flush() -> Result<void> override
    {
        return {};
    }

    [[nodiscard]] auto purge() -> Result<void> override;

private:
    struct UndoInfo {
        std::string before;
        PID id;
        Index offset {};
    };

    auto do_release(page::Page&) -> void;
    auto acquire_aux(PID, bool) -> page::Page;

    mutable std::mutex m_mutex;
    utils::ScratchManager m_scratch;
    std::vector<UndoInfo> m_stack;
    std::vector<Frame> m_frames;
    std::shared_ptr<spdlog::logger> m_logger;
    std::exception_ptr m_error;
    Size m_page_size {};
};

} // calico

#endif // CALICO_POOL_IN_MEMORY_H
