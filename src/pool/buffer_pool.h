#ifndef CALICO_POOL_BUFFER_POOL_H
#define CALICO_POOL_BUFFER_POOL_H

#include <list>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <spdlog/logger.h>
#include "page_cache.h"
#include "frame.h"
#include "interface.h"
#include "pager.h"
#include "utils/scratch.h"

namespace calico {

class ILogFile;
class IReadWriteFile;
class IWALReader;
class IWALWriter;
class Page;

class BufferPool: public IBufferPool {
public:
    struct Parameters {
        std::unique_ptr<IReadWriteFile> pool_file;
        std::unique_ptr<IWALReader> wal_reader;
        std::unique_ptr<IWALWriter> wal_writer;
        spdlog::sink_ptr log_sink;
        LSN flushed_lsn;
        Size frame_count {};
        Size page_count {};
        Size page_size {};
        bool use_transactions {};
    };

//    enum PageMode {
//        QUERY,
//        UPDATE,
//        TRACKED,
//    };

    explicit BufferPool(Parameters);
    ~BufferPool() override = default;

    [[nodiscard]] auto page_count() const -> Size override
    {
        return m_page_count;
    }

    [[nodiscard]] auto flushed_lsn() const -> LSN override
    {
        return m_flushed_lsn;
    }

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return m_cache.hit_ratio();
    }

    [[nodiscard]] auto page_size() const -> Size override
    {
        return m_pager.page_size();
    }

    [[nodiscard]] auto uses_transactions() const -> bool override
    {
        return m_uses_transactions;
    }

    [[nodiscard]] auto block_size() const -> Size override;
    [[nodiscard]] auto allocate(PageType) -> Page override;
    [[nodiscard]] auto acquire(PID, bool) -> Page override;
    [[nodiscard]] auto can_commit() const -> bool override;
    auto try_flush() -> bool override;
    auto try_flush_wal() -> bool override;
    auto commit() -> void override;
    auto abort() -> void override;
    auto purge() -> void override;
    auto recover() -> bool override;
    auto save_header(FileHeader&) -> void override;
    auto load_header(const FileHeader&) -> void override;
    auto on_page_release(Page&) -> void override;
    auto on_page_error() -> void override;

private:
    auto propagate_page_error() -> void;
    auto log_update(Page&) -> void;
    auto roll_forward() -> bool;
    auto roll_backward() -> void;
    auto fetch_page(PID, bool) -> Page;
    auto fetch_frame(PID) -> Frame;
    auto try_evict_frame() -> bool;

    mutable std::mutex m_mutex;
    std::unique_ptr<IWALReader> m_wal_reader;
    std::unique_ptr<IWALWriter> m_wal_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    std::exception_ptr m_error;
    ScratchManager m_scratch;
    PageCache m_cache;
    Pager m_pager;
    LSN m_flushed_lsn;
    LSN m_next_lsn;
    Size m_page_count {};
    Size m_dirty_count {};
    Size m_ref_sum {};
    bool m_uses_transactions {};
};

} // calico

#endif // CALICO_POOL_BUFFER_POOL_H
