#ifndef CCO_POOL_BUFFER_POOL_H
#define CCO_POOL_BUFFER_POOL_H

#include "frame.h"
#include "interface.h"
#include "page_cache.h"
#include "utils/scratch.h"
#include <list>
#include <mutex>
#include <spdlog/logger.h>
#include <unordered_map>

namespace cco {

class Storage;
class IFile;
class Pager;

class BufferPool : public IBufferPool {
public:
    struct Parameters {
        Storage &storage;
        WriteAheadLog *wal {};
        spdlog::sink_ptr log_sink;
        SequenceNumber flushed_lsn;
        Size frame_count {};
        Size page_count {};
        Size page_size {};
        bool use_xact {};
    };

    ~BufferPool() override;

    [[nodiscard]]
    static auto open(const Parameters &) -> Result<std::unique_ptr<BufferPool>>;

    [[nodiscard]]
    auto status() const -> Status override
    {
        return m_status;
    }

    auto reset_status() -> void override
    {
        m_status = Status::ok();
    }

    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto flushed_lsn() const -> SequenceNumber override;
    [[nodiscard]] auto allocate() -> Result<Page> override;
    [[nodiscard]] auto acquire(PageId, bool) -> Result<Page> override;
    [[nodiscard]] auto release(Page) -> Status override;
    [[nodiscard]] auto flush() -> Status override;
    auto update_page(Page &, Size, Index) -> void override;
    auto save_state(FileHeader &) -> void override;
    auto load_state(const FileHeader &) -> void override;

private:
    explicit BufferPool(const Parameters &);
    [[nodiscard]] auto pin_frame(PageId) -> Status;
    [[nodiscard]] auto try_evict_frame() -> Result<bool>;
    [[nodiscard]] auto do_release(Page &) -> Status;

    mutable std::mutex m_mutex;
    std::unique_ptr<Pager> m_pager;
    std::shared_ptr<spdlog::logger> m_logger;
    RollingScratchManager m_scratch;
    WriteAheadLog *m_wal {};
    DirtyList m_dirty;
    PageCache m_cache;
    Status m_status {Status::ok()};
    Size m_dirty_count {};
    Size m_ref_sum {};
};

} // namespace cco

#endif // CCO_POOL_BUFFER_POOL_H
