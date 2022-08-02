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

class IDirectory;
class IFile;
class IWALManager;
class Pager;

class BufferPool : public IBufferPool {
public:
    struct Parameters {
        IDirectory &directory;
        spdlog::sink_ptr log_sink;
        SequenceNumber flushed_lsn;
        Size frame_count {};
        Size page_count {};
        Size page_size {};
        int permissions {};
        bool use_xact {};
    };

    ~BufferPool() override = default;

    [[nodiscard]] static auto open(const Parameters &) -> Result<std::unique_ptr<IBufferPool>>;

    [[nodiscard]] auto uses_xact() const -> bool override
    {
        return m_uses_xact;
    }

    [[nodiscard]] auto page_count() const -> Size override
    {
        return m_page_count;
    }

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return m_cache.hit_ratio();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_status;
    }

    auto clear_error() -> void override
    {
        m_status = Status::ok();
    }
    [[nodiscard]] auto flushed_lsn() const -> SequenceNumber override;
    [[nodiscard]] auto page_size() const -> Size override;
    [[nodiscard]] auto can_commit() const -> bool override;
    [[nodiscard]] auto allocate() -> Result<Page> override;
    [[nodiscard]] auto fetch(PageId, bool) -> Result<Page> override;
    [[nodiscard]] auto acquire(PageId, bool) -> Result<Page> override;
    [[nodiscard]] auto release(Page) -> Result<void> override;
    [[nodiscard]] auto recover() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto commit() -> Result<void> override;
    [[nodiscard]] auto abort() -> Result<void> override;
    auto on_release(Page &) -> void override;
    auto save_header(FileHeaderWriter &) -> void override;
    auto load_header(const FileHeaderReader &) -> void override;

private:
    explicit BufferPool(const Parameters &);
    [[nodiscard]] auto pin_frame(PageId) -> Result<void>;
    [[nodiscard]] auto try_evict_frame() -> Result<bool>;
    [[nodiscard]] auto do_release(Page &) -> Result<void>;
    [[nodiscard]] auto has_updates() const -> bool;
    mutable std::mutex m_mutex;
    std::unique_ptr<Pager> m_pager;
    std::unique_ptr<IWALManager> m_wal;
    std::shared_ptr<spdlog::logger> m_logger;
//    std::vector<Size> m_ref_counts;                                         // TODO: Keeping track of ref counts differently.
    ScratchManager m_scratch;
    PageCache m_cache;
    Status m_status {Status::ok()};
    Size m_page_count {};
    Size m_dirty_count {};
    Size m_ref_sum {};
    bool m_uses_xact {};
};

} // namespace cco

#endif // CCO_POOL_BUFFER_POOL_H
