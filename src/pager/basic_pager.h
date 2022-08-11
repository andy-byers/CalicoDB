#ifndef CCO_POOL_BUFFER_POOL_H
#define CCO_POOL_BUFFER_POOL_H

#include "frame.h"
#include "pager.h"
#include "registry.h"
#include "utils/scratch.h"
#include <list>
#include <mutex>
#include <spdlog/logger.h>
#include <unordered_map>

namespace cco {

class Storage;
class IFile;
class Framer;

class BasicPager : public Pager {
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

    ~BasicPager() override;

    [[nodiscard]]
    static auto open(const Parameters &) -> Result<std::unique_ptr<BasicPager>>;

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
    auto save_state(FileHeader &) -> void override;
    auto load_state(const FileHeader &) -> void override;
private:

    explicit BasicPager(const Parameters &);
    [[nodiscard]] auto pin_frame(PageId) -> Status;
    [[nodiscard]] auto try_make_available() -> Result<bool>;
    auto register_page(Page &) -> void;

    mutable std::mutex m_mutex;
    std::unique_ptr<Framer> m_framer;
    std::shared_ptr<spdlog::logger> m_logger;
    RollingScratchManager m_scratch;
    WriteAheadLog *m_wal {};
    PageList m_dirty;
    PageRegistry m_registry;
    Status m_status {Status::ok()};
    Size m_dirty_count {};
    Size m_ref_sum {};
};

} // namespace cco

#endif // CCO_POOL_BUFFER_POOL_H
