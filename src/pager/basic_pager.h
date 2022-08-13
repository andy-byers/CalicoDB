#ifndef CALICO_POOL_BUFFER_POOL_H
#define CALICO_POOL_BUFFER_POOL_H

#include "pager.h"
#include "registry.h"
#include "utils/scratch.h"
#include <list>
#include <mutex>
#include <spdlog/logger.h>
#include <unordered_map>

namespace calico {

class Storage;
class Framer;

class BasicPager : public Pager {
public:
    struct Parameters {
        std::string root;
        Storage &storage;
        WriteAheadLog &wal;
        spdlog::sink_ptr log_sink;
        Size frame_count {};
        Size page_size {};
    };

    ~BasicPager() override = default;

    [[nodiscard]] static auto open(const Parameters &) -> Result<std::unique_ptr<BasicPager>>;
    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto flushed_lsn() const -> SequenceId override;
    [[nodiscard]] auto allocate() -> Result<Page> override;
    [[nodiscard]] auto acquire(PageId, bool) -> Result<Page> override;
    [[nodiscard]] auto release(Page) -> Status override;
    [[nodiscard]] auto flush() -> Status override;
    auto save_state(FileHeader &) -> void override;
    auto load_state(const FileHeader &) -> void override;

    [[nodiscard]]
    auto status() const -> Status override
    {
        return m_status;
    }

    auto reset_status() -> void override
    {
        m_status = Status::ok();
    }

private:
    explicit BasicPager(const Parameters &);
    [[nodiscard]] auto pin_frame(PageId) -> Status;
    [[nodiscard]] auto try_make_available() -> Result<bool>;
    auto register_page(Page &, PageRegistry::Entry &) -> void;

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

#endif // CALICO_POOL_BUFFER_POOL_H
