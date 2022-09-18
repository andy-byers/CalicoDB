#ifndef CALICO_PAGER_PAGER_H
#define CALICO_PAGER_PAGER_H

#include "pager.h"
#include "registry.h"
#include "spdlog/logger.h"
#include "utils/scratch.h"
#include "wal/helpers.h"
#include <list>
#include <mutex>
#include <unordered_set>

namespace calico {

class Storage;
class Framer;

class BasicPager : public Pager {
public:
    struct Parameters {
        std::string prefix;
        Storage &storage; // TODO: Make these pointers...
        LogScratchManager *scratch {};
        std::unordered_set<PageId, PageId::Hash> *images {};
        WriteAheadLog &wal;
        Status &status;
        bool &has_xact;
        spdlog::sink_ptr log_sink;
        Size frame_count {};
        Size page_size {};
    };

    ~BasicPager() override = default;

    [[nodiscard]] static auto open(const Parameters &) -> Result<std::unique_ptr<BasicPager>>;
    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto page_size() const -> Size override;
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
        return *m_status;
    }

private:
    explicit BasicPager(const Parameters &);
    [[nodiscard]] auto pin_frame(PageId, bool &) -> Status;
    [[nodiscard]] auto try_make_available() -> Result<bool>;
    auto watch_page(Page &page, PageRegistry::Entry &entry) -> void;

    auto forward_status(Status s, const std::string &message) -> Status
    {
        if (!s.is_ok()) {
            m_logger->error(message);
            m_logger->error("(reason) {}", s.what());
        }
        return s;
    }

    auto save_and_forward_status(Status s, const std::string &message) -> Status
    {
        if (!s.is_ok()) {
            m_logger->error(message);
            m_logger->error("(reason) {}", s.what());
            if (m_status->is_ok()) *m_status = s;
        }
        return s;
    }

    mutable std::mutex m_mutex;
    std::unique_ptr<Framer> m_framer;
    std::shared_ptr<spdlog::logger> m_logger;
    std::unordered_set<PageId, PageId::Hash> *m_images {};
    LogScratchManager *m_scratch {};
    WriteAheadLog *m_wal {};
    PageList m_dirty;
    PageRegistry m_registry;
    Status *m_status {};
    bool *m_has_xact {};
    Size m_dirty_count {};
    Size m_ref_sum {};
};

} // namespace calico

#endif // CALICO_PAGER_PAGER_H
