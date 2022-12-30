#ifndef CALICO_PAGER_PAGER_H
#define CALICO_PAGER_PAGER_H

#include <mutex>
#include <unordered_set>

#include "spdlog/logger.h"

#include "pager.h"
#include "registry.h"

#include "utils/scratch.h"
#include "wal/helpers.h"

namespace calico {

class Storage;
class Framer;

class BasicPager : public Pager {
public:
    struct Parameters {
        std::string prefix;
        Storage *storage {};
        LogScratchManager *scratch {};
        std::unordered_set<identifier, identifier::hash> *images {};
        WriteAheadLog *wal {};
        Status *status {};
        bool *has_xact {};
        identifier *commit_lsn {};
        spdlog::sink_ptr log_sink;
        Size frame_count {};
        Size page_size {};
    };

    ~BasicPager() override = default;

    [[nodiscard]] static auto open(const Parameters &param, BasicPager **out) -> Status;
    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto page_size() const -> Size override;
    [[nodiscard]] auto hit_ratio() const -> double override;
    [[nodiscard]] auto flushed_lsn() const -> identifier override;
    [[nodiscard]] auto allocate() -> tl::expected<Page, Status> override;
    [[nodiscard]] auto acquire(identifier, bool) -> tl::expected<Page, Status> override;
    [[nodiscard]] auto release(Page) -> Status override;
    [[nodiscard]] auto flush(identifier target_lsn) -> Status override;
    auto save_state(FileHeader &) -> void override;
    auto load_state(const FileHeader &) -> void override;

    [[nodiscard]]
    auto status() const -> Status override
    {
        return *m_status;
    }

private:
    explicit BasicPager(const Parameters &);
    [[nodiscard]] auto pin_frame(identifier, bool &) -> Status;
    [[nodiscard]] auto try_make_available() -> tl::expected<bool, Status>;
    auto watch_page(Page &page, PageRegistry::Entry &entry) -> void;
    auto clean_page(PageRegistry::Entry &entry) -> PageList::Iterator;
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
    std::unordered_set<identifier, identifier::hash> *m_images {};
    LogScratchManager *m_scratch {};
    WriteAheadLog *m_wal {};
    PageList m_dirty;
    PageRegistry m_registry;
    Status *m_status {};
    bool *m_has_xact {};
    identifier *m_commit_lsn {};
};

} // namespace calico

#endif // CALICO_PAGER_PAGER_H