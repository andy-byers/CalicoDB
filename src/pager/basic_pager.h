#ifndef CALICO_PAGER_PAGER_H
#define CALICO_PAGER_PAGER_H

#include <mutex>
#include <unordered_set>

#include "spdlog/logger.h"

#include "page_cache.h"
#include "pager.h"

#include "utils/scratch.h"
#include "wal/helpers.h"

namespace Calico {

class Storage;
class Framer;

class BasicPager : public Pager {
public:
    struct Parameters {
        std::string prefix;
        Storage *storage {};
        LogScratchManager *scratch {};
        WriteAheadLog *wal {};
        System *system {};
        Size frame_count {};
        Size page_size {};
    };

    ~BasicPager() override = default;

    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<Pager::Ptr, Status>;
    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto page_size() const -> Size override;
    [[nodiscard]] auto hit_ratio() const -> double override;
    [[nodiscard]] auto recovery_lsn() -> Id override;
    [[nodiscard]] auto allocate() -> tl::expected<Page_, Status> override;
    [[nodiscard]] auto acquire(Id pid, bool is_writable) -> tl::expected<Page_, Status> override;
    [[nodiscard]] auto release(Page_ page) -> Status override;
    [[nodiscard]] auto flush(Lsn target_lsn) -> Status override;
    auto save_state(FileHeader__ &header) -> void override;
    auto load_state(const FileHeader__ &header) -> void override;

    [[nodiscard]] auto allocate_() -> tl::expected<Page, Status> override;
    [[nodiscard]] auto acquire_(Id) -> tl::expected<Page, Status> override;
    auto upgrade_(Page &page) -> void override;
    auto release_(Page page) -> void override;

    [[nodiscard]]
    auto bytes_written() const -> Size override
    {
        return m_framer.bytes_written();
    }

private:
    explicit BasicPager(const Parameters &param, Framer framer);
    [[nodiscard]] auto pin_frame(Id) -> Status;
    [[nodiscard]] auto try_make_available() -> tl::expected<bool, Status>;
    auto watch_page_(Page &page, PageCache::Entry &entry) -> void;
    auto watch_page(Page_ &page, PageCache::Entry &entry) -> void;
    auto clean_page(PageCache::Entry &entry) -> PageList::Iterator;
    auto set_recovery_lsn(Lsn lsn) -> void;

    mutable std::mutex m_mutex;
    Framer m_framer;
    PageList m_dirty;
    PageCache m_registry;
    LogPtr m_log;
    Lsn m_recovery_lsn;
    LogScratchManager *m_scratch {};
    WriteAheadLog *m_wal {};
    System *m_system {};
};

} // namespace Calico

#endif // CALICO_PAGER_PAGER_H