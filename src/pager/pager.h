#ifndef CALICO_PAGER_H
#define CALICO_PAGER_H

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

class Pager {
public:
    using Ptr = std::unique_ptr<Pager>;

    struct Parameters {
        std::string prefix;
        Storage *storage {};
        LogScratchManager *scratch {};
        WriteAheadLog *wal {};
        System *system {};
        Status *status {};
        Size frame_count {};
        Size page_size {};
    };

    System *system {};

    ~Pager() = default;

    [[nodiscard]] static auto open(const Parameters &param) -> tl::expected<Pager::Ptr, Status>;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto hit_ratio() const -> double;
    [[nodiscard]] auto allocate() -> tl::expected<Page, Status>;
    [[nodiscard]] auto acquire(Id pid) -> tl::expected<Page, Status>;
    [[nodiscard]] auto recovery_lsn() -> Id;
    [[nodiscard]] auto flush(Lsn target_lsn) -> Status;
    auto upgrade(Page &page) -> void;
    auto release(Page page) -> void;
    auto save_state(FileHeader &header) -> void;
    auto load_state(const FileHeader &header) -> void;

    [[nodiscard]]
    auto bytes_written() const -> Size
    {
        return m_framer.bytes_written();
    }

private:
    explicit Pager(const Parameters &param, Framer framer);
    [[nodiscard]] auto pin_frame(Id) -> Status;
    [[nodiscard]] auto try_make_available() -> tl::expected<bool, Status>;
    auto watch_page(Page &page, PageCache::Entry &entry) -> void;
    auto clean_page(PageCache::Entry &entry) -> PageList::Iterator;
    auto set_recovery_lsn(Lsn lsn) -> void;

    mutable std::mutex m_mutex;
    Framer m_framer;
    PageList m_dirty;
    PageCache m_registry;
    LogPtr m_log;
    Lsn m_recovery_lsn;
    Status *m_status {};
    LogScratchManager *m_scratch {};
    WriteAheadLog *m_wal {};
};

} // namespace Calico

#endif // CALICO_PAGER_H