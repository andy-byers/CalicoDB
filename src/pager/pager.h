#ifndef CALICO_PAGER_H
#define CALICO_PAGER_H

#include <unordered_set>

#include "page_cache.h"
#include "pager.h"

#include "wal/record.h"

namespace Calico {

class Storage;
class FrameManager;
class WriteAheadLog;

class Pager {
public:
    friend class DatabaseImpl;
    friend class Recovery;

    struct Parameters {
        std::string prefix;
        Storage *storage {};
        std::string *scratch {};
        WriteAheadLog *wal {};
        InfoLogger *info_log {};
        Status *status {};
        Lsn *commit_lsn {};
        bool *in_txn {};
        Size frame_count {};
        Size page_size {};
    };

    ~Pager() = default;

    [[nodiscard]] static auto open(const Parameters &param, Pager **out) -> Status;
    [[nodiscard]] auto page_count() const -> Size;
    [[nodiscard]] auto page_size() const -> Size;
    [[nodiscard]] auto hit_ratio() const -> double;
    [[nodiscard]] auto recovery_lsn() -> Id;
    [[nodiscard]] auto bytes_written() const -> Size;
    [[nodiscard]] auto truncate(Size page_count) -> Status;
    [[nodiscard]] auto flush(Lsn target_lsn = Lsn::null()) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto acquire(Id pid, Page &page) -> Status;
    auto upgrade(Page &page, int important = -1) -> void;
    auto release(Page page) -> void;
    auto save_state(FileHeader &header) -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    explicit Pager(const Parameters &param, Editor *file, AlignedBuffer buffer);
    [[nodiscard]] auto pin_frame(Id pid) -> Status;
    [[nodiscard]] auto do_pin_frame(Id pid) -> Status;
    [[nodiscard]] auto make_frame_available() -> bool;
    auto watch_page(Page &page, PageCache::Entry &entry, int important) -> void;
    auto clean_page(PageCache::Entry &entry) -> PageList::Iterator;

    std::string m_path;
    FrameManager m_frames;
    PageList m_dirty;
    PageCache m_cache;
    Lsn m_recovery_lsn;
    Lsn *m_commit_lsn {};
    bool *m_in_txn {};
    Status *m_status {};
    std::string *m_scratch {};
    WriteAheadLog *m_wal {};
    Storage *m_storage {};
    InfoLogger *m_info_log {};
};

} // namespace Calico

#endif // CALICO_PAGER_H