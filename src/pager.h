#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include <unordered_set>

#include "page_cache.h"
#include "pager.h"

#include "wal_record.h"

namespace calicodb
{

class Env;
class FrameManager;
class WriteAheadLog;
class TableSet;

class Pager
{
public:
    friend class DBImpl;
    friend class Recovery;

    struct Parameters {
        std::string filename;
        Env *env {};
        WriteAheadLog *wal {};
        InfoLogger *info_log {};
        Lsn *commit_lsn {};
        Status *status {};
        bool *is_running {};
        std::size_t frame_count {};
        std::size_t page_size {};
    };

    ~Pager() = default;

    [[nodiscard]] static auto open(const Parameters &param, Pager **out) -> Status;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto page_size() const -> std::size_t;
    [[nodiscard]] auto hit_ratio() const -> double;
    [[nodiscard]] auto recovery_lsn() -> Id;
    [[nodiscard]] auto bytes_written() const -> std::size_t;
    [[nodiscard]] auto truncate(std::size_t page_count) -> Status;
    [[nodiscard]] auto flush(Lsn target_lsn = Lsn::null()) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto acquire(Page &page) -> Status;
    auto upgrade(Page &page, int important = -1) -> void;
    auto release(Page page) -> void;
    auto discard(Page page) -> void;
    auto save_state(FileHeader &header) -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    explicit Pager(const Parameters &param, Editor &file, AlignedBuffer buffer);
    [[nodiscard]] auto pin_frame(Id pid) -> Status;
    [[nodiscard]] auto do_pin_frame(Id pid) -> Status;
    [[nodiscard]] auto make_frame_available() -> bool;
    auto watch_page(Page &page, PageCache::Entry &entry, int important) -> void;
    auto clean_page(PageCache::Entry &entry) -> PageList::Iterator;

    std::string m_filename;
    FrameManager m_frames;
    PageList m_dirty;
    PageCache m_cache;
    Lsn m_recovery_lsn;
    bool *m_is_running {};
    Status *m_status {};
    WriteAheadLog *m_wal {};
    Env *m_env {};
    InfoLogger *m_info_log {};
    Lsn *m_commit_lsn {};
};

} // namespace calicodb

#endif // CALICODB_PAGER_H