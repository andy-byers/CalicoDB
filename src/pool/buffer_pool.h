#ifndef CUB_POOL_PERSISTENT_POOL_H
#define CUB_POOL_PERSISTENT_POOL_H

#include <unordered_map>
#include <list>

#include "common.h"
#include "interface.h"
#include "pager.h"
#include "cache.h"
#include "utils/scratch.h"

namespace cub {

class ILogFile;
class IReadWriteFile;
class IWALReader;
class IWALWriter;
class Page;

class BufferPool: public IBufferPool {
public:
    struct Parameters {
        std::unique_ptr<IReadWriteFile> database_storage;
        std::unique_ptr<IWALReader> wal_reader;
        std::unique_ptr<IWALWriter> wal_writer;
        LSN flushed_lsn;
        Size frame_count{};
        Size page_count{};
        Size page_size{};
    };

    explicit BufferPool(Parameters);
    ~BufferPool() override = default;

    [[nodiscard]] auto page_count() const -> Size override;
    [[nodiscard]] auto flushed_lsn() const -> LSN override;
    [[nodiscard]] auto page_size() const -> Size override;
    [[nodiscard]] auto block_size() const -> Size override;
    [[nodiscard]] auto hit_ratio() const -> double override;
    [[nodiscard]] auto allocate(PageType) -> Page override;
    [[nodiscard]] auto acquire(PID, bool) -> Page override;
    auto flush() -> void override;
    auto commit() -> void override;
    auto abort() -> void override;
    auto purge() -> void override;
    auto recover() -> void override;
    auto save_header(FileHeader&) -> void override;
    auto on_page_release(Page&) -> void override;
    auto on_page_error() -> void override;

private:
    auto log_update(Page&) -> void;
    auto roll_forward() -> bool;
    auto roll_backward() -> void;
    auto do_acquire(PID, bool) -> Page;
    auto fetch_frame(PID) -> Frame;

    std::unordered_map<PID, Frame, PID::Hasher> m_pinned;
    std::unique_ptr<IWALReader> m_wal_reader;
    std::unique_ptr<IWALWriter> m_wal_writer;
    ScratchManager m_scratch;
    PageCache m_cache;
    Pager m_pager;
    LSN m_flushed_lsn{};
    LSN m_next_lsn{};
    Size m_page_count{};
    Size m_ref_sum{};
    int m_fault{};
    bool m_has_fault{};
};

} // cub

#endif // CUB_POOL_PERSISTENT_POOL_H
