#ifndef CCO_POOL_BUFFER_POOL_H
#define CCO_POOL_BUFFER_POOL_H

#include <list>
#include <mutex>
#include <unordered_map>
#include <spdlog/logger.h>
#include "page_cache.h"
#include "frame.h"
#include "interface.h"
#include "utils/scratch.h"

namespace cco {

class IDirectory;
class IFile;
class IWALReader;
class IWALWriter;
class Pager;

class BufferPool: public IBufferPool {
public:
    struct Parameters {
        IDirectory &directory;
        spdlog::sink_ptr log_sink;
        Size frame_count {};
        Size page_count {};
        Size page_size {};
        int permissions {};
        bool use_xact {};
    };

    ~BufferPool() override;

    [[nodiscard]] static auto open(const Parameters&) -> Result<std::unique_ptr<IBufferPool>>;

    [[nodiscard]] auto page_count() const -> Size override
    {
        return m_page_count;
    }

    [[nodiscard]] auto hit_ratio() const -> double override
    {
        return m_cache.hit_ratio();
    }

    [[nodiscard]] auto page_size() const -> Size override;
    [[nodiscard]] auto allocate() -> Result<page::Page> override;
    [[nodiscard]] auto acquire(PID, bool) -> Result<page::Page> override;
    [[nodiscard]] auto release(page::Page) -> Result<void> override;
    [[nodiscard]] auto purge() -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    [[nodiscard]] auto close() -> Result<void> override;
    auto on_release(page::Page&) -> void override;
    auto save_header(page::FileHeader&) -> void override;
    auto load_header(const page::FileHeader&) -> void override;

private:
    struct State {
        std::unique_ptr<IFile> file;
        std::unique_ptr<Pager> pager;
//        std::unique_ptr<IWALReader> wal_reader;
        std::unique_ptr<IWALWriter> wal_writer;
    };
    BufferPool(State, const Parameters&);
    [[nodiscard]] auto pin_frame(PID) -> Result<void>;
    [[nodiscard]] auto try_evict_frame() -> Result<bool>;
    [[nodiscard]] auto do_release(page::Page&) -> Result<void>;

    mutable std::mutex m_mutex;
    std::unique_ptr<IFile> m_file;
    std::unique_ptr<Pager> m_pager;
//    std::unique_ptr<IWALReader> m_wal_reader;
    std::unique_ptr<IWALWriter> m_wal_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    std::vector<Error> m_errors;
    utils::ScratchManager m_scratch;
    PageCache m_cache;
    LSN m_next_lsn {LSN::base()};
    Size m_page_count {};
    Size m_dirty_count {};
    Size m_ref_sum {};
    bool m_use_xact {};
};

} // calico

#endif // CCO_POOL_BUFFER_POOL_H
