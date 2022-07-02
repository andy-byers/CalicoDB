#include "buffer_pool.h"
#include "calico/options.h"
#include "calico/exception.h"
#include "page/file_header.h"
#include "page/page.h"
#include "file/interface.h"
#include "utils/logging.h"
#include "wal/interface.h"
#include "wal/wal_record.h"

namespace calico {

BufferPool::BufferPool(Parameters param)
    : m_wal_reader {std::move(param.wal_reader)}
    , m_wal_writer {std::move(param.wal_writer)}
    , m_logger {logging::create_logger(param.log_sink, "BufferPool")}
    , m_scratch {param.page_size}
    , m_pager {{std::move(param.pool_file), param.page_size, param.frame_count}}
    , m_flushed_lsn {param.flushed_lsn}
    , m_next_lsn {param.flushed_lsn + LSN {1}}
    , m_page_count {param.page_count}
    , m_uses_transactions {param.use_transactions}
{
    CALICO_EXPECT_EQ(m_uses_transactions, m_wal_reader && m_wal_writer);
    m_logger->trace("Constructing BufferPool object");

    if (!m_uses_transactions) {
        m_flushed_lsn = LSN::max();
        m_next_lsn = LSN::max();
    }
}

auto BufferPool::can_commit() const -> bool
{
    if (m_uses_transactions) {
        return m_wal_writer->has_committed() || m_wal_writer->has_pending();
    } else {
        return m_dirty_count > 0;
    }
}

auto BufferPool::block_size() const -> Size
{
    return m_uses_transactions ? m_wal_writer->block_size() : 0;
}

auto BufferPool::allocate(PageType type) -> Page
{
    CALICO_EXPECT_TRUE(is_page_type_valid(type));
    auto page = acquire(PID {ROOT_ID_VALUE + m_page_count}, true);
    page.set_type(type);
    m_page_count++;
    return page;
}

/**
 * Get a page object containing data from the database file.
 *
 * This method should only be used to acquire existing pages (unless the page count will be updated subsequently,
 * such as in the case of the "roll-forward" procedure).
 *
 * @param id Page ID of the page to retrieve.
 * @param is_writable True if the page object should allow modification, false otherwise.
 * @return A page object containing the page with the specified page ID.
 */
auto BufferPool::acquire(PID id, bool is_writable) -> Page
{
    CALICO_EXPECT_FALSE(id.is_null());
    auto page = fetch_page(id, is_writable);
    if (m_uses_transactions && is_writable)
        page.enable_tracking(m_scratch.get());
    return page;
}

auto BufferPool::fetch_page(PID id, bool is_writable) -> Page
{
    CALICO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    // Propagate exceptions from the Page destructor (likely I/O errors from the WALWriter).
    propagate_page_error();

    const auto try_fetch = [id, is_writable, this]() -> std::optional<Page> {
        if (auto ref = m_cache.get(id)) {
            auto page = ref->get().borrow(this, is_writable);
            m_ref_sum++;
            return page;
        }
        return std::nullopt;
    };

    // Frame is already pinned.
    if (auto page = try_fetch())
        return std::move(*page);

    // Get page from cache or disk.
    m_cache.put(id, fetch_frame(id));
    auto page = try_fetch();
    CALICO_EXPECT_NE(page, std::nullopt);
    return std::move(*page);
}

auto BufferPool::propagate_page_error() -> void
{
    if (m_error)
        std::rethrow_exception(std::exchange(m_error, {}));
}

auto BufferPool::log_update(Page &page) -> void
{
    CALICO_EXPECT_TRUE(m_uses_transactions);
    page.set_lsn(m_next_lsn);

    const WALRecord::Parameters param {
        page.collect_changes(),
        page.id(),
        page.lsn(),
        m_next_lsn,
    };

    if (const auto lsn = m_wal_writer->append(WALRecord{param}); !lsn.is_null())
        m_flushed_lsn = lsn;

    m_next_lsn++;
}

auto BufferPool::fetch_frame(PID id) -> Frame
{
    const auto try_evict_and_pin = [id, this]() -> std::optional<Frame> {
        if (try_evict_frame())
            return m_pager.pin(id);
        return std::nullopt;
    };
    if (auto frame = m_cache.extract(id))
        return std::move(*frame);

    if (auto frame = m_pager.pin(id))
        return std::move(*frame);

    if (auto frame = try_evict_and_pin())
        return std::move(*frame);

    if (!try_flush_wal()) {
        m_pager.unpin(Frame {m_pager.page_size()});
        return *m_pager.pin(id);
    }
    auto frame = try_evict_and_pin();
    CALICO_EXPECT_NE(frame, std::nullopt);
    return std::move(*frame);
}

auto BufferPool::on_page_release(Page &page) -> void
{
    if (page.has_changes())
        log_update(page);

    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_ref_sum, 0);

    auto ref = m_cache.get(page.id());
    CALICO_EXPECT_NE(ref, std::nullopt);
    auto &frame = ref->get();

    m_dirty_count += !frame.is_dirty() && page.is_dirty();
    frame.synchronize(page);
    m_ref_sum--;
}

auto BufferPool::on_page_error() -> void
{
    // This method should only be called from Page::~Page().
    m_error = std::current_exception();
}

auto BufferPool::roll_forward() -> bool
{
    CALICO_EXPECT_TRUE(uses_transactions());
    m_wal_reader->reset();

    if (!m_wal_reader->record())
        return false;

    do {
        const auto record = *m_wal_reader->record();

        if (m_next_lsn < record.lsn()) {
            m_flushed_lsn = record.lsn();
            m_next_lsn = m_flushed_lsn;
            m_next_lsn++;
        }
        // Stop at the first commit record.
        if (record.is_commit())
            return true;

        const auto update = record.decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() < record.lsn())
            page.redo_changes(record.lsn(), update.changes);

    } while (m_wal_reader->increment());

    return false;
}

auto BufferPool::roll_backward() -> void
{
    CALICO_EXPECT_TRUE(uses_transactions());
    // Make sure the WAL reader is at the end of the log.
    try {
        while (m_wal_reader->increment()) {}
    } catch (const CorruptionError&) {
        m_logger->warn("found a corrupted record at end of WAL");
    }
    CALICO_EXPECT_NE(m_wal_reader->record(), std::nullopt);

    // Skip commit record(s) at the end. These can arise from exceptions during commit(), i.e. we write the commit record,
    // but get an I/O error while flushing dirty database pages.
    while (m_wal_reader->record()->is_commit()) {
        if (!m_wal_reader->decrement()) {
            logging::MessageGroup group;
            group.set_primary("could not roll back");
            group.set_detail("transaction is empty");
            throw CorruptionError {group.err(*m_logger)};
        }
    }

    do {
        const auto record = *m_wal_reader->record();
        CALICO_EXPECT_FALSE(record.is_commit());
        const auto update = record.decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() >= record.lsn())
            page.undo_changes(update.previous_lsn, update.changes);

    } while (m_wal_reader->decrement());
}

auto BufferPool::commit() -> void
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("starting commit");

    if (m_uses_transactions) {
        m_wal_writer->append(WALRecord::commit(m_next_lsn++));
        try_flush_wal();
    }
    try_flush();
    CALICO_EXPECT_EQ(m_dirty_count, 0);

    m_pager.sync();

    // Don't do this until we have succeeded sync'ing the database file.
    if (m_uses_transactions)
        m_wal_writer->truncate();
    m_logger->trace("finished commit");
}

auto BufferPool::abort() -> void
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);
    CALICO_EXPECT_TRUE(m_uses_transactions);
    m_logger->trace("starting abort");

    try {
        try_flush_wal();
    } catch (const IOError &error) {
        m_logger->error("unable to flush WAL: {}", error.what());
    }
    if (!m_wal_writer->has_committed()) {
        m_logger->trace("stopping abort: transaction is empty");
        return;
    }

    // Throw away in-memory updates.
    purge();
    m_wal_reader->reset();
    roll_backward();
    try_flush();
    m_wal_writer->truncate();
    m_logger->trace("finished abort");
}

auto BufferPool::recover() -> bool
{
    CALICO_EXPECT_TRUE(m_uses_transactions);
    m_logger->trace("starting recovery");

    if (!m_wal_writer->has_committed()) {
        m_logger->trace("nothing to recover");
        return false;
    }

    // Read through the WAL in order, applying missing updates to database pages.
    bool found_commit {};
    try {
        found_commit = roll_forward();
    } catch (const CorruptionError &error) {
        logging::MessageGroup group;
        group.set_primary("unable to apply WAL updates");
        group.set_detail("{}",  error.what());
        group.log(*m_logger, spdlog::level::warn);
    }
    // If we didn't encounter a commit record, we must roll back.
    if (!found_commit)
        roll_backward();
    try_flush();
    m_wal_writer->truncate();
    m_logger->trace("finished recovery");
    return true;
}

auto BufferPool::try_flush() -> bool
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);
    if (m_cache.is_empty())
        return false;
    std::optional<Frame> frame;
    while (try_evict_frame()) {}
    return m_cache.is_empty();
}

auto BufferPool::try_evict_frame() -> bool
{
    for (Index i {}; i < m_cache.size(); ++i) {
        auto frame = m_cache.evict();
        CALICO_EXPECT_NE(frame, std::nullopt);
        const auto not_pinned = frame->ref_count() == 0;
        const auto is_safe = !frame->is_dirty() || frame->page_lsn() <= m_flushed_lsn;
        if (not_pinned && is_safe) {
            m_dirty_count -= frame->is_dirty();
            m_pager.unpin(std::move(*frame));
            return true;
        }
        const auto id = frame->page_id();
        m_cache.put(id, std::move(*frame));
    }
    return false;
}

auto BufferPool::try_flush_wal() -> bool
{
    if (const auto lsn = m_wal_writer->flush(); !lsn.is_null()) {
        CALICO_EXPECT_EQ(m_next_lsn, lsn + LSN {1});
        m_flushed_lsn = lsn;
        return true;
    }
    return false;
}

auto BufferPool::purge() -> void
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);
    while (!m_cache.is_empty()) {
        auto frame = m_cache.evict();
        CALICO_EXPECT_NE(frame, std::nullopt);
        m_dirty_count -= frame->is_dirty();
        frame->clean();
        m_pager.unpin(std::move(*frame));
    }
}

auto BufferPool::save_header(FileHeader &header) -> void
{
    header.set_flushed_lsn(m_uses_transactions ? m_flushed_lsn : LSN::null());
    header.set_page_count(m_page_count);
}

auto BufferPool::load_header(const FileHeader &header) -> void
{
    if (m_uses_transactions)
        m_flushed_lsn = header.flushed_lsn();
    m_page_count = header.page_count();
}

} // calico
