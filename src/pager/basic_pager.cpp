#include "basic_pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page/page.h"
#include "store/disk.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <chrono>
#include <thread>

namespace calico {

namespace fs = std::filesystem;

auto BasicPager::open(const Parameters &param) -> Result<std::unique_ptr<BasicPager>>
{
    auto pager = std::unique_ptr<BasicPager> {new (std::nothrow) BasicPager {param}};

    if (!pager) {
        ThreePartMessage message;
        message.set_primary("cannot open block pool");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }

    fs::path root {param.root};
    root /= DATA_FILENAME;

    // Open the database file.
    RandomEditor *file {};
    auto s = param.storage.open_random_editor(root, &file);

    CALICO_TRY_STORE(pager->m_framer, Framer::open(
        std::unique_ptr<RandomEditor> {file},
        param.wal,
        param.page_size,
        param.frame_count
    ));
    return pager;
}

BasicPager::BasicPager(const Parameters &param)
    : m_logger {create_logger(param.log_sink, "pager")},
      m_wal {&param.wal},
      m_status {&param.status}
{
    m_logger->info("constructing BasicPager instance");
}

auto BasicPager::page_count() const -> Size
{
    return m_framer->page_count();
}

auto BasicPager::page_size() const -> Size
{
    return m_framer->page_size();
}

auto BasicPager::pin_frame(PageId id) -> Status
{
    static constexpr auto MSG = "could not pin frame: out of frames";
    CALICO_EXPECT_FALSE(m_registry.contains(id));

    if (!m_framer->available()) {
        const auto r = try_make_available();
        if (!r.has_value()) return r.error();

        // We are out of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
        // very often, if at all.
        if (!*r) {
            std::this_thread::yield();
            return Status::not_found(MSG);
        }
    }
    // Read the page into a frame.
    auto r = m_framer->pin(id);
    if (!r.has_value()) return r.error();

    // Associate the page ID with the frame ID we got from the framer.
    m_registry.put(id, *r);
    return Status::ok();
}

auto BasicPager::flush() -> Status
{
    m_logger->info("flushing cached database pages");
    CALICO_EXPECT_EQ(m_ref_sum, 0);

    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        CALICO_EXPECT_NE(m_registry.get(*itr), m_registry.end());
        auto entry = m_registry.get(*itr)->second;
        const auto page_lsn = m_framer->frame_at(entry.frame_id).lsn();

        if (page_lsn > m_wal->flushed_lsn()) {
            LogMessage message {*m_logger};
            message.set_primary("could not flush");
            message.set_detail("WAL has pending updates");
            message.set_hint("flush the WAL and try again");
            return message.logic_error();
        }
        auto s = m_framer->unpin(entry.frame_id, true);
        if (!s.is_ok()) return s;
        m_registry.erase(*itr);
        entry.dirty_token.reset();
        itr = m_dirty.remove(itr);
    }
    return m_framer->sync();
}

auto BasicPager::flushed_lsn() const -> SequenceId
{
    return m_framer->flushed_lsn();
}

auto BasicPager::try_make_available() -> Result<bool>
{
    auto itr = m_registry.find_entry([this](auto, auto fid, auto dirty_token) {
        const auto &frame = m_framer->frame_at(fid);

        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to consider for reuse.
        if (frame.ref_count() != 0)
            return false;

        if (!dirty_token.has_value())
            return true;

        if (m_wal->is_writing())
            return frame.lsn() <= m_wal->flushed_lsn();

        return true;
    });

    if (itr == m_registry.end()) {
        CALICO_EXPECT_TRUE(m_wal->is_writing());
        const auto s = m_wal->flush_pending();
        if (!s.is_ok()) return Err {s};
        return false;
    }

    auto &[pid, entry] = *itr;
    auto &[frame_id, dirty_token] = entry;
    const auto was_dirty = dirty_token.has_value();
    auto s = m_framer->unpin(frame_id, was_dirty);
    if (!s.is_ok()) return Err {s};

    if (was_dirty) {
        m_dirty.remove(*dirty_token);
        dirty_token.reset();
    }
    m_registry.erase(pid);
    return true;
}

auto BasicPager::release(Page page) -> Status
{
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_ref_sum, 0);

    if (const auto deltas = page.collect_deltas(); m_wal->is_writing() && !deltas.empty()) {
        page.set_lsn(SequenceId {m_wal->current_lsn()});
        auto s = m_wal->log_deltas(page.id().value, page.view(0), page.collect_deltas());

        if (!s.is_ok()) {
            *m_status = s;
            m_logger->error("could not write page deltas to WAL");
            m_logger->error("(reason) {}", s.what());
            return s;
        }
    }

    CALICO_EXPECT_TRUE(m_registry.contains(page.id()));
    auto &entry = m_registry.get(page.id())->second;
    m_framer->unref(entry.frame_id, page);
    m_ref_sum--;
    return Status::ok();
}

auto BasicPager::watch_page(Page &page, PageRegistry::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_ref_sum, 0);

    if (!page.is_dirty()) {
        entry.dirty_token = m_dirty.insert(page.id());
        m_dirty_count++;
    }

    if (m_wal->is_writing()) {
        auto s = m_wal->log_image(page.id().value, page.view(0));

        if (!s.is_ok()) {
            m_logger->error("could not write full image to WAL");
            m_logger->error("(reason) {}", s.what());
            *m_status = s;
        }
    } else if (m_wal->is_enabled()) {
        LogMessage message {*m_logger};
        message.set_primary("omitting watch for page {}", page.id().value);
        message.set_detail("WAL writer has not been started");
        message.log(spdlog::level::info);
    }
}

auto BasicPager::save_state(FileHeader &header) -> void
{
    m_logger->info("saving header fields");
    m_framer->save_state(header);
    m_wal->save_state(header);
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    m_logger->info("loading header fields");
    m_framer->load_state(header);
    m_wal->load_state(header);
}

auto BasicPager::allocate() -> Result<Page>
{
    return acquire(PageId::from_index(m_framer->page_count()), true);
}

auto BasicPager::acquire(PageId id, bool is_writable) -> Result<Page>
{
    CALICO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (!m_status->is_ok())
        return Err {*m_status};

    const auto do_acquire = [this, is_writable](auto &entry) {
        m_ref_sum++;
        const auto is_frame_dirty = entry.dirty_token.has_value();
        auto page = m_framer->ref(entry.frame_id, *this, is_writable, is_frame_dirty);

        // We write a full image WAL record for the page if we are acquiring it as writable for the first time. This is part of the reason we should never
        // acquire a writable page unless we intend to write to it immediately. This way we keep the surface between the Pager interface and Page small
        // (page just uses its Pager* member to call release on itself, but only if it was not already released manually).
        if (is_writable) watch_page(page, entry);

        return page;
    };
    CALICO_EXPECT_FALSE(id.is_null());

    if (auto itr = m_registry.get(id); itr != m_registry.end())
        return do_acquire(itr->second);

    // Spin until a frame becomes available. This may depend on the WAL writing out more WAL records so that we can flush those pages and
    // reuse the frames they were in. pin_frame() checks the WAL flushed LSN, which is incremented each time the WAL flushes a block.
    for (; ; ) {
        auto s = pin_frame(id);
        if (s.is_ok()) break;

        if (!s.is_not_found()) {
            m_logger->error(s.what());
            *m_status = s;
            return Err {s};
        }
    }

    auto itr = m_registry.get(id);
    CALICO_EXPECT_NE(itr, m_registry.end());
    return do_acquire(itr->second);
}

} // namespace calico
