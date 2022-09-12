#include "basic_pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page/page.h"
#include "storage/posix_storage.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <thread>

namespace calico {

namespace fs = std::filesystem;

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

auto BasicPager::open(const Parameters &param) -> Result<std::unique_ptr<BasicPager>>
{
    auto pager = std::unique_ptr<BasicPager> {new (std::nothrow) BasicPager {param}};

    if (!pager) {
        ThreePartMessage message;
        message.set_primary("cannot open block pool");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }

    // Open the database file.
    RandomEditor *file {};
    auto s = param.storage.open_random_editor(param.prefix + DATA_FILENAME, &file);
    if (!s.is_ok()) return Err {s};

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
      m_status {&param.status},
      m_has_xact {&param.has_xact}
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

auto BasicPager::pin_frame(PageId id, bool &is_fragile) -> Status
{
    static constexpr auto MSG = "could not pin frame: out of frames";
    CALICO_EXPECT_FALSE(m_registry.contains(id));
    is_fragile = true;

    if (!m_framer->available()) {
        const auto r = try_make_available();
        if (!r.has_value()) return r.error();

        // We are out of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
        // very often, if at all, since we don't let the WAL get more than a fixed distance behind the pager.
        if (!*r) {
            m_logger->warn(MSG);
            std::this_thread::yield();
            return Status::not_found(MSG);
        }
    }
    // Read the page into a frame.
    is_fragile = false;
    auto r = m_framer->pin(id);
    if (!r.has_value()) return r.error();

    // Associate the page ID with the frame ID we got from the framer.
    m_registry.put(id, *r);
    return Status::ok();
}

auto BasicPager::flush() -> Status
{
    static constexpr auto MSG = "could not flush";
    m_logger->info("flushing cached database pages");
    CALICO_EXPECT_EQ(m_ref_sum, 0);

    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        CALICO_EXPECT_TRUE(m_registry.contains(*itr));
        auto &entry = m_registry.get(*itr)->second;
        const auto frame_id = entry.frame_id;
        const auto page_lsn = m_framer->frame_at(frame_id).lsn();
        const auto page_id = *itr;
        const auto in_range = page_id.as_index() < m_framer->page_count();
        auto s = Status::ok();

        // "entry" is a reference.
        entry.dirty_token.reset();
        itr = m_dirty.remove(itr);

        if (in_range) {
            if (page_lsn > m_wal->flushed_lsn()) {
                LogMessage message {*m_logger};
                message.set_primary("could not flush page {}", page_id.value);
                message.set_detail("page updates for LSN {} were not logged", page_lsn.value);
                message.log(spdlog::level::warn);
            } else {
                s = m_framer->write_back(frame_id);
                MAYBE_FORWARD(s, MSG);
            }
        } else {
            // Get rid of all pages that are not in range. This should only happen after pages are deleted during abort() or recovery.
            m_registry.erase(page_id);
            m_framer->unpin(frame_id);
        }
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
//fmt::print("hi\n");
        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to consider for reuse.
        if (frame.ref_count() != 0)
            return false;

        if (!dirty_token.has_value())
            return true;

        if (m_wal->is_working()) {
//fmt::print("{} <= {}\n", frame.lsn().value, m_wal->flushed_lsn());
            return frame.lsn() <= m_wal->flushed_lsn();
        }

        return true;
    });

    if (itr == m_registry.end()) {
        CALICO_EXPECT_TRUE(m_wal->is_working());
        std::this_thread::yield();
        return false;
    }

    auto &[pid, entry] = *itr;
    auto &[frame_id, dirty_token] = entry;
    auto s = Status::ok();
    if (dirty_token.has_value()) {
        s = m_framer->write_back(frame_id);
        // We remove the frame's "dirty" status, even if write_back() failed.
        m_dirty.remove(*dirty_token);
        dirty_token.reset();
    }
    m_framer->unpin(frame_id);
    m_registry.erase(pid);
    if (!s.is_ok()) return Err {s};
    return true;
}

auto BasicPager::release(Page page) -> Status
{
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_ref_sum, 0);

    // This method can only fail for writable pages.
    auto s = Status::ok();
    if (const auto deltas = page.collect_deltas(); m_wal->is_working() && !deltas.empty()) {
        CALICO_EXPECT_TRUE(page.is_writable());
        page.set_lsn(SequenceId {m_wal->current_lsn()});
        s = m_wal->log(page.id().value, page.view(0), page.collect_deltas());
        save_and_forward_status(s, "could not write page deltas to WAL");
    }

    CALICO_EXPECT_TRUE(m_registry.contains(page.id()));
    auto &entry = m_registry.get(page.id())->second;
    m_framer->unref(entry.frame_id, page);
    m_ref_sum--;
    return s;
}

auto BasicPager::watch_page(Page &page, PageRegistry::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_ref_sum, 0);

    if (!page.is_dirty()) {
        entry.dirty_token = m_dirty.insert(page.id());
        m_dirty_count++;
    }

    // TODO: We also have info about whether or not we should watch this page. Like *m_has_xact? Although that may break abort() since it doesn't set *m_has_xact to false until it is totally finished...
    if (m_wal->is_working()) {
        auto s = m_wal->log(page.id().value, page.view(0));
        save_and_forward_status(s, "could not write full image to WAL");

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
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    m_logger->info("loading header fields");
    m_framer->load_state(header);
}

auto BasicPager::allocate() -> Result<Page>
{
    return acquire(PageId::from_index(m_framer->page_count()), true);
}

auto BasicPager::acquire(PageId id, bool is_writable) -> Result<Page>
{
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
    std::lock_guard lock {m_mutex};

    if (auto itr = m_registry.get(id); itr != m_registry.end())
        return do_acquire(itr->second);

    // Spin until a frame becomes available. This may depend on the WAL writing out more WAL records so that we can flush those pages and
    // reuse the frames they were in. pin_frame() checks the WAL flushed LSN, which is incremented each time the WAL flushes a block.
    for (; ; ) {
        bool is_fragile {};
        auto s = pin_frame(id, is_fragile);
        if (s.is_ok()) break;

        if (!s.is_not_found()) {
            m_logger->error(s.what());

            // Only set the database error state if we are in a transaction. Otherwise, there is no chance of corruption, so we just
            // return the error. Also, we should be in the same thread that controls the core component, so we shouldn't have any
            // data races on this check.
            if (*m_has_xact || is_fragile) {
                m_logger->error("setting database error state");
                *m_status = s;
            }
            return Err {s};
        }
    }

    auto itr = m_registry.get(id);
    CALICO_EXPECT_NE(itr, m_registry.end());
    return do_acquire(itr->second);
}

#undef MAYBE_FORWARD

} // namespace calico
