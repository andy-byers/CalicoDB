#include "basic_pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page/page.h"
#include "storage/disk.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal/wal_manager.h"

namespace cco {

auto BasicPager::open(const Parameters &param) -> Result<std::unique_ptr<BasicPager>>
{
    auto pool = std::unique_ptr<BasicPager> {new (std::nothrow) BasicPager {param}};
    pool->m_logger->trace("opening");

    if (!pool) {
        ThreePartMessage message;
        message.set_primary("cannot open buffer pool");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }

    // Open the database file.
    RandomAccessEditor *file {};
    auto s = param.storage.open_random_access_editor(DATA_FILENAME, &file);

    CCO_TRY_STORE(pool->m_framer, Framer::open(
        std::unique_ptr<RandomAccessEditor> {file},
        param.page_size,
        param.frame_count
    ));
    return pool;
}

BasicPager::BasicPager(const Parameters &param)
    : m_logger {create_logger(param.log_sink, "pool")},
      m_scratch {param.page_size},
      m_wal {param.wal}
{}

BasicPager::~BasicPager()
{
//    flush();
}

auto BasicPager::page_count() const -> Size
{
    return m_framer->page_count();
}

auto BasicPager::pin_frame(PageId id) -> Status
{
    if (m_registry.contains(id))
        return Status::ok();

    if (!m_framer->available()) {
        const auto r = try_make_available();
        if (!r.has_value()) return r.error();

        // We are of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
        // very often, if at all.
        if (!*r) {
            LogMessage message {*m_logger};
            message.set_primary("could not pin frame");
            message.set_detail("out of frames");
            message.set_hint("release some pages or wait for the WAL to flush");
            return message.not_found();
        }
    }
    // Read the page into a frame.
    auto r = m_framer->pin(id);
    if (!r.has_value()) return r.error();

    // Associate the page ID with the frame ID we got from the pager.
    m_registry.put(id, *r);
    return Status::ok();
}

auto BasicPager::flush() -> Status
{
    CCO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("flushing");

    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        CCO_EXPECT_NE(m_registry.get(*itr), m_registry.end());
        auto entry = m_registry.get(*itr)->second;
        auto s = m_framer->unpin(entry.frame_id);
        if (!s.is_ok()) return s;

        entry.dirty_itr = m_dirty.end();
        itr = m_dirty.remove(itr);
    }
    return Status::ok();
}

auto BasicPager::flushed_lsn() const -> SequenceNumber
{
    return m_framer->flushed_lsn();
}

auto BasicPager::try_make_available() -> Result<bool>
{
    auto itr = m_registry.find_entry([this](auto, auto fid, auto dirty_itr) {
        const auto &frame = m_framer->frame_at(fid);

        // The buffer pool management happens under lock, so if this frame is not referenced, it is safe to consider for reuse.
        if (frame.ref_count() != 0)
            return false;

        const auto is_protected = frame.lsn() <= m_wal->flushed_lsn();
        CCO_EXPECT_EQ(frame.is_dirty(), dirty_itr == m_dirty.end());
        return !frame.is_dirty() || is_protected;
    });

    if (itr == m_registry.end())
        return false;

    auto [frame_id, dirty_itr] = itr->second;
    const auto &frame = m_framer->frame_at(frame_id);
    const auto was_dirty = frame.is_dirty();
    auto pid = itr->first;
    auto s = m_framer->unpin(frame_id);
    if (!s.is_ok()) return Err {s};

    if (was_dirty)
        m_dirty.remove(dirty_itr);
    m_registry.erase(pid);
    return true;
}

auto BasicPager::release(Page page) -> Status
{
    std::lock_guard lock {m_mutex};
    CCO_EXPECT_GT(m_ref_sum, 0);

    if (m_wal->is_enabled() && !page.deltas().empty()) {
        CCO_EXPECT_TRUE(page.is_dirty());
        page.set_lsn(SequenceNumber {m_wal->current_lsn()});
        m_wal->log_deltas(page.id().value, page.view(0), page.deltas());
    }

    auto itr = m_registry.get(page.id());
    CCO_EXPECT_NE(itr, m_registry.end());
    m_framer->unref(itr->second.frame_id, page);
    m_ref_sum--;
    return Status::ok();
}

auto BasicPager::register_page(Page &page) -> void
{
    // This function needs external synchronization!
    CCO_EXPECT_GT(m_ref_sum, 0);

    auto itr = m_registry.get(page.id());
    CCO_EXPECT_NE(itr, m_registry.end());

    itr->second.dirty_itr = m_dirty.insert(page.id());
    m_dirty_count++;

    if (m_wal->is_enabled())
        m_wal->log_image(page.id().value, page.view(0));
}

auto BasicPager::save_state(FileHeader &header) -> void
{
    m_logger->trace("saving header fields");
    m_framer->save_state(header);
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    m_logger->trace("loading header fields");
    m_framer->load_state(header);
}

auto BasicPager::allocate() -> Result<Page>
{
    return acquire(PageId::from_index(m_framer->page_count()), true);
}

auto BasicPager::acquire(PageId id, bool is_writable) -> Result<Page>
{
    CCO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (!m_status.is_ok())
        return Err {m_status};

    const auto do_acquire = [this, is_writable](PageRegistry::Entry entry) {
        m_ref_sum++;
        auto page = m_framer->ref(entry.frame_id, *this, is_writable);
        // We write a full image WAL record for the page if we are acquiring it as writable for the first time. This is part of the reason we should never
        // acquire a writable page unless we intend to write to it immediately. This way we keep the surface between the Pager interface and Page small
        // (page just uses its Pager* member to call release on itself, but only if it was not already released manually).
        if (is_writable && !page.is_dirty())
            register_page(page);
        return page;
    };
    CCO_EXPECT_FALSE(id.is_null());

    if (auto itr = m_registry.get(id); itr != m_registry.end())
        return do_acquire(itr->second);

    if (auto s = pin_frame(id); !s.is_ok()) {
        m_logger->error(s.what());
        m_status = s;
        return Err {s};
    }

    auto itr = m_registry.get(id);
    CCO_EXPECT_NE(itr, m_registry.end());
    auto x = itr->second;
    return do_acquire(itr->second);
}

} // namespace cco
