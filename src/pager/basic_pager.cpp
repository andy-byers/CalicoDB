#include "basic_pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page/page.h"
#include "utils/header.h"
#include "utils/system.h"
#include "utils/types.h"
#include <thread>

namespace Calico {

namespace fs = std::filesystem;

static constexpr Id MAX_ID {std::numeric_limits<Size>::max()};

auto BasicPager::open(const Parameters &param) -> tl::expected<Pager::Ptr, Status>
{
    auto framer = Framer::open(
        param.prefix,
        param.storage,
        param.page_size,
        param.frame_count);
    if (!framer.has_value())
        return tl::make_unexpected(framer.error());
    
    auto ptr = Pager::Ptr {new (std::nothrow) BasicPager {param, std::move(*framer)}};
    if (ptr == nullptr)
        return tl::make_unexpected(system_error("could not allocate pager object: out of memory"));
    return ptr;
}

BasicPager::BasicPager(const Parameters &param, Framer framer)
    : m_framer {std::move(framer)},
      m_log {param.system->create_log("pager")},
      m_images {param.images},
      m_scratch {param.scratch},
      m_wal {param.wal},
      m_system {param.system}
{
    m_log->info("initializing with {} frames, each of size {} B", param.frame_count, param.page_size);

    CALICO_EXPECT_NE(param.system, nullptr);
    CALICO_EXPECT_NE(param.scratch, nullptr);
    CALICO_EXPECT_NE(param.images, nullptr);
    CALICO_EXPECT_NE(param.storage, nullptr);
    CALICO_EXPECT_NE(param.wal, nullptr);
}

auto BasicPager::page_count() const -> Size
{
    return m_framer.page_count();
}

auto BasicPager::page_size() const -> Size
{
    return m_framer.page_size();
}

auto BasicPager::hit_ratio() const -> double
{
    return m_registry.hit_ratio();
}

auto BasicPager::pin_frame(Id pid) -> Status
{
    CALICO_EXPECT_FALSE(m_registry.contains(pid));

    if (!m_framer.available()) {
        const auto r = try_make_available();
        if (!r.has_value()) return r.error();

        // We are out of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
        // very often, if at all, since we don't let the WAL get more than a fixed distance behind the pager.
        if (!*r) {
            auto s = not_found("could not find a frame to use");
            CALICO_WARN(s);
            m_wal->flush();
            return s;
        }
    }
    // Read the page into a frame.
    auto r = m_framer.pin(pid);
    if (!r.has_value()) return r.error();

    // Associate the page ID with the frame index we got from the framer.
    m_registry.put(pid, {*r});
    return ok();
}

auto BasicPager::clean_page(PageCache::Entry &entry) -> PageList::Iterator
{
    auto token = *entry.dirty_token;
    // Reset the dirty list reference.
    entry.dirty_token.reset();
    return m_dirty.remove(token);
}

auto BasicPager::set_recovery_lsn(Id lsn) -> void
{
     m_log->info("set recovery lsn to {} (was {})", m_recovery_lsn.value, lsn.value);
    CALICO_EXPECT_LE(m_recovery_lsn, lsn);
    m_recovery_lsn = lsn;
}

auto BasicPager::flush(Id target_lsn) -> Status
{
    CALICO_EXPECT_EQ(m_framer.ref_sum(), 0);

    // An LSN of NULL causes all pages to be flushed.
    if (target_lsn.is_null())
        target_lsn = MAX_ID;

    auto largest = Id::null();
    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        const auto [page_id, record_lsn] = *itr;
        CALICO_EXPECT_TRUE(m_registry.contains(page_id));
        auto &entry = m_registry.get(page_id)->value;
        const auto frame_id = entry.frame_index;
        const auto page_lsn = m_framer.frame_at(frame_id).lsn();

        if (largest < page_lsn)
            largest = page_lsn;

        if (page_id.as_index() >= m_framer.page_count()) {
            // Page is out of range (abort was called and the database got smaller).
            m_registry.erase(page_id);
            m_framer.unpin(frame_id);
            itr = m_dirty.remove(itr);
            continue;
        } else if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
            CALICO_WARN(logic_error(
                "could not flush page {}: page updates for LSN {} are not in the WAL", page_id.value, page_lsn.value));
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            auto s = m_framer.write_back(frame_id);

            // Advance to the next dirty list entry.
            itr = clean_page(entry);
            CALICO_TRY_S(s);
            continue;
        }
        // Skip this page.
        itr = next(itr);
    }
    CALICO_TRY_S(m_framer.sync());

    // We have flushed the entire cache.
    if (target_lsn == MAX_ID)
        target_lsn = largest;

    // We don't have any pages in memory with LSNs below this value.
    if (m_recovery_lsn < target_lsn)
        set_recovery_lsn(target_lsn);
    return ok();
}

auto BasicPager::recovery_lsn() -> Id
{
    auto lowest = MAX_ID;
    for (auto entry: m_dirty) {
        if (lowest > entry.record_lsn)
            lowest = entry.record_lsn;
    }
    // NOTE: Recovery LSN is not always up-to-date. We update it here and in flush(), making sure it is monotonically increasing.
    if (lowest != MAX_ID && m_recovery_lsn < lowest)
        set_recovery_lsn(lowest);
    return m_recovery_lsn;
}

auto BasicPager::try_make_available() -> tl::expected<bool, Status>
{
    Id page_id;
    auto evicted = m_registry.evict([this, &page_id](auto pid, auto entry) {
        const auto &frame = m_framer.frame_at(entry.frame_index);
        page_id = pid;

        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to evict.
        if (frame.ref_count() != 0)
            return false;

        if (!entry.dirty_token)
            return true;

        if (m_system->has_xact)
            return frame.lsn() <= m_wal->flushed_lsn();

        return true;
    });

    if (!evicted.has_value()) {
        m_wal->flush();
        return false;
    }

    auto &[frame_index, dirty_token] = *evicted;
    auto s = ok();

    if (dirty_token) {
        // NOTE: We don't update the record LSN field because we are getting rid of this page.
        s = m_framer.write_back(frame_index);
        clean_page(*evicted);
    }
    m_framer.unpin(frame_index);
    if (!s.is_ok()) return tl::make_unexpected(s);
    return true;
}

auto BasicPager::release(Page page) -> Status
{
    // NOTE: This block should be safe, because we can only have 1 writable page acquired at any given time. We will not enter unless the
    //       page was written to while it was acquired.
    if (const auto deltas = page.collect_deltas(); m_system->has_xact && !deltas.empty()) {
        CALICO_EXPECT_TRUE(page.is_writable());

        // Write the next LSN to the page. Note that this change will be recorded in the delta record we are
        // about to write.
        const auto lsn = m_wal->current_lsn();
        page.set_lsn(lsn);

        // Convert the deltas into a WAL payload, written to scratch memory.
        WalPayloadIn payload {lsn, m_scratch->get()};
        const auto size = encode_deltas_payload(
            page.id(), page.view(0),
            page.collect_deltas(), payload.data());
        payload.shrink_to_fit(size);

        // Log the delta record.
        m_wal->log(payload);
    }
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // This page must already be acquired.
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    auto &entry = itr->value;
    m_framer.unref(entry.frame_index, page);

    // Only write back dirty pages that haven't been written since the last commit.
    if (entry.dirty_token && (*entry.dirty_token)->record_lsn < m_system->commit_lsn) {
        auto s = m_framer.write_back(entry.frame_index);

        if (s.is_ok()) {
            clean_page(entry);
        } else {
            CALICO_ERROR(s);
            return s;
        }
    }
    return ok();
}

auto BasicPager::watch_page(Page &page, PageCache::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // Make sure this page is in the dirty list. LSN is saved to determine when the page should be written back.
    if (!entry.dirty_token.has_value())
        entry.dirty_token = m_dirty.insert(page.id(), page.lsn());

    if (m_system->has_xact) {
        // Don't write a full image record to the WAL if we already have one for this page during this transaction.
        const auto itr = m_images->find(page.id());
        if (itr != cend(*m_images))
            return;
        m_images->emplace(page.id());

        WalPayloadIn payload {m_wal->current_lsn(), m_scratch->get()};
        const auto size = encode_full_image_payload(
            page.id(), page.view(0), payload.data());
        payload.shrink_to_fit(size);
        page.set_lsn(payload.lsn());

        m_wal->log(payload);
    }
}

auto BasicPager::save_state(FileHeader &header) -> void
{
    header.recovery_lsn = m_recovery_lsn.value;
    m_framer.save_state(header);
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    if (m_recovery_lsn.value < header.recovery_lsn)
        set_recovery_lsn(Id {header.recovery_lsn});
    m_framer.load_state(header);
}

auto BasicPager::allocate() -> tl::expected<Page, Status>
{
    return acquire(Id::from_index(m_framer.page_count()), true);
}

auto BasicPager::acquire(Id id, bool is_writable) -> tl::expected<Page, Status>
{
    using std::end;

    const auto do_acquire = [this, is_writable](auto &entry) {
        auto page = m_framer.ref(entry.frame_index, *this, is_writable);

        // We write a full image WAL record for the page if we are acquiring it as writable for the first time. This is part of the reason we should never
        // acquire a writable page unless we intend to write to it immediately. This way we keep the surface between the Pager interface and Page small
        // (page just uses its Pager* member to call release on itself, but only if it was not already released manually).
        if (is_writable) watch_page(page, entry);

        return page;
    };
    CALICO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (auto itr = m_registry.get(id); itr != end(m_registry))
        return do_acquire(itr->value);

    // Spin until a frame becomes available. This may depend on the WAL writing out more WAL records so that we can flush those pages and
    // reuse the frames they were in. pin_frame() checks the WAL flushed LSN, which is incremented each time the WAL flushes a block.
    auto success = true;
    while ((success = !m_system->has_error())) {
        auto s = pin_frame(id);
        if (s.is_ok()) break;

        if (!s.is_not_found()) {
            CALICO_WARN(s);

            // Only set the database error state if we are in a transaction. Otherwise, there is no chance of corruption, so we just
            // return the error. Also, we should be in the same thread that controls the core component, so we shouldn't have any
            // data races on this check.
            if (m_system->has_xact)
                CALICO_ERROR(s);
            return tl::make_unexpected(s);
        }
    }
    if (!success)
        return tl::make_unexpected(m_system->original_error().status);
    auto itr = m_registry.get(id);
    CALICO_EXPECT_NE(itr, end(m_registry));
    return do_acquire(itr->value);
}

} // namespace Calico
