#include "pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page.h"
#include "tree/header.h"
#include "utils/system.h"
#include "utils/types.h"
#include "wal/wal.h"
#include <thread>

namespace Calico {

namespace fs = std::filesystem;

static constexpr Id MAX_ID {std::numeric_limits<Size>::max()};

auto Pager::open(const Parameters &param) -> tl::expected<Pager::Ptr, Status>
{
    auto framer = Framer::open(
        param.prefix,
        param.storage,
        param.page_size,
        param.frame_count);
    if (!framer.has_value())
        return tl::make_unexpected(framer.error());
    
    auto ptr = Pager::Ptr {new (std::nothrow) Pager {param, std::move(*framer)}};
    if (ptr == nullptr)
        return tl::make_unexpected(system_error("could not allocate pager object: out of memory"));
    return ptr;
}

Pager::Pager(const Parameters &param, Framer framer)
    : system {param.system},
      m_framer {std::move(framer)},
      m_log {param.system->create_log("pager")},
      m_scratch {param.scratch},
      m_wal {param.wal}
{
    m_log->info("initializing, cache size is {}", param.frame_count * param.page_size);

    CALICO_EXPECT_NE(system, nullptr);
    CALICO_EXPECT_NE(m_scratch, nullptr);
    CALICO_EXPECT_NE(m_wal, nullptr);
}

auto Pager::page_count() const -> Size
{
    return m_framer.page_count();
}

auto Pager::page_size() const -> Size
{
    return m_framer.page_size();
}

auto Pager::hit_ratio() const -> double
{
    return m_registry.hit_ratio();
}

auto Pager::pin_frame(Id pid) -> Status
{
    CALICO_EXPECT_FALSE(m_registry.contains(pid));

    if (!m_framer.available()) {
        if (const auto success = try_make_available()) {
            // We are out of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
            // very often, if at all, since we don't let the WAL get more than a fixed distance behind the pager.
            if (!*success) {
                auto s = not_found("could not find a frame to use");
                CALICO_WARN(s);
                m_wal->flush();
                return s;
            }
        } else {
            return success.error();
        }
    }
    if (const auto index = m_framer.pin(pid)) {
        // Associate the page ID with the frame index we got from the framer.
        m_registry.put(pid, {*index});
        return ok();
    } else {
        return index.error();
    }
}

auto Pager::clean_page(PageCache::Entry &entry) -> PageList::Iterator
{
    auto token = *entry.dirty_token;
    // Reset the dirty list reference.
    entry.dirty_token.reset();
    return m_dirty.remove(token);
}

auto Pager::set_recovery_lsn(Lsn lsn) -> void
{
    CALICO_EXPECT_LE(m_recovery_lsn, lsn);
    m_recovery_lsn = lsn;
}

auto Pager::flush(Lsn target_lsn) -> Status
{
    // An LSN of NULL causes all pages to be flushed.
    if (target_lsn.is_null()) {
        target_lsn = MAX_ID;
    }

    auto largest = Id::null();
    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        const auto [page_id, record_lsn] = *itr;
        CALICO_EXPECT_TRUE(m_registry.contains(page_id));
        auto &entry = m_registry.get(page_id)->value;
        const auto frame_id = entry.frame_index;
        const auto page_lsn = m_framer.frame_at(frame_id).lsn();

        if (largest < page_lsn) {
            largest = page_lsn;
        }

        if (page_id.as_index() >= m_framer.page_count()) {
            // Page is out of range (abort was called and the database got smaller).
            m_registry.erase(page_id);
            m_framer.unpin(frame_id);
            itr = m_dirty.remove(itr);
        } else if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
            CALICO_WARN(logic_error(
                "could not flush page {}: updates for lsn {} are not in the wal", page_id.value, page_lsn.value));
            itr = next(itr);
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            auto s = m_framer.write_back(frame_id);

            // Advance to the next dirty list entry.
            itr = clean_page(entry); // TODO: We will clean the page regardless of error.
            CALICO_TRY_S(s);
        } else {
            itr = next(itr);
        }
    }
    CALICO_TRY_S(m_framer.sync());

    // We have flushed the entire cache.
    if (target_lsn == MAX_ID) {
        target_lsn = largest;
    }

    // We don't have any pages in memory with LSNs below this value.
    if (m_recovery_lsn < target_lsn) {
        set_recovery_lsn(target_lsn);
    }
    return ok();
}

auto Pager::recovery_lsn() -> Id
{
    auto lowest = MAX_ID;
    for (auto entry: m_dirty) {
        if (lowest > entry.record_lsn) {
            lowest = entry.record_lsn;
        }
    }
    // NOTE: Recovery LSN is not always up-to-date. We update it here and in flush(), making sure it is monotonically increasing.
    if (lowest != MAX_ID && m_recovery_lsn < lowest) {
        set_recovery_lsn(lowest);
    }
    return m_recovery_lsn;
}

auto Pager::try_make_available() -> tl::expected<bool, Status>
{
    Id page_id;
    auto evicted = m_registry.evict([this, &page_id](auto pid, auto entry) {
        const auto &frame = m_framer.frame_at(entry.frame_index);
        page_id = pid;

        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to evict.
        if (frame.ref_count() != 0) {
            return false;
        }

        if (!entry.dirty_token) {
            return true;
        }

        if (system->has_xact) {
            return frame.lsn() <= m_wal->flushed_lsn();
        }

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
    if (!s.is_ok()) {
        return tl::make_unexpected(s);
    }
    return true;
}

auto Pager::watch_page(Page &page, PageCache::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);
    const auto lsn = read_page_lsn(page);

    // Make sure this page is in the dirty list. LSN is saved to determine when the page should be written back.
    if (!entry.dirty_token.has_value()) {
        entry.dirty_token = m_dirty.insert(page.id(), lsn);
    }

    if (system->has_xact && lsn <= system->commit_lsn) {
        const auto next_lsn = m_wal->current_lsn();
        m_wal->log(encode_full_image_payload(
            next_lsn, page.id(), page.view(0), *m_scratch->get()));
        write_page_lsn(page, next_lsn);
    }
}

auto Pager::allocate() -> tl::expected<Page, Status>
{
    return acquire(Id::from_index(m_framer.page_count()));
}

auto Pager::acquire(Id pid) -> tl::expected<Page, Status>
{
    using std::end;

    if (system->has_error()) {
        return tl::make_unexpected(system->original_error().status);
    }

    const auto do_acquire = [this](auto &entry) {
        return m_framer.ref(entry.frame_index);
    };

    CALICO_EXPECT_FALSE(pid.is_null());
    std::lock_guard lock {m_mutex};

    if (auto itr = m_registry.get(pid); itr != end(m_registry)) {
        return do_acquire(itr->value);
    }

    // Spin until a frame becomes available. This may depend on the WAL writing out more WAL records so that we can flush those pages and
    // reuse the frames they were in. pin_frame() checks the WAL flushed LSN, which is increased each time the WAL flushes a block.
    bool success;
    while ((success = !system->has_error())) {
        auto s = pin_frame(pid);
        if (s.is_ok()) {
            break;
        }

        if (!s.is_not_found()) {
            CALICO_WARN(s);

            // Only set the database error state if we are in a transaction. Otherwise, there is no chance of corruption, so we just
            // return the error. Also, we should be in the same thread that controls the core component, so we shouldn't have any
            // data races on this check.
            if (system->has_xact) {
                CALICO_ERROR(s);
            }
            return tl::make_unexpected(s);
        }
    }
    if (!success) {
        return tl::make_unexpected(system->original_error().status);
    }
    auto itr = m_registry.get(pid);
    CALICO_EXPECT_NE(itr, end(m_registry));
    return do_acquire(itr->value);
}

auto Pager::upgrade(Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    m_framer.upgrade(itr->value.frame_index, page);
    watch_page(page, itr->value);
}

auto Pager::release(Page page) -> void
{
    if (page.is_writable() && system->has_xact) {
        const auto next_lsn = m_wal->current_lsn();
        write_page_lsn(page, next_lsn);
        m_wal->log(encode_deltas_payload(
            next_lsn, page.id(), page.view(0),
            page.deltas(), *m_scratch->get()));
    }
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // This page must already be acquired.
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    auto &entry = itr->value;
    m_framer.unref(entry.frame_index, std::move(page));

    if (entry.dirty_token) {
        const auto checkpoint = (*entry.dirty_token)->record_lsn;
        const auto cutoff = system->commit_lsn;

        if (checkpoint <= cutoff) {
            auto s = m_framer.write_back(entry.frame_index);

            if (s.is_ok()) {
                clean_page(entry);
            } else {
                CALICO_ERROR(s);
            }
        }
    }
}

auto Pager::save_state(FileHeader &header) -> void
{
    header.recovery_lsn = m_recovery_lsn;
    m_framer.save_state(header);
}

auto Pager::load_state(const FileHeader &header) -> void
{
    if (m_recovery_lsn.value < header.recovery_lsn.value) {
        set_recovery_lsn(Id {header.recovery_lsn});
    }
    m_framer.load_state(header);
}

} // namespace Calico
