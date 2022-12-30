#include "basic_pager.h"
#include "calico/options.h"
#include "framer.h"
#include "page/page.h"
#include "storage/posix_storage.h"
#include "utils/info_log.h"
#include "utils/types.h"
#include <thread>

namespace calico {

namespace fs = std::filesystem;

#define MAYBE_FORWARD(expr, message) \
    do { \
        const auto &calico_s = (expr); \
        if (!calico_s.is_ok()) return forward_status(calico_s, message); \
    } while (0)

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
        return tl::make_unexpected(Status::system_error("could not allocate pager object: out of memory"));
    return ptr;
}

BasicPager::BasicPager(const Parameters &param, Framer framer)
    : m_framer {std::move(framer)},
      m_logger {create_logger(param.log_sink, "pager")},
      m_images {param.images},
      m_scratch {param.scratch},
      m_wal {param.wal},
      m_status {param.status},
      m_has_xact {param.has_xact},
      m_commit_lsn {param.commit_lsn}
{
    m_logger->info("constructing BasicPager instance");
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

auto BasicPager::pin_frame(Id pid, bool &is_fragile) -> Status
{
    static constexpr auto MSG = "could not pin frame: out of frames";
    CALICO_EXPECT_FALSE(m_registry.contains(pid));
    is_fragile = true;

    if (!m_framer.available()) {
        const auto r = try_make_available();
        if (!r.has_value()) return r.error();

        // We are out of frames! We may need to wait until the WAL has performed another flush. This really shouldn't happen
        // very often, if at all, since we don't let the WAL get more than a fixed distance behind the pager.
        if (!*r) {
            m_logger->warn(MSG);
            *m_status = m_wal->flush();
            return Status::not_found(MSG);
        }
    }
    // Read the page into a frame.
    is_fragile = false;
    auto r = m_framer.pin(pid);
    if (!r.has_value()) return r.error();

    // Associate the page ID with the frame index we got from the framer.
    m_registry.put(pid, {*r});
    return Status::ok();
}

auto BasicPager::clean_page(PageRegistry::Entry &entry) -> PageList::Iterator
{
    auto next = m_dirty.remove(*entry.dirty_token);
    entry.record_lsn = Id::null();
    entry.dirty_token.reset();
    return next;
}

auto BasicPager::flush(Id target_lsn) -> Status
{
    static constexpr auto MSG = "could not flush";
    m_logger->info("flushing cached database pages");
    CALICO_EXPECT_EQ(m_framer.ref_sum(), 0);

    // An LSN of NULL causes all pages to be flushed.
    if (target_lsn.is_null())
        target_lsn.value = std::numeric_limits<size_t>::max();

    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        CALICO_EXPECT_TRUE(m_registry.contains(*itr));
        const auto page_id = *itr;
        auto &entry = m_registry.get(page_id)->value;
        const auto frame_id = entry.frame_index;
        const auto record_lsn = entry.record_lsn;
        const auto page_lsn = m_framer.frame_at(frame_id).lsn();
        auto s = Status::ok();

        if (page_id.as_index() >= m_framer.page_count()) {
            // Page is out of range (abort was called and the database got smaller).
            m_registry.erase(page_id);
            m_framer.unpin(frame_id);
        } else if (page_lsn > m_wal->flushed_lsn()) {
            // WAL record referencing this page has not been flushed yet.
            LogMessage message {*m_logger};
            message.set_primary("could not flush page {}", page_id.value);
            message.set_detail("page updates for LSN {} are not in the WAL", page_lsn.value);
            message.log(spdlog::level::warn);
            itr = next(itr);
            continue;
        } else if (record_lsn <= target_lsn) {
            // Flush the page.
            s = m_framer.write_back(frame_id);
            save_and_forward_status(s, MSG);
        } else {
            // Page doesn't need to be flushed yet.
            itr = next(itr);
            continue;
        }
        // Advance to the next dirty list entry.
        itr = clean_page(entry);
        MAYBE_FORWARD(s, MSG);
    }
    return m_framer.sync();
}

auto BasicPager::flushed_lsn() const -> Id
{
    return m_framer.flushed_lsn();
}

auto BasicPager::try_make_available() -> tl::expected<bool, Status>
{
    Id page_id;
    auto evicted = m_registry.evict([&](auto id, auto entry) {
        const auto &frame = m_framer.frame_at(entry.frame_index);
        page_id = id;

        // The page/frame management happens under lock, so if this frame is not referenced, it is safe to evict.
        if (frame.ref_count() != 0)
            return false;

        if (!entry.dirty_token)
            return true;

        if (m_wal->is_working())
            return frame.lsn() <= m_wal->flushed_lsn();

        return true;
    });

    if (!evicted.has_value()) {
        CALICO_EXPECT_TRUE(m_wal->is_working());
        *m_status = m_wal->flush();
        save_and_forward_status(*m_status, "could not flush WAL");
        return false;
    }

    auto &[frame_index, record_lsn, dirty_token] = *evicted;
    auto s = Status::ok();
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
    auto s = Status::ok();
    if (const auto deltas = page.collect_deltas(); m_wal->is_working() && !deltas.empty()) {
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
        s = m_wal->log(payload);
        save_and_forward_status(s, "could not write page deltas to WAL");
    }
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // This page must already be acquired.
    auto itr = m_registry.get(page.id());
    CALICO_EXPECT_NE(itr, m_registry.end());
    auto &entry = itr->value;
    m_framer.unref(entry.frame_index, page);

    // Note that the commit LSN is always less than or equal to the WAL flushed LSN.
    if (entry.dirty_token && entry.record_lsn < *m_commit_lsn) {
        s = m_framer.write_back(entry.frame_index);
        save_and_forward_status(s, "could not write back old page");
        if (s.is_ok()) clean_page(entry);
    }
    return s;
}

auto BasicPager::watch_page(Page &page, PageRegistry::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_framer.ref_sum(), 0);

    // Make sure this page is in the dirty list. LSN is saved to determine when the page should be written back.
    if (!entry.dirty_token) {
        entry.record_lsn = m_wal->current_lsn();
        entry.dirty_token = m_dirty.insert(page.id());
    }

    if (m_wal->is_working()) {
        // Don't write a full image record to the WAL if we already have one for
        // this page during this transaction.
        const auto itr = m_images->find(page.id());
        if (itr != cend(*m_images))
            return;
        m_images->emplace(page.id());

        WalPayloadIn payload {m_wal->current_lsn(), m_scratch->get()};
        const auto size = encode_full_image_payload(
            page.id(), page.view(0), payload.data());
        payload.shrink_to_fit(size);

        auto s = m_wal->log(payload);
        save_and_forward_status(s, "could not write full image to WAL");
    }
}

auto BasicPager::save_state(FileHeader &header) -> void
{
    m_logger->info("saving header fields");
    m_framer.save_state(header);
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    m_logger->info("loading header fields");
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

    // A fatal error has occurred.
    if (!m_status->is_ok())
        return tl::make_unexpected(*m_status);

    if (auto itr = m_registry.get(id); itr != end(m_registry))
        return do_acquire(itr->value);

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
            return tl::make_unexpected(s);
        }
        s = m_wal->worker_status();
        if (!s.is_ok()) return tl::make_unexpected(s);
    }

    auto itr = m_registry.get(id);
    CALICO_EXPECT_NE(itr, end(m_registry));
    return do_acquire(itr->value);
}

#undef MAYBE_FORWARD

} // namespace calico
