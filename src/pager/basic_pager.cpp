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

auto BasicPager::open(const Parameters &param, BasicPager **out) -> Status
{
    *out = new (std::nothrow) BasicPager {param};
    if (*out == nullptr)
        return Status::system_error("could not allocate pager object: out of memory");

    // Open the database file.
    RandomEditor *temp_file {};
    auto s = param.storage->open_random_editor(param.prefix + DATA_FILENAME, &temp_file);
    std::unique_ptr<RandomEditor> file {temp_file};

    if (s.is_ok()) {
        // Allocate the frames.
        Framer *temp;
        s = Framer::open(
            std::move(file),
            param.page_size,
            param.frame_count,
            &temp);
        (*out)->m_framer.reset(temp);
    }
    // Caller is only responsible for freeing this memory if the returned status is OK.
    if (!s.is_ok())
        delete *out;
    return s;
}

BasicPager::BasicPager(const Parameters &param)
    : m_logger {create_logger(param.log_sink, "pager")},
      m_images {param.images},
      m_scratch {param.scratch},
      m_wal {param.wal},
      m_status {param.status},
      m_has_xact {param.has_xact}
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

auto BasicPager::hit_ratio() const -> double
{
    return m_registry.hit_ratio();
}

auto BasicPager::pin_frame(identifier id, bool &is_fragile) -> Status
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
            *m_status = m_wal->flush();
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

auto BasicPager::make_dirty(PageRegistry::Entry &entry, identifier pid) -> void
{
    entry.dirty_token = m_dirty.insert(pid);
}

auto BasicPager::make_clean(PageRegistry::Entry &entry) -> PageList::Iterator
{
    auto next = m_dirty.remove(*entry.dirty_token);
    entry.dirty_token.reset();
    return next;
}

auto BasicPager::flush() -> Status
{
    static constexpr auto MSG = "could not flush";
    m_logger->info("flushing cached database pages");
    CALICO_EXPECT_EQ(m_framer->ref_sum(), 0);

    for (auto itr = m_dirty.begin(); itr != m_dirty.end(); ) {
        CALICO_EXPECT_TRUE(m_registry.contains(*itr));
        auto &entry = m_registry.get(*itr)->value;
        const auto frame_id = entry.frame_index;
        const auto page_lsn = m_framer->frame_at(frame_id).lsn();
        const auto page_id = *itr;
        const auto in_range = page_id.as_index() < m_framer->page_count();
        auto s = Status::ok();

        itr = make_clean(entry);

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

auto BasicPager::flushed_lsn() const -> identifier
{
    return m_framer->flushed_lsn();
}

auto BasicPager::try_make_available() -> tl::expected<bool, Status>
{
    identifier page_id;
    auto evicted = m_registry.evict([&](auto id, auto entry) {
        const auto &frame = m_framer->frame_at(entry.frame_index);
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

    auto &[frame_id, record_lsn, dirty_token] = *evicted;
    auto s = Status::ok();
    if (dirty_token) {
        s = m_framer->write_back(frame_id);
        make_clean(*evicted);
    }
    m_framer->unpin(frame_id);
    if (!s.is_ok()) return Err {s};
    return true;
}

auto BasicPager::release(Page page) -> Status
{
    // This method can only fail for writable pages.
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_GT(m_framer->ref_sum(), 0);

    auto s = Status::ok();
    if (const auto deltas = page.collect_deltas(); m_wal->is_working() && !deltas.empty()) {
        CALICO_EXPECT_TRUE(page.is_writable());
        const auto lsn = m_wal->current_lsn();
        page.set_lsn(lsn);

        WalPayloadIn payload {lsn, m_scratch->get()};
        const auto size = encode_deltas_payload(page.id(), page.view(0), page.collect_deltas(), payload.data());
        payload.shrink_to_fit(size);

        s = m_wal->log(payload);
        save_and_forward_status(s, "could not write page deltas to WAL");
    }

    CALICO_EXPECT_TRUE(m_registry.contains(page.id()));
    const auto &entry = m_registry.get(page.id())->value;
    m_framer->unref(entry.frame_index, page);
    return s;
}

auto BasicPager::watch_page(Page &page, PageRegistry::Entry &entry) -> void
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_framer->ref_sum(), 0);

    // Make sure this page is in the dirty list.
    if (!entry.dirty_token)
        make_dirty(entry, page.id());

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
    m_framer->save_state(header);
}

auto BasicPager::load_state(const FileHeader &header) -> void
{
    m_logger->info("loading header fields");
    m_framer->load_state(header);
}

auto BasicPager::allocate() -> tl::expected<Page, Status>
{
    return acquire(identifier::from_index(m_framer->page_count()), true);
}

auto BasicPager::acquire(identifier id, bool is_writable) -> tl::expected<Page, Status>
{
    using std::end;

    const auto do_acquire = [this, is_writable](auto &entry) {
        auto page = m_framer->ref(entry.frame_index, *this, is_writable);

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
        return Err {*m_status};

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
            return Err {s};
        }
        s = m_wal->worker_status();
        if (!s.is_ok()) return Err {s};
    }

    auto itr = m_registry.get(id);
    CALICO_EXPECT_NE(itr, end(m_registry));
    return do_acquire(itr->value);
}

#undef MAYBE_FORWARD

} // namespace calico
