#include "helpers.h"
#include "writer.h"

namespace calico {

auto SegmentGuard::start() -> Status
{
    CALICO_EXPECT_FALSE(m_writer->is_attached());
    const SegmentId next_id {m_collection->most_recent_id().value + 1};

    AppendWriter *file {};
    const auto path = m_prefix + next_id.to_name();
    auto s = m_store->open_append_writer(path, &file);
    if (s.is_ok()) {
        m_current.id = next_id;
        m_writer->attach(file);
    }
    return s;
}

auto SegmentGuard::is_started() const -> bool
{
    return m_writer->is_attached();
}

SegmentGuard::~SegmentGuard()
{
    if (is_started()) (void)abort();
}

auto SegmentGuard::abort() -> Status
{
    CALICO_EXPECT_TRUE(is_started());

    // If this returns a non-OK status, the file should still be closed and the writer ready to be attached again.
    return m_writer->detach([](auto) {});
}

auto SegmentGuard::finish(bool has_commit) -> Status
{
    CALICO_EXPECT_TRUE(is_started());
    m_current.has_commit = has_commit;
    m_collection->add_segment(m_current);

    return m_writer->detach([this](auto lsn) {
        m_flushed_lsn->store(lsn);
    });
}

} // namespace calico