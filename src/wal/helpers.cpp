#include "helpers.h"
#include "writer.h"

namespace calico {

auto SegmentGuard::start(WalRecordWriter &writer, WalCollection &collection) -> Status
{
    CALICO_EXPECT_FALSE(writer.is_attached());
    CALICO_EXPECT_FALSE(collection.is_segment_started());
    const SegmentId next_id {collection.most_recent_id().value + 1};

    AppendWriter *file {};
    const auto path = m_prefix + next_id.to_name();
    auto s = m_store->open_append_writer(path, &file);
    if (!s.is_ok()) return s;

    collection.start_segment(next_id);
    writer.attach(file);

    m_collection = &collection;
    m_writer = &writer;
    return Status::ok();
}

SegmentGuard::~SegmentGuard()
{
    if (is_started()) (void)abort();
}

auto SegmentGuard::abort() -> Status
{
    CALICO_EXPECT_TRUE(is_started());

    m_collection->abort_segment();
    m_collection = nullptr;

    auto s = m_writer->detach();
    m_writer = nullptr;
    return s;
}

auto SegmentGuard::finish(bool has_commit) -> Status
{
    CALICO_EXPECT_TRUE(is_started());

    m_collection->finish_segment(has_commit);
    m_collection = nullptr;

    auto s = m_writer->detach();
    m_writer = nullptr;
    return s;
}

} // namespace calico