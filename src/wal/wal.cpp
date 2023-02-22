#include "wal.h"
#include "cleanup.h"
#include "reader.h"
#include "utils/logging.h"
#include "writer.h"

namespace Calico {

WriteAheadLog::WriteAheadLog(const Parameters &param)
    : m_prefix {param.prefix},
      m_storage {param.store},
      m_tail(wal_block_size(param.page_size), '\x00'),
      m_segment_cutoff {param.segment_cutoff}
{
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_segment_cutoff, 0);
}

auto WriteAheadLog::open(const Parameters &param, WriteAheadLog **out) -> Status
{
    auto path = param.prefix;
    if (auto pos = path.rfind('/'); pos != std::string::npos) {
        path.erase(pos + 1);
    }

    std::vector<std::string> child_names;
    Calico_Try(param.store->get_children(path, child_names));

    std::vector<Id> segment_ids;
    for (auto &name: child_names) {
        name.insert(0, path);
        if (Slice {name}.starts_with(param.prefix)) {
            Size file_size;
            Calico_Try(param.store->file_size(name, file_size));
            if (file_size) {
                segment_ids.emplace_back(decode_segment_name(param.prefix, name));
            }
        }
    }
    std::sort(begin(segment_ids), end(segment_ids));

    auto *wal = new (std::nothrow) WriteAheadLog {param};
    if (wal == nullptr) {
        return Status::system_error("out of memory");
    }

    // Keep track of the segment files.
    for (const auto &id: segment_ids) {
        wal->m_set.add_segment(id);
    }
    *out = wal;
    return Status::ok();
}

auto WriteAheadLog::close() -> Status
{
    m_cleanup.reset();
    if (m_writer) {
        std::move(*m_writer).destroy();
        m_writer.reset();
    }
    return status();
}

auto WriteAheadLog::start_writing() -> Status
{
    m_writer = std::unique_ptr<WalWriter> {
        new(std::nothrow) WalWriter {{
            m_prefix,
            m_tail,
            m_storage,
            &m_error,
            &m_set,
            m_segment_cutoff,
        }}};
    if (m_writer == nullptr) {
        return Status::system_error("cannot allocate writer object: out of memory");
    }

    m_cleanup = std::unique_ptr<WalCleanup> {
        new(std::nothrow) WalCleanup {{
            m_prefix,
            m_storage,
            &m_error,
            &m_set,
        }}};
    if (m_cleanup == nullptr) {
        return Status::system_error("cannot allocate cleanup object: out of memory");
    }

    return Status::ok();
}

auto WriteAheadLog::flushed_lsn() const -> Lsn
{
    if (m_writer) {
        m_flushed_lsn = m_writer->flushed_lsn();
    }
    return m_flushed_lsn;
}

auto WriteAheadLog::current_lsn() const -> Lsn
{
    return {m_last_lsn.value + 1};
}

auto WriteAheadLog::log(WalPayloadIn payload) -> void
{
    CALICO_EXPECT_NE(m_writer, nullptr);

    m_last_lsn.value++;
    CALICO_EXPECT_EQ(payload.lsn(), m_last_lsn);

    m_bytes_written += payload.data().size() + sizeof(Lsn);
    m_writer->write(payload);
}

auto WriteAheadLog::flush() -> Status
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_writer->flush();
    return m_error.get();
}

auto WriteAheadLog::advance() -> void
{
    CALICO_EXPECT_NE(m_writer, nullptr);
    m_writer->advance();
}

auto WriteAheadLog::cleanup(Lsn recovery_lsn) -> void
{
    m_cleanup->cleanup(recovery_lsn);
}

} // namespace Calico