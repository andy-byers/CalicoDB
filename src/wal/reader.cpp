#include "reader.h"
#include "calico/storage.h"
#include "pager/page.h"
#include "utils/expected.hpp"

namespace Calico {

[[nodiscard]]
static auto read_tail(Reader &file, Size number, Span tail) -> Status
{
    auto temp = tail;
    auto read_size = tail.size();
    Calico_Try_S(file.read(temp.data(), read_size, number * tail.size()));

    if (read_size == 0) {
        return Status::not_found("end of file");
    } else if (read_size != tail.size()) {
        return Status::system_error("incomplete read");
    }
    return Status::ok();
}

WalIterator::WalIterator(Reader &file, Span tail)
    : m_tail {tail},
      m_file {&file}
{}

auto WalIterator::read(Span &payload) -> Status
{
    if (m_block + m_offset == 0) {
        Calico_Try_S(read_tail(*m_file, 0, m_tail));
    }
    auto out = payload;
    WalRecordHeader header;

    for (; ; ) {
        const auto has_enough_space = m_tail.size() > m_offset + WalRecordHeader::SIZE;
        auto rest = m_tail.range(m_offset);

        if (has_enough_space && WalRecordHeader::contains_record(rest)) {
            const auto temp = read_wal_record_header(rest);
            rest.advance(WalRecordHeader::SIZE);

            Calico_Try_S(merge_records_left(header, temp));
            if (temp.size == 0 || temp.size > rest.size()) {
                return Status::corruption("fragment size is invalid");
            }
            mem_copy(out, rest.truncate(temp.size));
            m_offset += WalRecordHeader::SIZE + temp.size;
            out.advance(temp.size);

            if (header.type == WalRecordHeader::Type::FULL) {
                payload.truncate(header.size);
                if (header.crc != crc32c::Value(payload.data(), header.size)) {
                    return Status::corruption("crc is incorrect");
                }
                break;
            }
            if (!rest.is_empty()) {
                continue;
            }
        }
        // Read the next block into the tail buffer.
        Calico_Try_S(read_tail(*m_file, ++m_block, m_tail));
        m_offset = 0;
    }
    return Status::ok();
}

auto WalIterator::offset() const -> Size
{
    return m_offset;
}

auto WalReader::open(const Parameters &param, WalReader **out) -> Status
{
    const auto id = param.set->first();
    if (id.is_null()) {
        return Status::corruption("wal is empty");
    }
    auto *reader = new WalReader;
    reader->m_storage = param.storage;
    reader->m_prefix = param.prefix;
    reader->m_id = param.set->first();
    reader->m_set = param.set;
    reader->m_data = param.data;
    reader->m_tail = param.tail;
    *out = reader;

    return Status::ok();
}

auto WalReader::seek(Lsn lsn) -> Status
{
    m_id = m_set->first();
    while (!m_id.is_null()) {
        // Caches the first LSN of each segment encountered here.
        const auto first = read_first_lsn(*m_storage, m_prefix, m_id, *m_set);
        if (!first.has_value()) {
            return first.error();
        }
        if (*first == lsn) {
            return Status::ok();
        } else if (*first > lsn) {
            if (m_id > m_set->first()) {
                m_id.value--;
                return reopen();
            }
            return Status::not_found("not found");
        }
        Calico_Try_S(skip());
    }
    m_id = m_set->last();
    return Status::not_found("segment does not exist");
}

auto WalReader::skip() -> Status
{
    const auto next_id = m_set->id_after(m_id);
    if (next_id.is_null()) {
        return Status::not_found("end of wal");
    }
    m_id = next_id;
    return reopen();
}

auto WalReader::reopen() -> Status
{
    Reader *file;
    Calico_Try_S(m_storage->new_reader(encode_segment_name(m_prefix, m_id), &file));
    m_file.reset(file);
    m_itr = WalIterator {*m_file, m_tail};
    return Status::ok();
}

auto WalReader::read(WalPayloadOut &payload) -> Status
{
    if (!m_itr.has_value()) {
        Calico_Try_S(reopen());
    }

    auto data = m_data;
    auto s = m_itr->read(data);

    if (s.is_not_found()) {
        Calico_Try_S(skip());
        s = m_itr->read(data);
    }
    if (s.is_ok()) {
        payload = WalPayloadOut {data};
    }
    return s;
}

} // namespace Calico