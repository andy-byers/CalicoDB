#ifndef CALICO_WAL_RECORD_H
#define CALICO_WAL_RECORD_H

#include "calico/storage.h"
#include "pager/delta.h"
#include "utils/encoding.h"
#include "utils/types.h"
#include <algorithm>
#include <map>
#include <variant>

namespace calicodb
{

static constexpr Size WAL_BLOCK_SCALE {4};

[[nodiscard]] inline constexpr auto wal_block_size(Size page_size) -> Size
{
    return std::min(MAXIMUM_PAGE_SIZE, page_size * WAL_BLOCK_SCALE);
}

[[nodiscard]] inline constexpr auto wal_scratch_size(Size page_size) -> Size
{
    const Size DELTA_PAYLOAD_HEADER_SIZE {11};
    return page_size + DELTA_PAYLOAD_HEADER_SIZE + sizeof(PageDelta);
}

[[nodiscard]] inline auto decode_segment_name(const Slice &prefix, const Slice &path) -> Id
{
    if (path.size() <= prefix.size()) {
        return Id::null();
    }
    auto name = path.range(prefix.size());

    // Don't call std::stoul() if it's going to fail.
    const auto is_valid = std::all_of(name.data(), name.data() + name.size(), [](auto c) {
        return std::isdigit(c);
    });

    if (!is_valid) {
        return Id::null();
    }

    return {std::stoull(name.to_string())};
}

[[nodiscard]] inline auto encode_segment_name(const Slice &prefix, Id id) -> std::string
{
    return prefix.to_string() + std::to_string(id.value);
}

enum WalPayloadType : Byte {
    WPT_Delta = '\xD0',
    WPT_FullImage = '\xF0',
};

enum WalRecordType : Byte {
    WRT_Empty = '\x00',
    WRT_Full = '\xA4',
    WRT_First = '\xB3',
    WRT_Middle = '\xC2',
    WRT_Last = '\xD1',
};

/*
 * Header fields associated with each WAL record. Based off of the WAL protocol found in RocksDB.
 */
struct WalRecordHeader {
    static constexpr Size SIZE {7};

    [[nodiscard]] static auto contains_record(const Slice &data) -> bool
    {
        return data.size() > WalRecordHeader::SIZE && data[0] != '\x00';
    }

    WalRecordType type {};
    std::uint16_t size {};
    std::uint32_t crc {};
};

/*
 * Header fields associated with each payload.
 */
struct WalPayloadHeader {
    static constexpr Size SIZE {8};

    Lsn lsn;
};

// Routines for working with WAL records.
auto write_wal_record_header(Span out, const WalRecordHeader &header) -> void;
[[nodiscard]] auto read_wal_record_header(Slice in) -> WalRecordHeader;
[[nodiscard]] auto split_record(WalRecordHeader &lhs, const Slice &payload, Size available_size) -> WalRecordHeader;
[[nodiscard]] auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status;

struct DeltaDescriptor {
    struct Delta {
        Size offset {};
        Slice data {};
    };

    Id pid;
    Lsn lsn;
    std::vector<Delta> deltas;
};

struct FullImageDescriptor {
    Id pid;
    Lsn lsn;
    Slice image;
};

using PayloadDescriptor = std::variant<std::monostate, DeltaDescriptor, FullImageDescriptor>;

class WalPayloadIn
{
public:
    friend class WalWriter;

    WalPayloadIn(Lsn lsn, Span buffer)
        : m_buffer {buffer}
    {
        put_u64(buffer, lsn.value);
    }

    [[nodiscard]] auto lsn() const -> Lsn
    {
        return {get_u64(m_buffer)};
    }

    [[nodiscard]] auto data() const -> Slice
    {
        return m_buffer.range(sizeof(Id));
    }

private:
    Slice m_buffer;
};

class WalPayloadOut
{
public:
    WalPayloadOut() = default;

    explicit WalPayloadOut(const Slice &payload)
        : m_payload {payload}
    {
    }

    [[nodiscard]] auto lsn() const -> Lsn
    {
        return {get_u64(m_payload)};
    }

    [[nodiscard]] auto data() -> Slice
    {
        return m_payload.range(sizeof(Lsn));
    }

private:
    Slice m_payload;
};

[[nodiscard]] auto decode_payload(WalPayloadOut in) -> PayloadDescriptor;
[[nodiscard]] auto encode_deltas_payload(Lsn lsn, Id page_id, const Slice &image, const ChangeBuffer &deltas, Span buffer) -> WalPayloadIn;
[[nodiscard]] auto encode_full_image_payload(Lsn lsn, Id page_id, const Slice &image, Span buffer) -> WalPayloadIn;

/*
 * Stores a collection of WAL segment descriptors and caches their first LSNs.
 */
class WalSet final
{
public:
    WalSet() = default;
    ~WalSet() = default;

    auto add_segment(Id id) -> void
    {
        m_segments.emplace(id, Lsn::null());
    }

    [[nodiscard]] auto first_lsn(Id id) const -> Lsn
    {
        const auto itr = m_segments.find(id);
        if (itr == end(m_segments)) {
            return Lsn::null();
        }

        return itr->second;
    }

    auto set_first_lsn(Id id, Lsn lsn) -> void
    {
        auto itr = m_segments.find(id);
        CALICO_EXPECT_NE(itr, end(m_segments));
        itr->second = lsn;
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_segments.empty();
    }

    [[nodiscard]] auto first() const -> Id
    {
        return m_segments.empty() ? Id::null() : cbegin(m_segments)->first;
    }

    [[nodiscard]] auto last() const -> Id
    {
        return m_segments.empty() ? Id::null() : crbegin(m_segments)->first;
    }

    [[nodiscard]] auto id_before(Id id) const -> Id
    {
        if (m_segments.empty()) {
            return Id::null();
        }

        auto itr = m_segments.lower_bound(id);
        if (itr == cbegin(m_segments)) {
            return Id::null();
        }
        return prev(itr)->first;
    }

    [[nodiscard]] auto id_after(Id id) const -> Id
    {
        auto itr = m_segments.upper_bound(id);
        return itr != cend(m_segments) ? itr->first : Id::null();
    }

    auto remove_before(Id id) -> void
    {
        // Removes segments in [<begin>, id).
        auto itr = m_segments.lower_bound(id);
        m_segments.erase(cbegin(m_segments), itr);
    }

    auto remove_after(Id id) -> void
    {
        // Removes segments in (id, <end>).
        auto itr = m_segments.upper_bound(id);
        m_segments.erase(itr, cend(m_segments));
    }

    [[nodiscard]] auto segments() const -> const std::map<Id, Lsn> &
    {
        return m_segments;
    }

private:
    std::map<Id, Lsn> m_segments;
};

[[nodiscard]] inline auto read_first_lsn(Storage &storage, const std::string &prefix, Id id, WalSet &set, Id &out) -> Status
{
    if (auto lsn = set.first_lsn(id); !lsn.is_null()) {
        out = lsn;
        return Status::ok();
    }

    Reader *temp;
    CALICO_TRY(storage.new_reader(encode_segment_name(prefix, id), &temp));

    char buffer[sizeof(Lsn)];
    Span bytes {buffer, sizeof(buffer)};
    std::unique_ptr<Reader> file {temp};

    // Read the first LSN. If it exists, it will always be at the same location: right after the first
    // record header, which is written at offset 0.
    auto read_size = bytes.size();
    CALICO_TRY(file->read(bytes.data(), read_size, WalRecordHeader::SIZE));

    bytes.truncate(read_size);

    if (bytes.is_empty()) {
        return Status::corruption("segment is empty");
    }
    if (bytes.size() != WalPayloadHeader::SIZE) {
        return Status::corruption("incomplete block");
    }
    const Lsn lsn {get_u64(bytes)};
    if (lsn.is_null()) {
        return Status::corruption("lsn is null");
    }

    set.set_first_lsn(id, lsn);
    out = lsn;
    return Status::ok();
}

} // namespace calicodb

#endif // CALICO_WAL_RECORD_H