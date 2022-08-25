/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CALICO_WAL_RECORD_H
#define CALICO_WAL_RECORD_H

#include "calico/wal.h"
#include "utils/encoding.h"
#include "utils/types.h"

namespace calico {

static constexpr auto WAL_PREFIX = "wal-";
static constexpr Size WAL_SCRATCH_SCALE {3};
static constexpr Size WAL_BLOCK_SCALE {1};

struct SegmentId
    : public NullableId<SegmentId>,
      public EqualityComparableTraits<SegmentId>,
      public OrderableTraits<SegmentId>
{
    static constexpr auto NAME_FORMAT = "{}{:06d}";
    static constexpr Size DIGITS_SIZE {6};
    using Hash = IndexHash<SegmentId>;

    constexpr SegmentId() noexcept = default;

    template<class U>
    constexpr explicit SegmentId(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    [[nodiscard]]
    static auto from_name(BytesView name) -> SegmentId
    {
        if (name.size() < DIGITS_SIZE)
            return null();

        auto digits = name.advance(name.size() - DIGITS_SIZE);

        // Don't call std::stoul() if it's going to throw an exception.
        const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});

        if (!is_valid)
            return null();

        return SegmentId {std::stoull(btos(digits))};
    }

    [[nodiscard]]
    auto to_name() const -> std::string
    {
        return fmt::format(NAME_FORMAT, WAL_PREFIX, value);
    }

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct BlockNumber: public EqualityComparableTraits<SegmentId> {
    using Hash = IndexHash<BlockNumber>;

    constexpr BlockNumber() noexcept = default;

    template<class U>
    constexpr explicit BlockNumber(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct BlockOffset: public EqualityComparableTraits<SegmentId> {
    using Hash = IndexHash<BlockNumber>;

    constexpr BlockOffset() noexcept = default;

    template<class U>
    constexpr explicit BlockOffset(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    constexpr explicit operator std::uint64_t() const
    {
        return value;
    }

    std::uint64_t value {};
};

struct LogPosition {

    [[nodiscard]]
    auto is_start() const -> bool
    {
        return number == 0 && offset == 0;
    }

    BlockNumber number;
    BlockOffset offset;
};

struct RecordPosition {
    SegmentId id;
    LogPosition pos;
};

struct LogSegment {
    SegmentId id;
    SequenceId first_lsn;
};

struct WalRecordHeader {
    enum Type: Byte {
        FULL   = '\xA4',
        FIRST  = '\xB3',
        MIDDLE = '\xC2',
        LAST   = '\xD1',
    };

    std::uint64_t lsn;
    std::uint32_t crc;
    std::uint16_t size;
    Type type;
    Byte pad; ///< Padding byte that should always be zero.
};

// TODO: May need some compiler intrinsics to make this actually true on all platforms...
static_assert(sizeof(WalRecordHeader) == 16);

enum WalPayloadType: Byte {
    COMMIT     = '\xFF',
    DELTAS     = '\xEE',
    FULL_IMAGE = '\xDD',
};

struct WalDeltasHeader {
    std::uint64_t page_id;
    std::uint16_t count;
};

// Routines for working with WAL records.
auto write_wal_record_header(Bytes out, const WalRecordHeader &header) -> void;
[[nodiscard]] auto contains_record(BytesView in) -> bool;
[[nodiscard]] auto read_wal_record_header(BytesView in) -> WalRecordHeader;
[[nodiscard]] auto encode_deltas_payload(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(PageId page_id, BytesView image, Bytes out) -> Size;
auto encode_commit_payload(Bytes in) -> void;
[[nodiscard]] auto decode_commit_payload(const WalRecordHeader&, BytesView in) -> RedoDescriptor;
[[nodiscard]] auto decode_deltas_payload(const WalRecordHeader&, BytesView in) -> RedoDescriptor;
[[nodiscard]] auto decode_full_image_payload(BytesView in) -> UndoDescriptor;
[[nodiscard]] auto split_record(WalRecordHeader &lhs, BytesView payload, Size available_size) -> WalRecordHeader;
[[nodiscard]] auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status;
[[nodiscard]] auto merge_records_right(const WalRecordHeader &lhs, WalRecordHeader &rhs) -> Status;

[[nodiscard]]
inline auto read_payload_type(BytesView payload) -> WalPayloadType
{
    CALICO_EXPECT_FALSE(payload.is_empty());
    return WalPayloadType {payload[0]};
}

} // namespace calico

#endif // CALICO_WAL_RECORD_H