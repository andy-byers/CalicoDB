#ifndef CALICO_WAL_RECORD_H
#define CALICO_WAL_RECORD_H

#include "utils/encoding.h"
#include "utils/types.h"
#include "wal.h"
#include "spdlog/fmt/fmt.h"

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

        return SegmentId {std::stoull(digits.to_string())};
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

    auto operator++() -> SegmentId&
    {
        value++;
        return *this;
    }

    auto operator++(int) -> SegmentId
    {
        auto temp = *this;
        ++(*this);
        return temp;
    }

    auto operator--() -> SegmentId&
    {
        CALICO_EXPECT_FALSE(is_null());
        value--;
        return *this;
    }

    auto operator--(int) -> SegmentId
    {
        auto temp = *this;
        --(*this);
        return temp;
    }

    std::uint64_t value {};
};

struct WalRecordHeader {
    enum Type: Byte {
        FULL   = '\xA4',
        FIRST  = '\xB3',
        MIDDLE = '\xC2',
        LAST   = '\xD1',
    };

    [[nodiscard]]
    static auto contains_record(BytesView data) -> bool
    {
        return data.size() > sizeof(WalRecordHeader) && data[6] != '\x00';
    }

    std::uint32_t crc;
    std::uint16_t size;
    Type type;
};

static_assert(sizeof(WalRecordHeader) == 8);

enum WalPayloadType: Byte {
    COMMIT     = '\xFF',
    DELTAS     = '\xEE',
    FULL_IMAGE = '\xDD',
};

// Routines for working with WAL records.
auto write_wal_record_header(Bytes out, const WalRecordHeader &header) -> void;
[[nodiscard]] auto contains_record(BytesView in) -> bool;
[[nodiscard]] auto read_wal_record_header(BytesView in) -> WalRecordHeader;
[[nodiscard]] auto split_record(WalRecordHeader &lhs, BytesView payload, Size available_size) -> WalRecordHeader;
[[nodiscard]] auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status;
[[nodiscard]] auto merge_records_right(const WalRecordHeader &lhs, WalRecordHeader &rhs) -> Status;

// Routines for working with WAL payloads.
[[nodiscard]] auto encode_deltas_payload(SequenceId lsn, PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(SequenceId lsn, PageId page_id, BytesView image, Bytes out) -> Size;
[[nodiscard]] auto encode_commit_payload(SequenceId lsn, Bytes out) -> Size;
[[nodiscard]] auto decode_commit_payload(BytesView in) -> CommitDescriptor;
[[nodiscard]] auto decode_deltas_payload(BytesView in) -> DeltasDescriptor;
[[nodiscard]] auto decode_full_image_payload(BytesView in) -> FullImageDescriptor;

static constexpr Size MINIMUM_PAYLOAD_SIZE {sizeof(WalPayloadType) + sizeof(SequenceId)};

[[nodiscard]]
inline auto decode_payload_type(BytesView in) -> WalPayloadType
{
    CALICO_EXPECT_GE(in.size(), MINIMUM_PAYLOAD_SIZE);
    return WalPayloadType {in[0]};
}

[[nodiscard]]
inline auto decode_lsn(BytesView in) -> SequenceId
{
    CALICO_EXPECT_GE(in.size(), MINIMUM_PAYLOAD_SIZE);
    return SequenceId {get_u64(in.advance())};
}

} // namespace calico

#endif // CALICO_WAL_RECORD_H