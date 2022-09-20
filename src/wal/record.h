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
        static constexpr Size PREFIX_SIZE {std::char_traits<char>::length(WAL_PREFIX)};

        if (name.size() <= PREFIX_SIZE)
            return null();

        auto digits = name.advance(PREFIX_SIZE);

        // Don't call std::stoul() if it's going to throw an exception.
        const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});

        if (!is_valid)
            return null();

        return SegmentId {std::stoull(digits.to_string())};
    }

    // TODO: Current naming scheme won't work. We'll eventually run out of names... Come up with a scheme for storing segment names!
    [[nodiscard]]
    auto to_name() const -> std::string
    {
        return WAL_PREFIX + std::to_string(value);
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

/*
 * Header fields associated with each WAL record. Based off of the WAL protocol found in RocksDB.
 */
struct WalRecordHeader {
    enum Type: Byte {
        FULL   = '\xA4',
        FIRST  = '\xB3',
        MIDDLE = '\xC2',
        LAST   = '\xD1',
    };

    static constexpr Size SIZE {7};

    [[nodiscard]]
    static auto contains_record(BytesView data) -> bool
    {
        return data.size() > WalRecordHeader::SIZE && data[0] != '\x00';
    }

    Type type {};
    std::uint16_t size {};
    std::uint32_t crc {};
};

/*
 * Header fields associated with each payload.
 */
struct WalPayloadHeader {
    static constexpr Size SIZE {8};

    SequenceId lsn;
};

// Routines for working with WAL records.
auto write_wal_record_header(Bytes out, const WalRecordHeader &header) -> void;
[[nodiscard]] auto read_wal_record_header(BytesView in) -> WalRecordHeader;
[[nodiscard]] auto read_wal_payload_header(BytesView in) -> WalPayloadHeader;
[[nodiscard]] auto split_record(WalRecordHeader &lhs, BytesView payload, Size available_size) -> WalRecordHeader;
[[nodiscard]] auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status;
[[nodiscard]] auto merge_records_right(const WalRecordHeader &lhs, WalRecordHeader &rhs) -> Status;

} // namespace calico

#endif // CALICO_WAL_RECORD_H