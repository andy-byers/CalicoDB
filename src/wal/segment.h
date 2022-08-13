#ifndef CALICO_WAL_SEGMENT_H
#define CALICO_WAL_SEGMENT_H

#include "basic_wal.h"
#include "utils/types.h"

namespace calico {

struct SegmentNumber: public NullableId<SegmentNumber> {
    using Hash = IndexHash<SegmentNumber>;

    constexpr SegmentNumber() noexcept = default;

    template<class U>
    constexpr explicit SegmentNumber(U u) noexcept
        : value {std::uint64_t(u)}
    {}

    [[nodiscard]]
    static auto from_name(BytesView name) -> SegmentNumber
    {
        auto digits = name.range(4);

        // Don't call std::stoul() if it's going to throw an exception.
        const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});

        if (!is_valid)
            return null();

        return SegmentNumber {std::stoull(btos(digits))};
    }

    [[nodiscard]]
    auto to_name() const -> std::string
    {
        return fmt::format("{}-{:06d}", WAL_PREFIX, value);
    }

    std::uint64_t value {};
};

class LogSegment {
public:

private:

};

} // namespace calico

#endif //CALICO_WAL_SEGMENT_H
