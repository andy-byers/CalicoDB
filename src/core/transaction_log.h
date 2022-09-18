#ifndef CALICO_CORE_TRANSACTION_LOG_H
#define CALICO_CORE_TRANSACTION_LOG_H

#include <optional>
#include <variant>
#include <vector>
#include "calico/bytes.h"
#include "utils/encoding.h"
#include "utils/types.h"

namespace calico {

struct PageDelta {
    Size offset {};
    Size size {};
};

struct DeltasDescriptor {
    struct Delta {
        Size offset {};
        BytesView data {};
    };

    PageId pid;
    std::vector<Delta> deltas;
};

struct FullImageDescriptor {
    PageId pid;
    BytesView image;
};

struct CommitDescriptor {};

using PayloadDescriptor = std::variant<DeltasDescriptor, FullImageDescriptor, CommitDescriptor>;

[[nodiscard]] auto decode_payload(BytesView in) -> std::optional<PayloadDescriptor>;
[[nodiscard]] auto encode_deltas_payload(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(PageId page_id, BytesView image, Bytes out) -> Size;
[[nodiscard]] auto encode_commit_payload(Bytes out) -> Size;

enum XactPayloadType : Byte {
    COMMIT     = '\xC0',
    DELTAS     = '\xD0',
    FULL_IMAGE = '\xF0',
};

static constexpr Size MINIMUM_PAYLOAD_SIZE {sizeof(XactPayloadType)};

inline auto encode_payload_type(Bytes out, XactPayloadType type) -> void
{
    CALICO_EXPECT_FALSE(out.is_empty());
    out[0] = type;
}

} // namespace calico

#endif // CALICO_CORE_TRANSACTION_LOG_H

