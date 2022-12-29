#ifndef CALICO_CORE_TRANSACTION_LOG_H
#define CALICO_CORE_TRANSACTION_LOG_H

#include "calico/bytes.h"
#include "utils/encoding.h"
#include "utils/types.h"
#include "wal/wal.h"
#include <optional>
#include <variant>
#include <vector>

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

    identifier pid;
    identifier lsn;
    std::vector<Delta> deltas;
};

struct FullImageDescriptor {
    identifier pid;
    identifier lsn;
    BytesView image;
};

struct CommitDescriptor {
    identifier lsn;
};

using PayloadDescriptor = std::variant<DeltasDescriptor, FullImageDescriptor, CommitDescriptor>;

[[nodiscard]] auto decode_payload(WalPayloadOut in) -> std::optional<PayloadDescriptor>;
[[nodiscard]] auto encode_deltas_payload(identifier page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(identifier page_id, BytesView image, Bytes out) -> Size;
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

class Pager;
class WriteAheadLog;

class Recovery {
public:
    Recovery(Pager &pager, WriteAheadLog &wal)
        : m_pager {&pager},
          m_wal {&wal}
    {}

    [[nodiscard]] auto start_abort(identifier commit_lsn) -> Status;
    [[nodiscard]] auto finish_abort(identifier commit_lsn) -> Status;
    [[nodiscard]] auto start_recovery(identifier &commit_lsn) -> Status;
    [[nodiscard]] auto finish_recovery(identifier commit_lsn) -> Status;

private:
    [[nodiscard]] auto finish_routine(identifier commit_lsn) -> Status;

    Pager *m_pager {};
    WriteAheadLog *m_wal {};
};

} // namespace calico

#endif // CALICO_CORE_TRANSACTION_LOG_H

