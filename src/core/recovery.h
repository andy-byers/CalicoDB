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

class System;

struct PageDelta {
    Size offset {};
    Size size {};
};

struct DeltasDescriptor {
    struct Delta {
        Size offset {};
        BytesView data {};
    };

    Id pid;
    Id lsn;
    std::vector<Delta> deltas;
};

struct FullImageDescriptor {
    Id pid;
    Id lsn;
    BytesView image;
};

struct CommitDescriptor {
    Id lsn;
};

using PayloadDescriptor = std::variant<DeltasDescriptor, FullImageDescriptor, CommitDescriptor>;

[[nodiscard]] auto decode_payload(WalPayloadOut in) -> std::optional<PayloadDescriptor>;
[[nodiscard]] auto encode_deltas_payload(Id page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(Id page_id, BytesView image, Bytes out) -> Size;
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

    [[nodiscard]] auto start_abort(Id commit_lsn) -> Status;
    [[nodiscard]] auto finish_abort(Id commit_lsn) -> Status;
    [[nodiscard]] auto start_recovery(Id &commit_lsn) -> Status;
    [[nodiscard]] auto finish_recovery(Id commit_lsn) -> Status;

private:
    [[nodiscard]] auto finish_routine(Id commit_lsn) -> Status;

    Pager *m_pager {};
    WriteAheadLog *m_wal {};
};

} // namespace calico

#endif // CALICO_CORE_TRANSACTION_LOG_H

