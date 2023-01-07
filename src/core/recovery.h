#ifndef CALICO_CORE_TRANSACTION_LOG_H
#define CALICO_CORE_TRANSACTION_LOG_H

#include "calico/slice.h"
#include "utils/encoding.h"
#include "utils/system.h"
#include "utils/types.h"
#include "wal/wal.h"
#include <optional>
#include <variant>
#include <vector>

namespace Calico {

struct PageDelta {
    Size offset {};
    Size size {};
};

struct DeltaDescriptor {
    struct Delta {
        Size offset {};
        Slice data {};
    };

    Id pid;
    Id lsn;
    std::vector<Delta> deltas;
};

struct FullImageDescriptor {
    Id pid;
    Id lsn;
    Slice image;
};

struct CommitDescriptor {
    Id lsn;
};

using PayloadDescriptor = std::variant<DeltaDescriptor, FullImageDescriptor, CommitDescriptor>;

[[nodiscard]] auto decode_payload(WalPayloadOut in) -> std::optional<PayloadDescriptor>;
[[nodiscard]] auto encode_deltas_payload(Id page_id, Slice image, const std::vector<PageDelta> &deltas, Bytes out) -> Size;
[[nodiscard]] auto encode_full_image_payload(Id page_id, Slice image, Bytes out) -> Size;
[[nodiscard]] auto encode_commit_payload(Bytes out) -> Size;

enum XactPayloadType : Byte {
    COMMIT     = '\xC0',
    DELTAS     = '\xD0',
    FULL_IMAGE = '\xF0',
};

static constexpr Size MINIMUM_PAYLOAD_SIZE {sizeof(XactPayloadType)};

class Pager;
class WriteAheadLog;

class Recovery {
public:
    Recovery(Pager &pager, WriteAheadLog &wal, System &system)
        : m_pager {&pager},
          m_wal {&wal},
          m_system {&system},
          m_log {system.create_log("recovery")}
    {}

    [[nodiscard]] auto start_abort() -> Status;
    [[nodiscard]] auto finish_abort() -> Status;
    [[nodiscard]] auto start_recovery() -> Status;
    [[nodiscard]] auto finish_recovery() -> Status;

private:
    Pager *m_pager {};
    WriteAheadLog *m_wal {};
    System *m_system {};
    LogPtr m_log;
};

} // namespace Calico

#endif // CALICO_CORE_TRANSACTION_LOG_H

