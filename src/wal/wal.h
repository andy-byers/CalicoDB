#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include "calico/bytes.h"
#include "calico/status.h"
#include "utils/encoding.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include <functional>
#include <variant>
#include <vector>

namespace calico {

struct FileHeader;
struct identifier;
class WalReader;

// The main thread will block after the worker has queued up this number of write requests.
static constexpr Size WORKER_CAPACITY {16};

class WalPayloadIn {
public:
    WalPayloadIn(identifier lsn, Scratch buffer)
        : m_buffer {buffer}
    {
        put_u64(*buffer, lsn.value);
    }

    [[nodiscard]]
    auto lsn() const -> identifier
    {
        return identifier {get_u64(*m_buffer)};
    }

    [[nodiscard]]
    auto data() -> Bytes
    {
        return m_buffer->range(sizeof(identifier));
    }

    [[nodiscard]]
    auto raw() -> BytesView
    {
        return *m_buffer;
    }

    auto shrink_to_fit(Size size) -> void
    {
        m_buffer->truncate(size + sizeof(identifier));
    }

private:
    Scratch m_buffer;
};

class WalPayloadOut {
public:
    WalPayloadOut() = default;

    explicit WalPayloadOut(BytesView payload)
        : m_payload {payload}
    {}

    [[nodiscard]]
    auto lsn() const -> identifier
    {
        return identifier {get_u64(m_payload)};
    }

    [[nodiscard]]
    auto data() -> BytesView
    {
        return m_payload.range(sizeof(identifier));
    }

private:
    BytesView m_payload;
};

class WriteAheadLog {
public:
    using Callback = std::function<Status(WalPayloadOut)>;

    virtual ~WriteAheadLog() = default;

    [[nodiscard]]
    virtual auto is_enabled() const -> bool
    {
        return true;
    }

    [[nodiscard]] virtual auto worker_status() const -> Status = 0;
    [[nodiscard]] virtual auto is_working() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> identifier = 0;
    [[nodiscard]] virtual auto current_lsn() const -> identifier = 0;
    [[nodiscard]] virtual auto log(WalPayloadIn payload) -> Status = 0;
    [[nodiscard]] virtual auto flush() -> Status = 0;
    [[nodiscard]] virtual auto advance() -> Status = 0;
    [[nodiscard]] virtual auto start_workers() -> Status = 0;
    [[nodiscard]] virtual auto stop_workers() -> Status = 0;
    [[nodiscard]] virtual auto roll_forward(identifier begin_lsn, const Callback &callback) -> Status = 0;
    [[nodiscard]] virtual auto roll_backward(identifier end_lsn, const Callback &callback) -> Status = 0;

    // Since we're using callbacks to traverse the log, we need a second phase to
    // remove obsolete segments. This gives us a chance to flush the pages that
    // were made dirty while traversing.
    [[nodiscard]] virtual auto remove_after(identifier lsn) -> Status = 0;
    [[nodiscard]] virtual auto remove_before(identifier lsn) -> Status = 0;
};

} // namespace calico

#endif // CALICO_WAL_H
