#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include "calico/slice.h"
#include "calico/status.h"
#include "utils/encoding.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include <functional>
#include <variant>
#include <vector>

namespace Calico {

struct FileHeader;
class WalReader;

class WalPayloadIn {
public:
    friend class LogWriter;

    WalPayloadIn(Lsn lsn, Span buffer)
        : m_buffer {buffer}
    {
        put_u64(buffer, lsn.value);
    }

    [[nodiscard]]
    auto lsn() const -> Lsn
    {
        return Lsn {get_u64(m_buffer)};
    }

    [[nodiscard]]
    auto data() const -> Slice
    {
        return m_buffer.range(sizeof(Id));
    }

private:
    Slice m_buffer;
};

class WalPayloadOut {
public:
    WalPayloadOut() = default;

    explicit WalPayloadOut(Slice payload)
        : m_payload {payload}
    {}

    [[nodiscard]]
    auto lsn() const -> Lsn
    {
        return {get_u64(m_payload)};
    }

    [[nodiscard]]
    auto data() -> Slice
    {
        return m_payload.range(sizeof(Lsn));
    }

private:
    Slice m_payload;
};

class WriteAheadLog {
public:
    using Ptr = std::unique_ptr<WriteAheadLog>;
    using Callback = std::function<Status(WalPayloadOut)>;

    virtual ~WriteAheadLog() = default;

    [[nodiscard]] virtual auto flushed_lsn() const -> Id = 0;
    [[nodiscard]] virtual auto current_lsn() const -> Id = 0;
    [[nodiscard]] virtual auto bytes_written() const -> Size = 0;
    [[nodiscard]] virtual auto roll_forward(Lsn begin_lsn, const Callback &callback) -> Status = 0;
    [[nodiscard]] virtual auto roll_backward(Lsn end_lsn, const Callback &callback) -> Status = 0;
    [[nodiscard]] virtual auto start_workers() -> Status = 0;

    // Since we're using callbacks to traverse the log, we need a second phase to
    // remove obsolete segments. This gives us a chance to flush the pages that
    // were made dirty while traversing.
    [[nodiscard]] virtual auto truncate(Lsn lsn) -> Status = 0;

    virtual auto log(WalPayloadIn payload) -> void = 0;
    virtual auto flush() -> void = 0;
    virtual auto advance() -> void = 0;
    virtual auto cleanup(Lsn lsn) -> void = 0;
};

} // namespace Calico

#endif // CALICO_WAL_H
