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
struct Id;
class WalReader;

class WalPayloadIn {
public:
    WalPayloadIn(Id lsn, Scratch buffer)
        : m_buffer {buffer}
    {
        put_u64(*buffer, lsn.value);
    }

    [[nodiscard]]
    auto lsn() const -> Id
    {
        return Id {get_u64(*m_buffer)};
    }

    [[nodiscard]]
    auto data() -> Bytes
    {
        return m_buffer->range(sizeof(Id));
    }

    [[nodiscard]]
    auto raw() -> Slice
    {
        return *m_buffer;
    }

    auto shrink_to_fit(Size size) -> void
    {
        m_buffer->truncate(size + sizeof(Id));
    }

private:
    Scratch m_buffer;
};

class WalPayloadOut {
public:
    WalPayloadOut() = default;

    explicit WalPayloadOut(Slice payload)
        : m_payload {payload}
    {}

    [[nodiscard]]
    auto lsn() const -> Id
    {
        return Id {get_u64(m_payload)};
    }

    [[nodiscard]]
    auto data() -> Slice
    {
        return m_payload.range(sizeof(Id));
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
    [[nodiscard]] virtual auto roll_forward(Id begin_lsn, const Callback &callback) -> Status = 0;
    [[nodiscard]] virtual auto roll_backward(Id end_lsn, const Callback &callback) -> Status = 0;
    [[nodiscard]] virtual auto start_workers() -> Status = 0;

    // Since we're using callbacks to traverse the log, we need a second phase to
    // remove obsolete segments. This gives us a chance to flush the pages that
    // were made dirty while traversing.
    [[nodiscard]] virtual auto truncate(Id lsn) -> Status = 0;

    virtual auto log(WalPayloadIn payload) -> void = 0;
    virtual auto flush() -> void = 0;
    virtual auto advance() -> void = 0;
    virtual auto cleanup(Id lsn) -> void = 0;
};

} // namespace Calico

#endif // CALICO_WAL_H
