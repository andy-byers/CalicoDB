#ifndef CALICO_FUZZ_FUZZER_H
#define CALICO_FUZZ_FUZZER_H

#include "calico/calico.h"
#include "tools.h"

namespace Calico {

static constexpr Size MAX_KEY_SIZE {12};
static constexpr Size MAX_VALUE_SIZE {255};
static constexpr Options DB_OPTIONS {
    0x200,
    0x200 * 32,
    0x200 * 32,
    {},
    0,
    0,
    LogLevel::OFF,
    {},
    nullptr,
};

static auto extract_payload(const std::uint8_t *&data, Size &size, Size max_size)
{
    if (size == 0) {
        return Slice {};
    }
    Size actual {1};

    // If possible, use the first byte to denote the payload size.
    if (size > 1) {
        const auto requested = std::min<Size>(data[0], max_size);
        actual = std::min(requested + !requested, size - 1);
        data++;
        size--;
    }
    const Slice payload {reinterpret_cast<const Byte *>(data), actual};
    data += actual;
    size -= actual;
    return payload;
}

static auto extract_key(const std::uint8_t *&data, Size &size)
{
    assert(size != 0);
    return extract_payload(data, size, MAX_KEY_SIZE);
}

static auto extract_value(const std::uint8_t *&data, Size &size)
{
    return extract_payload(data, size, MAX_VALUE_SIZE);
}

} // namespace Calico

#endif //CALICO_FUZZ_FUZZER_H
