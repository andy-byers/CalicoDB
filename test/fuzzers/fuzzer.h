#ifndef CALICO_FUZZ_FUZZER_H
#define CALICO_FUZZ_FUZZER_H

#include <iostream>
#include "calico/calico.h"
#include "tools.h"

namespace Calico {

static constexpr Size MAX_KEY_SIZE {12};
static constexpr Size MAX_VALUE_SIZE {0x200};
static constexpr Options DB_OPTIONS {
    0x200,
    0x200 * 32,
    {},
    LogLevel::OFF,
    nullptr,
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
    Size actual {1};

    // If possible, use the first byte to denote the payload size.
    if (size > 1) {
        const auto requested = std::min<Size>(data[0], MAX_KEY_SIZE);
        actual = std::min(requested + !requested, size - 1);
        data++;
        size--;
    }
    const Slice payload {reinterpret_cast<const Byte *>(data), actual};
    data += actual;
    size -= actual;
    return payload;
}

static auto extract_value(const std::uint8_t *&data, Size &size)
{
    // Allow zero-length values.
    if (size == 0) {
        return std::string {};
    }
    const auto needed_size = std::min<Size>(size, 2);
    Size result_size;

    if (needed_size == 1) {
        result_size = data[0];
    } else {
        result_size = data[0] << 8 | data[1];
    }
    result_size %= MAX_VALUE_SIZE;
    data += needed_size;
    size -= needed_size;
    return std::string(result_size, 'x');
}

} // namespace Calico

#endif //CALICO_FUZZ_FUZZER_H
