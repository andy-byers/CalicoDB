#ifndef CALICO_FUZZ_FUZZER_H
#define CALICO_FUZZ_FUZZER_H

#include <iostream>
#include "calico/calico.h"
#include "utils/encoding.h"
#include "tools.h"

namespace Calico {

static constexpr auto PAGE_SIZE = MINIMUM_PAGE_SIZE;

static constexpr Options DB_OPTIONS {
    PAGE_SIZE,
    PAGE_SIZE * 32,
    {},
    LogLevel::OFF,
    nullptr,
    nullptr,
};

static auto extract_key(const std::uint8_t *&data, Size &size)
{
    assert(size != 0);
    if (size == 1) {
        return Slice {reinterpret_cast<const Byte *>(data), size};
    }
    Size actual {2};
    if (size > 2) {
        const auto requested = std::min<Size>(data[0] << 8 | data[1], PAGE_SIZE * 2);
        actual = std::min(requested + !requested, size - 2);
        data += 2;
        size -= 2;
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
    result_size %= PAGE_SIZE * 2;
    data += needed_size;
    size -= needed_size;

    std::string result(result_size, '_');
    result.append(std::to_string(result_size));
    return result;
}

} // namespace Calico

#endif // CALICO_FUZZ_FUZZER_H
