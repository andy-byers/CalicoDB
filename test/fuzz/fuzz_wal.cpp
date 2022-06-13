#include "cub/cub.h"
#include "../tools/fakes.h"

using namespace cub;
static constexpr Size BLOCK_SIZE {0x200};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, Size size)
{
    auto file = std::make_unique<ReadOnlyMemory>();
    auto backing = file->memory();
    backing.memory() = std::string(reinterpret_cast<const Byte*>(data), size);

    try {
        WALReader reader {std::move(file), BLOCK_SIZE};
        while (reader.increment()) {}
        while (reader.decrement()) {}
    } catch (const CorruptionError&) {

    } catch (const std::exception &error) {
        puts(error.what());
        throw;
    }
    return 0;
}