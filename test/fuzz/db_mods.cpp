#include "fuzz.h"

using namespace cco;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Options options;
    options.page_size = 0x200;
    options.frame_count = 16;
    options.use_xact = false;
    DatabaseTarget target {basic_modify_instructions, options};
    target.fuzz(BytesView {reinterpret_cast<const Byte*>(data), size});
    return 0;
}