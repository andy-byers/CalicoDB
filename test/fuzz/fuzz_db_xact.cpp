#include "fuzz.h"

using namespace cco;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Options options;
    options.path = "/tmp/calico_fuzz_db_xact__";
    options.page_size = 0x200;
    options.frame_count = 16;
    DatabaseTarget target {basic_xact_instructions, options};
    target.fuzz(BytesView {reinterpret_cast<const Byte*>(data), size});
    return 0;
}