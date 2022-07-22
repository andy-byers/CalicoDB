#include "fuzz.h"
#include <filesystem>

using namespace cco;
namespace fs = std::filesystem;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    // We generally shouldn't access disk during fuzzing since it greatly slows down execution speed.
//    static constexpr auto PATH = "/tmp/calico_fuzz_db_xact__";
//    std::error_code ignore;
//    if (fs::file_size(PATH, ignore) > 0x4000)
//        fs::remove_all(PATH, ignore);

    Options options;
//    options.path = PATH;
    options.page_size = 0x200;
    options.frame_count = 16;
    options.use_xact = true;

    DatabaseTarget target {basic_xact_instructions, options};
    target.fuzz(BytesView {reinterpret_cast<const Byte*>(data), size});
    return 0;
}