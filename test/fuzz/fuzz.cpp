
#include <filesystem>
#include <fstream>

#include "cub.h"
#include "../tools/random.h"
#include "../tools/tools.h"

using namespace cub;

static constexpr Size PAGE_SIZE = 0x200;

extern "C" int LLVMFuzzerTestOneInput(const Byte *data, Size size)
{
    auto db = Database::temp(PAGE_SIZE);
    db.insert({data, size}, {data, size});
    return 0;
}