
// TODO: Use a fake file!
#include <filesystem>
#include <fstream>

#include "cub.h"
#include "file/file.h"
#include "../tools/fakes.h"
#include "../tools/random.h"
#include "../tools/tools.h"

using namespace cub;


auto run_test(BytesView data)
{
    constexpr auto PATH = "/tmp/cub_fuzz_test";
    std::filesystem::remove(PATH);
    {
        WriteOnlyFile file {PATH, Mode::CREATE, 0666};
        write_exact(file, data);
        file.sync();
    }

    Options options;
    options.page_size = 0x100;
    options.block_size = 0x200;
    auto db = Database::open(PATH, options);

    auto cursor = db.get_cursor();
    cursor.find_minimum();

    while (cursor.increment());
}

extern "C" int LLVMFuzzerTestOneInput(const Byte *data, Size size)
{
    puts("hi");
    run_test({data, size});
    return 0;
}