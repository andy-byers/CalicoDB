/*
 * Code is modified from StandaloneFuzzTargetMain.c, from @llvm-mirror/compiler-rt.
 */
#include <vector>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include "cub/cub.h"
#include "file/file.h"

using namespace cub;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t*, size_t);

auto main(int argc, char **argv) -> int
{
    fprintf(stderr, "StandaloneFuzzTargetMain: running %d inputs\n", argc - 1);

    for (int i {1}; i < argc; i++) {
        fprintf(stderr, "Running: %s\n", argv[i]);

        ReadOnlyFile file {argv[i], {}, 0666};
        std::string buffer(file.size(), '\x00');
        read_exact(file, _b(buffer));
        const auto *data = reinterpret_cast<uint8_t*>(buffer.data());

        LLVMFuzzerTestOneInput(data, buffer.size());
        fprintf(stderr, "Done:    %s: (%zu bytes)\n", argv[i], buffer.size());
    }
}