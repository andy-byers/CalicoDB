
#include <filesystem>
#include "fuzzers.h"

auto main(int, const char*[]) -> int
{
    static constexpr auto PREFIX = "/tmp/cub_corpus/";
    using namespace cub;
    fuzz::OperationFuzzer fuzzer;
    std::filesystem::create_directory(PREFIX);

    // TODO: This generates random databases files and WALs. We can use the WALs to seed `fuzz_wal`, but we need
    //       to generate seeds for `fuzz_ops` as well.
    for (Index n {}; n < 20; ++n)
        fuzzer.generate_seed(PREFIX + std::to_string(n), 1'000);
}