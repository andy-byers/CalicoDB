
#include <filesystem>
#include "fuzzers.h"

auto main(int, const char*[]) -> int
{
    static constexpr auto PREFIX = "/tmp/cub_corpus/";
    using namespace cub;
    fuzz::OperationFuzzer fuzzer;
    std::filesystem::create_directory(PREFIX);

    for (Index n {}; n < 20; ++n)
        fuzzer.generate_seed(PREFIX + std::to_string(n), 1'000);
}