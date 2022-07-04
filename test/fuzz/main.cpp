/*
 * Code is modified from StandaloneFuzzTargetMain.c, from @llvm-mirror/compiler-rt.
 */
#include <vector>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_sinks.h>
#include "calico/calico.h"
#include "file/file.h"
#include "utils/expect.h"

using namespace calico;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t*, size_t);

auto main(int argc, char **argv) -> int
{
    CALICO_EXPECT_GT(argc, 0);
    auto logger = spdlog::stdout_logger_st("main");
    logger->set_pattern("[%H:%M:%S.%e] %v");
    logger->info("StandaloneFuzzTargetMain: running {} input(s)", argc - 1);

    const auto run = [&logger](ReadOnlyFile file, const std::string &path) {
        std::string buffer(file.size(), '\x00');
        read_exact(file, stob(buffer));
        const auto *data = reinterpret_cast<uint8_t*>(buffer.data());

        logger->info("Running \"{}\" ({} B)", path, buffer.size());
        LLVMFuzzerTestOneInput(data, buffer.size());
    };
    Size num_passed {};

    for (Index index {1}; index < static_cast<Index>(argc); index++) {
        const std::string path {argv[index]};
        try {
            run(ReadOnlyFile {path, {}, 0666}, path);
            logger->info("Pass: \"{}\"", path);
            num_passed++;
        } catch (const std::exception &error) {
            logger->error("Fail: \"{}\": {}", path, error.what());
        }
    }
    logger->info("Finished: Passed {}/{} tests", num_passed, argc - 1);
}