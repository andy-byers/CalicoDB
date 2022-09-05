/*
* Code is modified from StandaloneFuzzTargetMain.c, from @llvm-mirror/compiler-rt.
*/
#include <vector>
#include <cstdio>
#include "spdlog/sinks/stdout_sinks.h"
#include "calico/calico.h"
#include "storage/file.h"
#include "utils/expect.h"

using namespace calico;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t*, size_t);

auto main(int argc, char **argv) -> int
{
   CALICO_EXPECT_GT(argc, 0);
   auto logger = spdlog::stdout_logger_st("main");
   logger->set_pattern("[%H:%M:%S.%e] %v");
   logger->info("StandaloneFuzzTargetMain: running {} input(s)", argc - 1);

   const auto run = [&logger](File file) {
       std::string buffer(file.size().value(), '\x00');
       read_exact(file, stob(buffer));
       const auto *data = reinterpret_cast<uint8_t*>(buffer.data());

       logger->info("Running \"{}\" ({} B)", file.path(), buffer.size());
       LLVMFuzzerTestOneInput(data, buffer.size());
   };
   Size num_passed {};
   for (Index index {1}; index < static_cast<Index>(argc); index++) {
       const std::string path {argv[index]};
       File file;
       const auto result = file.open(path, Mode::READ_ONLY, 0666);
       if (result.has_value()) {
           run(file);
           num_passed++;
           logger->info("Pass: \"{}\"", path);
       } else {
           logger->info("Cannot open file \"{}\"", path);
       }
   }
   logger->info("Finished: Passed {}/{} tests", num_passed, argc - 1);
}