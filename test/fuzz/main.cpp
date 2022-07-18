/*
* Code is modified from StandaloneFuzzTargetMain.c, from @llvm-mirror/compiler-rt.
*/
#include <vector>
#include <cstdio>
#include <spdlog/sinks/stdout_sinks.h>
#include "calico/calico.h"
#include "storage/file.h"
#include "utils/expect.h"

using namespace cco;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t*, size_t);

auto main(int argc, char **argv) -> int
{
   CCO_EXPECT_GT(argc, 0);
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
   for (Index index {1}; index < static_cast<Index>(argc); index++) {
       const std::string path {argv[index]};
       File file;
       CCO_EXPECT_TRUE(file.open(path, Mode::READ_ONLY, 0666).has_value());
       run(std::move(file));
       logger->info("Pass: \"{}\"", path);
   }
   logger->info("Finished: Passed {} tests", argc - 1);
}