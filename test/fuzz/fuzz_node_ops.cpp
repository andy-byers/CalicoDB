/*
* Fuzz target that exercises node operations.
*/
#include "fuzzers.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
   using Fuzzer = cub::fuzz::NodeOpsFuzzer;
   Fuzzer fuzzer {Fuzzer::Transformer {}};
   fuzzer(data, size);
   return 0;
}
