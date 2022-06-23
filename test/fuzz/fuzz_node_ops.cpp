/*
* Fuzz target that exercises node operations.
*/
#include "fuzzers.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
   using Fuzzer = calico::fuzz::NodeOpsFuzzer;
   Fuzzer fuzzer {Fuzzer::Transformer {}};
   fuzzer(data, size);
   return 0;
}
