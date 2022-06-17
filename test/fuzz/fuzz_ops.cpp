/*
* Fuzz target that exercises database operations.
*/
#include "fuzzers.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
   using Fuzzer = cub::fuzz::OpsFuzzer;
   Fuzzer fuzzer {Fuzzer::Decoder {}};

   try {
       fuzzer(data, size);
   } catch (const std::invalid_argument&) {

   }
   return 0;
}
