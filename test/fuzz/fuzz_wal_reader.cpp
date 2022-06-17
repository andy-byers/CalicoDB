/*
* Fuzz target that exercises the WAL reader.
*/
#include "fuzzers.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    using Fuzzer = cub::fuzz::WALReaderFuzzer;
    Fuzzer fuzzer {Fuzzer::Decoder {}};

    try {
        fuzzer(data, size);
    } catch (const cub::CorruptionError&) {

    }
    return 0;
}