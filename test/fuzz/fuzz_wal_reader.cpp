/*
* Fuzz target that exercises the WAL reader.
*/
#include "fuzzers.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    using Fuzzer = calico::fuzz::WALReaderFuzzer;
    Fuzzer fuzzer {Fuzzer::Transformer{}};

    try {
        fuzzer(data, size);
    } catch (const calico::CorruptionError&) {

    }
    return 0;
}