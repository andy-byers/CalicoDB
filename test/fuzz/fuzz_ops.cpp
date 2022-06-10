#include "fuzzers.h"
#include "validators.h"

using namespace cub;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz::OperationFuzzer fuzzer;
    fuzzer.fuzzer_action(data, size);
    fuzzer.fuzzer_validation();
    return 0;
}
