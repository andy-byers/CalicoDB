#ifndef CALICO_FUZZERS_OPS_FUZZER_H
#define CALICO_FUZZERS_OPS_FUZZER_H

#include "fuzzer.h"

namespace Calico {

class OpsFuzzer : public DbFuzzer {
public:
    ~OpsFuzzer() override;
    explicit OpsFuzzer(std::string path, Options *options = nullptr);
    [[nodiscard]] auto step(const std::uint8_t *&data, std::size_t &size) -> Status override;
};

} // namespace Calico

#endif // CALICO_FUZZERS_OPS_FUZZER_H
