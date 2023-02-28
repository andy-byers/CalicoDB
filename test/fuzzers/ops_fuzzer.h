#ifndef CALICODB_FUZZERS_OPS_FUZZER_H
#define CALICODB_FUZZERS_OPS_FUZZER_H

#include "fuzzer.h"

namespace calicodb
{

class OpsFuzzer : public DbFuzzer
{
public:
    ~OpsFuzzer() override = default;
    explicit OpsFuzzer(std::string path, Options *options = nullptr);
    [[nodiscard]] auto step(const std::uint8_t *&data, std::size_t &size) -> Status override;
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_OPS_FUZZER_H
