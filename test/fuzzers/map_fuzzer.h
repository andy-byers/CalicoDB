#ifndef CALICO_FUZZERS_MAP_FUZZER_H
#define CALICO_FUZZERS_MAP_FUZZER_H

#include <map>
#include <set>
#include "fuzzer.h"

namespace Calico {

using Set = std::set<std::string>;
using Map = std::map<std::string, std::string>;

class MapFuzzer : public Fuzzer {
public:
    ~MapFuzzer() override = default;
    explicit MapFuzzer(std::string path, Options *options = nullptr);
    auto step(const std::uint8_t *&data, std::size_t &size) -> void override;

private:
    std::string m_path;
    Options m_options;
    Database *m_db {};
};

} // namespace Calico

#endif // CALICO_FUZZERS_MAP_FUZZER_H
