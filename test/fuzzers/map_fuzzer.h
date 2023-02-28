#ifndef CALICO_FUZZERS_MAP_FUZZER_H
#define CALICO_FUZZERS_MAP_FUZZER_H

#include "fuzzer.h"
#include <map>
#include <set>

namespace calicodb
{

class MapFuzzer : public DbFuzzer
{
public:
    ~MapFuzzer() override = default;
    explicit MapFuzzer(std::string path, Options *options = nullptr);
    auto step(const std::uint8_t *&data, std::size_t &size) -> Status override;

private:
    std::map<std::string, std::string> m_map;
    std::map<std::string, std::string> m_added;
    std::set<std::string> m_erased;
};

} // namespace calicodb

#endif // CALICO_FUZZERS_MAP_FUZZER_H
