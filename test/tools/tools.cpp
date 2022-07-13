
#include "tools.h"
#include "fakes.h"
#include "utils/logging.h"
#include <unordered_set>

namespace calico {

unsigned RecordGenerator::default_seed = 0;

RecordGenerator::RecordGenerator(Parameters param)
    : m_param {param} {}

auto RecordGenerator::generate(Random &random, Size num_records) -> std::vector<Record>
{
    const auto [mks, mvs, spread, is_sequential] = m_param;
    std::vector<Record> records(num_records);

    const auto min_ks = mks < spread ? 1 : mks - spread;
    const auto min_vs = mvs < spread ? 0 : mvs - spread;
    const auto max_ks = mks + spread;
    const auto max_vs = mvs + spread;

    for (auto &[key, value]: records) {
        key = random_string(random, min_ks, max_ks);
        value = random_string(random, min_vs, max_vs);
    }
    if (is_sequential)
        std::sort(begin(records), end(records));
    return records;
}

} // calico