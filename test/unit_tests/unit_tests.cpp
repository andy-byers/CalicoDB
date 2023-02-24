#include "unit_tests.h"

namespace Calico {

unsigned RecordGenerator::default_seed = 0;

RecordGenerator::RecordGenerator(Parameters param)
    : m_param {param}
{}

auto RecordGenerator::generate(Tools::RandomGenerator &random, Size num_records) const -> std::vector<Record>
{
    const auto [mks, mvs, spread, is_sequential, is_unique] = m_param;
    std::vector<Record> records(num_records);

    const auto min_ks = mks < spread ? 1 : mks - spread;
    const auto min_vs = mvs < spread ? 0 : mvs - spread;
    const auto max_ks = mks + spread;
    const auto max_vs = mvs + spread;
    auto itr = records.begin();
    std::unordered_set<std::string> set;
    Size num_collisions {};

    while (itr != records.end()) {
        const auto ks = random.Next<Size>(min_ks, max_ks);
        auto key = random.Generate(ks).to_string();
        if (is_sequential) {
            if (set.find(key) != end(set)) {
                CALICO_EXPECT_LT(num_collisions, num_records);
                num_collisions++;
                continue;
            }
            set.emplace(key);
        }
        const auto vs = random.Next<Size>(min_vs, max_vs);
        itr->key = std::move(key);
        itr->value = random.Generate(vs).to_string();
        itr++;
    }
    if (is_sequential) {
        std::sort(begin(records), end(records));
    }
    return records;
}

} // namespace Calico

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}