#include "unit_tests.h"

namespace Calico::UnitTests {

std::uint32_t random_seed;

} // namespace Calico::UnitTests

int main(int argc, char** argv)
{
    namespace Cco = Calico;

    // Custom parameter prefixes.
    static constexpr Cco::Slice SEED_PREFIX {"--random_seed="};

    for (int i {1}; i < argc; ++i) {
        Cco::Slice arg {argv[i]};

        if (arg.starts_with(SEED_PREFIX)) {
            arg.advance(SEED_PREFIX.size());
            std::uint32_t seed;
            if (arg == "<random>") {
                seed = static_cast<uint32_t>(rand());
            } else {
                seed = static_cast<uint32_t>(std::stoul(arg.to_string()));
            }
            Cco::UnitTests::random_seed = seed;
        }
    }

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}