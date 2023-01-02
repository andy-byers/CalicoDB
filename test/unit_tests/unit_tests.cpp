#include "unit_tests.h"

namespace Calico::internal {

std::uint32_t random_seed;

} // namespace Calico::internal

int main(int argc, char** argv) {
    namespace cco = Calico;

    // Custom parameter prefixes.
    static constexpr cco::BytesView SEED_PREFIX {"--random_seed="};

    for (int i {1}; i < argc; ++i) {
        cco::BytesView arg {argv[i]};

        if (arg.starts_with(SEED_PREFIX)) {
            arg.advance(SEED_PREFIX.size());
            std::uint32_t seed {};
            if (arg == "<random>") {
                seed = static_cast<uint32_t>(rand());
            } else {
                seed = static_cast<uint32_t>(std::stoul(arg.to_string()));
            }
            cco::internal::random_seed = seed;
        }
    }

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}