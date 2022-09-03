#include "unit_tests.h"

namespace calico::internal {

std::uint32_t random_seed;

} // namespace calico::internal

int main(int argc, char** argv) {
    namespace cco = calico;

    // Custom parameter prefixes.
    static constexpr cco::BytesView SEED_PREFIX {"--random_seed="};

    for (int i {1}; i < argc; ++i) {
        cco::BytesView arg {argv[i]};

        if (arg.starts_with(SEED_PREFIX)) {
            arg.advance(SEED_PREFIX.size());
            const auto seed = std::stoul(arg.to_string());
            cco::internal::random_seed = static_cast<std::uint32_t>(seed);
        }
    }

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}