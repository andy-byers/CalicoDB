#ifndef CCO_BENCHMARKS_BENCH_H
#define CCO_BENCHMARKS_BENCH_H

#include <random>
#include "calico/bytes.h"
#include "calico/common.h"
#include <benchmark/benchmark.h>

namespace cco {

auto next_string(std::default_random_engine &rng, Size size) -> std::string
{
    auto result = std::string(size, '\x00');
    std::uniform_int_distribution<Size> dist {0xFF};
    std::generate_n(result.begin(), size, [&]() -> char {
        return static_cast<char>(dist(rng));
    });
    return result;
}

// Modified from LevelDB.
class RandomGenerator {
public:
    RandomGenerator():
          m_engine {301}
    {
        m_data = next_string(m_engine, 0x400000);
    }

    auto generate(Size size) -> BytesView
    {
        if (m_offset + size > m_data.size()) {
            m_offset = 0;
            shuffle(); // Don't repeat the same records.
            if (size >= m_data.size()) {
                printf("error: size is out of range");
                std::exit(EXIT_FAILURE);
            }
        }
        m_offset += size;
        return stob(m_data).range(m_offset - size, size);
    }

    auto shuffle() -> void
    {
        std::shuffle(begin(m_data), end(m_data), m_engine);
    }

private:
    std::default_random_engine m_engine;
    std::string m_data;
    Index m_offset {};
};

} // cco

#endif // CCO_BENCHMARKS_BENCH_H
