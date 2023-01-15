#ifndef CALICO_BENCHMARKS_H
#define CALICO_BENCHMARKS_H

#include <algorithm>
#include <calico/slice.h>
#include <climits>
#include <random>

namespace Calico {

static constexpr Size DB_PAYLOAD_SIZE {100};
static constexpr auto DB_VALUE =
    "____________________"
    "____________________"
    "____________________"
    "____________________"
    "________";
static constexpr auto DB_VALUE_SIZE = std::char_traits<Byte>::length(DB_VALUE);
static_assert(DB_VALUE_SIZE < DB_PAYLOAD_SIZE);

static constexpr auto DB_KEY_SIZE = DB_PAYLOAD_SIZE - DB_VALUE_SIZE;
static constexpr Size DB_INITIAL_SIZE {10'000};
static constexpr Size DB_BATCH_SIZE {500};


struct State {
    static std::default_random_engine s_rng;
    
    static auto random_int() -> int
    {
        std::uniform_int_distribution<int> dist;
        return dist(s_rng);
    }
};

template<std::size_t Length = DB_KEY_SIZE>
static auto make_key(Size key) -> std::string
{
    auto key_string = std::to_string(key);
    if (key_string.size() == Length) {
        return key_string;
    } else if (key_string.size() > Length) {
        return key_string.substr(0, Length);
    } else {
        return std::string(Length - key_string.size(), '0') + key_string;
    }
}

// Modified from LevelDB.
class RandomGenerator {
private:
    std::string m_data;
    mutable Size m_pos {};
    
public:
    RandomGenerator()
    {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.

        // https://stackoverflow.com/questions/25298585/
        std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> engine {42};
        std::generate(begin(m_data), end(m_data), std::ref(engine));
    }

    auto Generate(int len) const -> Slice
    {
        assert(len >= 0);
        if (m_pos + len > m_data.size()) {
            m_pos = 0;
            assert(len < m_data.size());
        }
        m_pos += len;
        return Slice {m_data.data() + m_pos - len, static_cast<Size>(len)};
    }
};

} // namespace Calico

#endif // CALICO_BENCHMARKS_H
