// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BENCHMARKS_UTILS_H
#define CALICODB_BENCHMARKS_UTILS_H

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstdint>
#include <random>
#include <string_view>

namespace calicodb::benchmarks
{

// Modified from LevelDB.
class RandomGenerator
{
private:
    using Engine = std::default_random_engine;

    std::string m_data;
    mutable std::size_t m_pos = 0;
    mutable Engine m_rng; // Not in LevelDB.

public:
    explicit RandomGenerator(std::size_t size)
        : m_data(size, '\0'),
          m_rng(42)
    {
        std::independent_bits_engine<Engine, CHAR_BIT, unsigned char> engine(m_rng);
        std::generate(begin(m_data), end(m_data), std::ref(engine));
    }

    auto Generate(std::size_t len) const -> std::string_view
    {
        if (m_pos + len > m_data.size()) {
            m_pos = 0;
            assert(len < m_data.size());
        }
        m_pos += len;
        return {m_data.data() + m_pos - len, static_cast<std::size_t>(len)};
    }

    // Not in LevelDB.
    auto Next(std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist(0, t_max);
        return dist(m_rng);
    }

    // Not in LevelDB.
    auto Next(std::uint64_t t_min, std::uint64_t t_max) const -> std::uint64_t
    {
        std::uniform_int_distribution<std::uint64_t> dist(t_min, t_max);
        return dist(m_rng);
    }
};

template <std::size_t Length = 16>
static auto numeric_key(std::size_t key, char padding = '0') -> std::string
{
    const auto key_string = std::to_string(key);
    assert(Length >= key_string.size());
    return std::string(Length - key_string.size(), padding) + key_string;
}

} // namespace calicodb::benchmarks

#endif // CALICODB_BENCHMARKS_UTILS_H
