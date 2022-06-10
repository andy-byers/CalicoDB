#include <filesystem>
#include "fuzzers.h"
#include "validators.h"
#include "utils/expect.h"
#include "utils/utils.h"
#include "../tools/random.h"
#include "../tools/tools.h"

namespace cub::fuzz {

OperationFuzzer::OperationFuzzer()
    : m_db {Database::temp(PAGE_SIZE)} {}

auto OperationFuzzer::choose_operation(Index value) -> Operation
{
    Index counter {};
    for (const auto chance: m_chances) {
        if (value < chance)
            break;
        value -= chance;
        counter++;
    }
    CUB_EXPECT_LT(counter, 4);
    return static_cast<Operation>(counter);
}

auto OperationFuzzer::fuzzer_action(const uint8_t *data, Size size) -> void
{
    static constexpr Size INFO_SIZE {3};

    for (Index i {}; i + INFO_SIZE <= size; ) {
        // Use the 4 MSBs of the first byte to determine the action.
        const auto operation = choose_operation(data[i] >> 4);
        const Size key_size = (data[i]&0x0F) + 1;
        const Size repeat_size = data[i + 1];
        const Size multiplier = data[i + 2];
        const Size payload_size = key_size + repeat_size;
        i += INFO_SIZE;

        if (i + payload_size > size)
            break;

        std::string key(key_size, ' ');
        for (auto &c: key)
            c = CHARACTER_MAP[data[i++] % NUM_CHARS];

        std::string repeat(repeat_size, ' ');
        for (auto &r: repeat)
            r = CHARACTER_MAP[data[i++] % NUM_CHARS];
        auto value = repeat;
        for (Index m {}; m < multiplier; ++m)
            value += repeat;

        switch (operation) {
            case Operation::INSERT:
                m_db.insert(_b(key), _b(value));
                break;
            case Operation::REMOVE:
                // Remove the first record with a key >= `key`.
                if (const auto record = m_db.lookup(_b(key), false))
                    m_db.remove(_b(record->key));
                break;
            case Operation::COMMIT:
                m_db.commit();
                break;
            default:
                m_db.abort();
        }
    }
}

auto OperationFuzzer::fuzzer_validation() -> void
{
    validate_ordering(m_db);
}

auto OperationFuzzer::generate_seed(const std::string&, Size) -> void
{

}

auto OperationFuzzer::set_chance(Operation operation, Size chance) -> void
{
    const auto index = static_cast<Index>(operation);
    CUB_EXPECT_LT(index, 4);
    m_chances[index] = chance;
}

} // cub::fuzz

// Generate DBs and WALs
//std::filesystem::remove(path);
//std::filesystem::remove(get_wal_path(path));
//
//{
//    Options options;
//    options.page_size = PAGE_SIZE;
//    Random random {static_cast<Random::Seed>(num_records)};
//    auto db = Database::open(path, options);
//    for (Index i {}; i < num_records; ++i) {
//        const auto k = std::to_string(random.next_int(0, 100'000));
//        const auto v = random_string(random, 1, PAGE_SIZE / 2);
//        db.insert(_b(k), _b(v));
//    }
//    db.commit();
//}
//
//// Un-hide the WAL file. We can use it as a seed for fuzzing the WAL.
//std::filesystem::rename(get_wal_path(path), path + ".wal");