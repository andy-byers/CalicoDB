#ifndef CUB_TEST_FUZZ_FUZZERS_H
#define CUB_TEST_FUZZ_FUZZERS_H

#include <vector>
#include "cub/cub.h"
#include "cub/database.h"

namespace cub::fuzz {

class OperationFuzzer {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Byte CHARACTER_MAP[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    static constexpr Size NUM_CHARS {sizeof(CHARACTER_MAP) / sizeof(CHARACTER_MAP[0])};

    enum Operation: Index {
        INSERT = 0,
        REMOVE = 1,
        COMMIT = 2,
        ABORT = 3,
    };

    OperationFuzzer();
    virtual ~OperationFuzzer() = default;
    auto fuzzer_action(const uint8_t*, Size) -> void;
    auto fuzzer_validation() -> void;
    auto generate_seed(const std::string&, Size) -> void;
    auto set_chance(Operation, Size) -> void;

private:
    auto choose_operation(Index) -> Operation;

    std::vector<Record> m_records;
    Database m_db;
    Size m_chances[4] {80, 10, 5, 5};
};

} // cub::fuzz

#endif // CUB_TEST_FUZZ_FUZZERS_H
