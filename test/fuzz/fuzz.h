#ifndef CALICO_FUZZ_FUZZ_H
#define CALICO_FUZZ_FUZZ_H

#include <array>
#include "calico/calico.h"
#include "utils/expect.h"

namespace calico {

class InstructionParser {
public:
    struct Parsed {
        std::vector<BytesView> segments;
        Index opcode {};
        Size size {};
    };

    struct Instruction {
        Size chance {};
        Size num_segments {};
    };

    using Instructions = std::vector<Instruction>;

    explicit InstructionParser(Instructions instructions):
          m_instructions {std::move(instructions)} {}

    [[nodiscard]] auto parse(BytesView) const -> std::optional<Parsed>;

private:
    std::vector<Instruction> m_instructions;
};

const InstructionParser::Instructions basic_modify_instructions {
    // insert, remove
    {200, 2},
    {0, 1},
};

const InstructionParser::Instructions basic_xact_instructions {
    // insert, remove, commit, abort
    {150, 2},
    {50, 1},
    {30, 0},
    {0, 0},
};

class DatabaseTarget {
public:
    explicit DatabaseTarget(InstructionParser::Instructions, Options);
    auto fuzz(BytesView data) -> void;

private:
    auto insert_one(BytesView key, BytesView value) -> void;
    auto erase_one(BytesView key) -> void;
    auto do_commit() -> void;
    auto do_abort() -> void;

    InstructionParser m_parser;
    Database m_db;
};

} // cco


#endif // CALICO_FUZZ_FUZZ_H
