#include "fuzz.h"
#include <spdlog/fmt/fmt.h>

namespace cco {

auto InstructionParser::parse(BytesView view) const -> std::optional<Parsed>
{
    if (view.is_empty())
        return std::nullopt;

    Index byte = static_cast<unsigned char>(view[0]);
    Size total_size {1};
    Index opcode {};
    view.advance();

    for (const auto [chance, _]: m_instructions) {
        if (byte < chance)
            break;
        byte -= chance;
        opcode++;
    }
    opcode -= opcode == m_instructions.size();
    const auto opsize = m_instructions.at(opcode).num_segments;
    std::vector<BytesView> segments(opsize);

    for (auto &segment: segments) {
        if (view.is_empty())
            return std::nullopt;

        const auto size = static_cast<unsigned char>(view[0]);
        if (view.size() < static_cast<Size>(size + 1))
            return std::nullopt;

        segment = view.range(1, size);
        view.advance(size + 1);
        total_size += size + 1;
    }
    return Parsed {std::move(segments), opcode, total_size};
}

DatabaseTarget::DatabaseTarget(InstructionParser::Instructions instructions, Options options):
      m_parser {std::move(instructions)},
      m_db {options}
{
    CALICO_EXPECT_TRUE(m_db.open().is_ok());
}

auto DatabaseTarget::fuzz(BytesView data) -> void
{
    for (; ; ) {
        auto parsed = m_parser.parse(data);
        if (!parsed)
            break;

        auto [segments, opcode, size] = *parsed;
        data.advance(size);

        switch (opcode) {
            case 0:
                CALICO_EXPECT_EQ(segments.size(), 2);
//                fmt::print("insert: ({}/{} B, {} B)\n", btos(segments[0]).size(), m_db.info().maximum_key_size(), btos(segments[1]).size());
                insert_one(segments[0], segments[1]);
                break;
            case 1:
                CALICO_EXPECT_EQ(segments.size(), 1);
//                fmt::print("erase: {} B\n", btos(segments[0]).size());
                erase_one(segments[0]);
                break;
            case 2:
                CALICO_EXPECT_TRUE(segments.empty());
//                fmt::print("commit: ...\n");
                do_commit();
//                fmt::print("...{} records in database\n", m_db.info().record_count());
                break;
            default:
                CALICO_EXPECT_TRUE(segments.empty());
                CALICO_EXPECT_EQ(opcode, 3);
//                fmt::print("abort: {} records in database before\n", m_db.info().record_count());
                do_abort();
//                fmt::print("...{} records in database after\n", m_db.info().record_count());
        }
    }

    if (auto lhs = m_db.find_minimum(), rhs = lhs; rhs.is_valid() && rhs.increment()) {
        CALICO_EXPECT_TRUE(lhs.is_valid());
        for (; rhs.is_valid(); ) {
            CALICO_EXPECT_LT(lhs.key(), rhs.key());
            if (!rhs.increment())
                break;
            CALICO_EXPECT_TRUE(lhs.increment());
        }
    } else {
        CALICO_EXPECT_EQ(m_db.info().record_count(), 0);
        CALICO_EXPECT_TRUE(lhs.status().is_not_found());
        CALICO_EXPECT_TRUE(rhs.status().is_not_found());
    }
}

auto DatabaseTarget::insert_one(BytesView key, BytesView value) -> void
{
    const auto info = m_db.info();
    const auto n = info.record_count();
    const auto c = m_db.find_exact(key);
    const auto s = m_db.insert(key, value);

    if (s.is_ok()) {
        CALICO_EXPECT_EQ(info.record_count(), n + !c.is_valid());
    } else {
        CALICO_EXPECT_TRUE(s.is_invalid_argument());
    }
}

auto DatabaseTarget::erase_one(BytesView key) -> void
{
    const auto info = m_db.info();
    const auto n = info.record_count();
    auto c = m_db.find(key);
    auto s = m_db.erase(c);

    if (!s.is_ok()) {
        CALICO_EXPECT_TRUE(c.status().is_not_found() or c.status().is_invalid_argument());
        c = m_db.find_minimum();
        s = m_db.erase(c);
    }

    if (n) {
        CALICO_EXPECT_EQ(info.record_count(), n - 1);
        CALICO_EXPECT_TRUE(c.is_valid());
        CALICO_EXPECT_TRUE(s.is_ok());
    } else {
        CALICO_EXPECT_FALSE(c.is_valid());
        CALICO_EXPECT_TRUE(s.is_not_found());
    }
}

auto DatabaseTarget::do_commit() -> void
{
//    const auto s = m_db.commit();
//    CALICO_EXPECT_TRUE(s.is_ok() or s.is_logic_error());
}

auto DatabaseTarget::do_abort() -> void
{
//    const auto s = m_db.abort();
//    CALICO_EXPECT_TRUE(s.is_ok() or s.is_logic_error());
}

} // cco

