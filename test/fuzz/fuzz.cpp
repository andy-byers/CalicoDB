#include "fuzz.h"

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
        if (view.size() < size + 1)
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
    CCO_EXPECT_TRUE(m_db.open().is_ok());
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
                CCO_EXPECT_EQ(segments.size(), 2);
                insert_one(segments[0], segments[1]);
                break;
            case 1:
                CCO_EXPECT_EQ(segments.size(), 1);
                erase_one(segments[0]);
                break;
            case 2:
                CCO_EXPECT_TRUE(segments.empty());
                do_commit();
                break;
            default:
                CCO_EXPECT_TRUE(segments.empty());
                CCO_EXPECT_EQ(opcode, 3);
                do_abort();
        }
    }
}

auto DatabaseTarget::insert_one(BytesView key, BytesView value) -> void
{
    const auto info = m_db.info();
    const auto n = info.record_count();
    const auto c = m_db.find_exact(key);
    const auto s = m_db.insert(key, value);

    if (s.is_ok()) {
        CCO_EXPECT_EQ(info.record_count(), n + !c.is_valid());
    } else {
        CCO_EXPECT_TRUE(s.is_invalid_argument());
    }
}

auto DatabaseTarget::erase_one(BytesView key) -> void
{
    const auto info = m_db.info();
    const auto n = info.record_count();
    auto c = m_db.find(key);
    auto s = m_db.erase(c);

    if (!s.is_ok()) {
        CCO_EXPECT_TRUE(c.status().is_not_found() or c.status().is_invalid_argument());
        c = m_db.find_minimum();
        s = m_db.erase(c);
    }

    if (n) {
        CCO_EXPECT_EQ(info.record_count(), n - 1);
        CCO_EXPECT_TRUE(c.is_valid());
        CCO_EXPECT_TRUE(s.is_ok());
    } else {
        CCO_EXPECT_FALSE(c.is_valid());
        CCO_EXPECT_TRUE(s.is_not_found());
    }
}

auto DatabaseTarget::do_commit() -> void
{
    const auto s = m_db.commit();
    CCO_EXPECT_TRUE(s.is_ok() or s.is_logic_error());
}

auto DatabaseTarget::do_abort() -> void
{
    const auto s = m_db.abort();
    CCO_EXPECT_TRUE(s.is_ok() or s.is_logic_error());
}

} // cco

