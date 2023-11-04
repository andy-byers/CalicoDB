// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/config.h"
#include "common.h"
#include "json.h"
#include "test.h"
#include <iomanip>

namespace calicodb::test
{

using namespace json;

static auto integer_str(int64_t i) -> std::string
{
    return "<integer=" + std::to_string(i) + '>';
}

static auto real_str(double r) -> std::string
{
    return "<real=" + std::to_string(r) + '>';
}

class TestHandler : public Handler
{
public:
    std::vector<std::string> records;
    std::string current;
    uint32_t open_objects = 0;
    uint32_t closed_objects = 0;
    uint32_t open_arrays = 0;
    uint32_t closed_arrays = 0;

    explicit TestHandler() = default;
    ~TestHandler() override = default;

    [[nodiscard]] auto accept_key(const Slice &value) -> bool override
    {
        current = value.to_string() + ':';
        return true;
    }

    [[nodiscard]] auto accept_string(const Slice &value) -> bool override
    {
        records.push_back(current + value.to_string());
        current.clear();
        return true;
    }

    [[nodiscard]] auto accept_integer(int64_t value) -> bool override
    {
        records.push_back(current + integer_str(value));
        current.clear();
        return true;
    }

    [[nodiscard]] auto accept_real(double value) -> bool override
    {
        records.push_back(current + real_str(value));
        current.clear();
        return true;
    }

    [[nodiscard]] auto accept_boolean(bool value) -> bool override
    {
        records.push_back(current + (value ? "<true>" : "<false>"));
        current.clear();
        return true;
    }

    [[nodiscard]] auto accept_null() -> bool override
    {
        records.push_back(current + "<null>");
        current.clear();
        return true;
    }

    [[nodiscard]] auto begin_object() -> bool override
    {
        ++open_objects;
        records.push_back(current + "<object>");
        current.clear();
        return true;
    }

    [[nodiscard]] auto end_object() -> bool override
    {
        if (!current.empty()) {
            records.push_back(current);
            current.clear();
        }
        records.emplace_back("</object>");
        ++closed_objects;
        return true;
    }

    [[nodiscard]] auto begin_array() -> bool override
    {
        ++open_arrays;
        records.push_back(current + "<array>");
        current.clear();
        return true;
    }

    [[nodiscard]] auto end_array() -> bool override
    {
        if (!current.empty()) {
            records.push_back(current);
            current.clear();
        }
        records.emplace_back("</array>");
        ++closed_arrays;
        return true;
    }
};

class ReaderTests : public testing::Test
{
public:
    TestHandler m_handler;

    ~ReaderTests() override = default;

    auto reset_test_state()
    {
        m_handler.records.clear();
        m_handler.current.clear();
        m_handler.open_objects = 0;
        m_handler.closed_objects = 0;
        m_handler.open_arrays = 0;
        m_handler.closed_arrays = 0;
    }

    auto run_example_test(const std::vector<std::string> &target, size_t num_objects, size_t num_arrays, const Slice &input)
    {
        reset_test_state();
        Reader reader(m_handler);
        ASSERT_OK(reader.read(input));
        ASSERT_EQ(m_handler.records, target);
        ASSERT_EQ(m_handler.open_objects, num_objects);
        ASSERT_EQ(m_handler.closed_objects, num_objects);
        ASSERT_EQ(m_handler.open_arrays, num_arrays);
        ASSERT_EQ(m_handler.closed_arrays, num_arrays);
    }

    auto assert_ok(const Slice &input, const std::vector<std::string> &target)
    {
        reset_test_state();
        Reader reader(m_handler);
        ASSERT_OK(reader.read(input));
        ASSERT_EQ(m_handler.open_objects, m_handler.closed_objects);
        ASSERT_EQ(m_handler.open_arrays, m_handler.closed_arrays);
        ASSERT_EQ(m_handler.records, target);
    }

    auto assert_corrupted(const Slice &input)
    {
        reset_test_state();
        Reader reader(m_handler);
        const auto s = reader.read(input);
        ASSERT_TRUE(s.is_corruption()) << input.to_string();
    }
};

// Just objects and strings
TEST_F(ReaderTests, Example1)
{
    const std::vector<std::string> target = {
        "<object>", // Toplevel bucket
        "browsers:<object>",
        "firefox:<object>",
        "name:Firefox",
        "pref_url:about:config",
        "releases:<object>",
        "1:<object>",
        "release_date:2004-11-09",
        "status:retired",
        "engine:Gecko",
        "engine_version:1.7",
        "</object>",
        "</object>",
        "</object>",
        "</object>",
        "</object>"};

    // Example from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON
    // with whitespace stripped.
    run_example_test(target, 5, 0, R"({"browsers":{"firefox":{"name":"Firefox","pref_url":"about:config","releases":{"1":{"release_date":"2004-11-09","status":"retired","engine":"Gecko","engine_version":"1.7"}}}}})");

    // Original text.
    run_example_test(target, 5, 0, R"({
  "browsers": {
    "firefox": {
      "name": "Firefox",
      "pref_url": "about:config",
      "releases": {
        "1": {
          "release_date": "2004-11-09",
          "status": "retired",
          "engine": "Gecko",
          "engine_version": "1.7"
        }
      }
    }
  }
})");
}

static constexpr const char *kExample2 = R"([
{
        "id": "0001",
        "type": "donut",
        "name": "Cake",
        "ppu": 0.55,
        "batters":
                {
                        "batter":
                                [
                                        { "id": "1001", "type": "Regular" },
                                        { "id": "1002", "type": "Chocolate" },
                                        { "id": "1003", "type": "Blueberry" },
                                        { "id": "1004", "type": "Devil's Food" }
                                ]
                },
        "topping":
                [
                        { "id": "5001", "type": "None" },
                        { "id": "5002", "type": "Glazed" },
                        { "id": "5005", "type": "Sugar" },
                        { "id": "5007", "type": "Powdered Sugar" },
                        { "id": "5006", "type": "Chocolate with Sprinkles" },
                        { "id": "5003", "type": "Chocolate" },
                        { "id": "5004", "type": "Maple" }
                ]
}
])";

static const std::vector<std::string> s_example_target_2 = {
    "<array>",
    "<object>",
    "id:0001",
    "type:donut",
    "name:Cake",
    "ppu:" + real_str(0.55),
    "batters:<object>",
    "batter:<array>",
    "<object>",
    "id:1001",
    "type:Regular",
    "</object>",
    "<object>",
    "id:1002",
    "type:Chocolate",
    "</object>",
    "<object>",
    "id:1003",
    "type:Blueberry",
    "</object>",
    "<object>",
    "id:1004",
    "type:Devil's Food",
    "</object>",
    "</array>",
    "</object>",
    "topping:<array>",
    "<object>",
    "id:5001",
    "type:None",
    "</object>",
    "<object>",
    "id:5002",
    "type:Glazed",
    "</object>",
    "<object>",
    "id:5005",
    "type:Sugar",
    "</object>",
    "<object>",
    "id:5007",
    "type:Powdered Sugar",
    "</object>",
    "<object>",
    "id:5006",
    "type:Chocolate with Sprinkles",
    "</object>",
    "<object>",
    "id:5003",
    "type:Chocolate",
    "</object>",
    "<object>",
    "id:5004",
    "type:Maple",
    "</object>",
    "</array>",
    "</object>",
    "</array>"};

TEST_F(ReaderTests, Example2)
{

    // Example 5 from https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html,
    // shortened, with whitespace stripped.
    run_example_test(s_example_target_2, 13, 3, kExample2);
}

TEST_F(ReaderTests, ValidInput)
{
    // Single value
    assert_ok("1", {integer_str(1)});
    assert_ok(R"("")", {""});
    assert_ok("true", {"<true>"});
    assert_ok("false", {"<false>"});
    assert_ok("null", {"<null>"});
    assert_ok("42", {integer_str(42)});
    assert_ok("12.3", {real_str(12.3)});

    // Compound value
    assert_ok(R"({})", {"<object>", "</object>"});
    assert_ok(R"( {/*
                   */} )",
              {"<object>", "</object>"});
    assert_ok(R"({"":""})", {"<object>", ":", "</object>"});
    assert_ok(R"({"k":"v"})", {"<object>", "k:v", "</object>"});
    assert_ok(R"([])", {"<array>", "</array>"});
    assert_ok(R"( [/*
                   */] )",
              {"<array>", "</array>"});
    assert_ok(R"([""])", {"<array>", "", "</array>"});
    assert_ok(R"(["v"])", {"<array>", "v", "</array>"});
}

TEST_F(ReaderTests, OnlyAllowsSingleValue)
{
    assert_corrupted(R"(0, 1)");
    assert_corrupted(R"([], {})");
    assert_corrupted(R"({}, [])");
    assert_corrupted(R"([0], {})");
    assert_corrupted(R"({}, [0])");
    assert_corrupted(R"([0, 1], {})");
    assert_corrupted(R"({}, [0, 1])");
}

TEST_F(ReaderTests, TrailingCommasAreNotAllowed)
{
    // Single value
    assert_corrupted(R"("",)");
    assert_corrupted(R"(true,)");
    assert_corrupted(R"(false,)");
    assert_corrupted(R"(null,)");
    assert_corrupted(R"(42,)");

    // Compound values
    assert_corrupted(R"({},)");
    assert_corrupted(R"([],)");
    assert_corrupted(R"({"k": "v"},)");
    assert_corrupted(R"(["v"],)");
    assert_corrupted(R"({"k": "v",})");
    assert_corrupted(R"(["v",])");

    assert_corrupted(R"({"k1":"v1","k2":2,})");
    assert_corrupted(R"(["v1",2,])");
}

TEST_F(ReaderTests, HandlesMissingQuotes)
{
    assert_corrupted(R"({"k:"v"})");
    assert_corrupted(R"({k":"v"})");
    assert_corrupted(R"({"k":"v})");
    assert_corrupted(R"({"k":v"})");
    assert_corrupted(R"(["v])");
    assert_corrupted(R"([v"])");
}

TEST_F(ReaderTests, HandlesMissingSeparators)
{
    assert_corrupted(R"({"k""v"})");
    assert_corrupted(R"({"k1":"v1""k2":2})");
    assert_corrupted(R"({"k1":"v1","k2"2})");
    assert_corrupted(R"(["1""2"])");
    assert_corrupted(R"(["1"2])");
    assert_corrupted(R"([1"2"])");
    assert_corrupted(R"([1,"2"3])");
    assert_corrupted(R"([1,2"3"])");
}

TEST_F(ReaderTests, HandlesExcessiveNesting)
{
    std::string input;
    for (int i = 0; i < 50'000; ++i) {
        input.append(R"({"a":)");
    }
    // No need to close objects: the parser should exceed the maximum allowed object
    // nesting way before it gets that far.
    assert_corrupted(input);
}

TEST_F(ReaderTests, InvalidInput1)
{
    assert_corrupted(R"()");
    assert_corrupted(R"( )");
    assert_corrupted(R"({)");
    assert_corrupted(R"(})");
    assert_corrupted(R"([)");
    assert_corrupted(R"(])");
    assert_corrupted(R"(:)");
    assert_corrupted(R"(,)");
    assert_corrupted(R"(")");
    assert_corrupted(R"(a)");
}

TEST_F(ReaderTests, InvalidInput2)
{
    assert_corrupted(R"(,[])");
    assert_corrupted(R"(,{})");
    assert_corrupted(R"({"k"})");
    assert_corrupted(R"({"k":})");
    assert_corrupted(R"({:"v"})");
    assert_corrupted(R"({"k": "v",})");
}

TEST_F(ReaderTests, InvalidInput3)
{
    assert_corrupted(R"([[null]]abc)");
    assert_corrupted(R"({{"k":"v"})");
    assert_corrupted(R"({"k":"v"}})");
    assert_corrupted(R"([true)");
    assert_corrupted(R"(null])");
    assert_corrupted(R"([["v"])");
    assert_corrupted(R"(["v"]])");
}

TEST_F(ReaderTests, SkipsComments1)
{
    assert_ok(R"({/*comment*/})",
              {"<object>", "</object>"});
    assert_ok(R"({/*
                    comment
                           */})",
              {"<object>", "</object>"});
    assert_ok(R"(/*comment*/{})",
              {"<object>", "</object>"});
    assert_ok(R"({}/*comment*/)",
              {"<object>", "</object>"});
    assert_ok(R"({ /*c/o*m/m*e/n*t*/ })",
              {"<object>", "</object>"});
}

TEST_F(ReaderTests, SkipsComments2)
{
    assert_ok(R"({"k"/*the key*/: "v" /*the value*/})",
              {"<object>", "k:v", "</object>"});
    assert_ok(R"({"k"/*the*/ /*key*/: "v" /*the*//*value*/})",
              {"<object>", "k:v", "</object>"});
    assert_ok(R"(/*the*/{/*key*/"k":"v"/*the*/}/*value*/)",
              {"<object>", "k:v", "</object>"});
}

TEST_F(ReaderTests, InvalidComments)
{
    assert_corrupted(R"({/})");
    assert_corrupted(R"({/*})");
    assert_corrupted(R"({/**})");
    assert_corrupted(R"({/*comment*})");
}

TEST_F(ReaderTests, InvalidLiterals)
{
    for (const auto *literal : {"true", "false", "null"}) {
        for (size_t i = 1, n = std::strlen(literal); i < n; ++i) {
            char buffer[5];
            std::strncpy(buffer, literal, i);
            assert_corrupted(Slice(buffer, i));
        }
    }
}

TEST_F(ReaderTests, ControlCharacters)
{
    assert_ok("\"\x7F\"", {"\x7F"});
    assert_corrupted("\"\x0A\"");
}

TEST_F(ReaderTests, ValidEscapes)
{
    assert_ok(R"(["\/"])", {"<array>", "/", "</array>"});
    assert_ok(R"(["\\"])", {"<array>", "\\", "</array>"});
    assert_ok(R"(["\b"])", {"<array>", "\b", "</array>"});
    assert_ok(R"(["\f"])", {"<array>", "\f", "</array>"});
    assert_ok(R"(["\n"])", {"<array>", "\n", "</array>"});
    assert_ok(R"(["\r"])", {"<array>", "\r", "</array>"});
    assert_ok(R"(["\t"])", {"<array>", "\t", "</array>"});
}

TEST_F(ReaderTests, InvalidEscapes)
{
    assert_corrupted(R"(["\"])");
    assert_corrupted(R"(["\z"])");
    assert_corrupted(R"(["\0"])");
}

TEST_F(ReaderTests, ValidUnicodeEscapes)
{
    assert_ok(R"({"\u006b": "\u0076"})", {"<object>", "k:v", "</object>"});
    assert_ok(R"(["\u007F"])", {"<array>", "\u007F", "</array>"});
    assert_ok(R"(["\u07FF"])", {"<array>", "\u07FF", "</array>"});
    assert_ok(R"(["\uFFFF"])", {"<array>", "\uFFFF", "</array>"});
}

TEST_F(ReaderTests, InvalidUnicodeEscapes1)
{
    assert_corrupted(R"(["\u.000"])");
    assert_corrupted(R"(["\u0.00"])");
    assert_corrupted(R"(["\u00.0"])");
    assert_corrupted(R"(["\u000."])");
}

TEST_F(ReaderTests, InvalidUnicodeEscapes2)
{
    assert_corrupted(R"(["\u"])");
    assert_corrupted(R"(["\u0"])");
    assert_corrupted(R"(["\u00"])");
    assert_corrupted(R"(["\u000"])");
}

TEST_F(ReaderTests, ControlCharactersAreNotAllowed)
{
    assert_corrupted("[\"\x01\"]");
    assert_corrupted("[\"\x02\"]");
    assert_corrupted("[\"\x1E\"]");
    assert_corrupted("[\"\x1F\"]");
}

TEST_F(ReaderTests, 0x20IsAllowed)
{
    // U+0020 is the Unicode "Space" character.
    assert_ok("[\"\x20\"]", {"<array>", " ", "</array>"});
}

TEST_F(ReaderTests, ValidSurrogatePairs)
{
    assert_ok(R"(["\uD800\uDC00"])", {"<array>", "\U00010000", "</array>"});
    assert_ok(R"(["\uDBFF\uDFFF"])", {"<array>", "\U0010FFFF", "</array>"});
}

TEST_F(ReaderTests, InvalidSurrogatePairs1)
{
    // High surrogate (U+D800–U+DBFF) by itself.
    assert_corrupted(R"({"k": "\uD800")");
    assert_corrupted(R"({"k": "\uDBFE")");
}

TEST_F(ReaderTests, InvalidSurrogatePairs2)
{
    // High surrogate followed by an invalid codepoint.
    assert_corrupted(R"({"k": "\uD800\")");
    assert_corrupted(R"({"k": "\uD800\u")");
    assert_corrupted(R"({"k": "\uD800\u0")");
}

TEST_F(ReaderTests, InvalidSurrogatePairs3)
{
    // High surrogate followed by a codepoint that isn't a low surrogate (U+DC00–U+DFFF).
    assert_corrupted(R"({"k": "\uD800\uDBFE")"); // High, high
    assert_corrupted(R"({"k": "\uDBFE\uE000")"); // High, non-surrogate
}

TEST_F(ReaderTests, InvalidSurrogatePairs4)
{
    // Low surrogate by itself.
    assert_corrupted(R"({"k": "\uDC00")");
}

TEST_F(ReaderTests, NestedArrays)
{
    assert_ok(R"([[[[[[[[[], [], [], []]]]]]]]])",
              {"<array>", "<array>", "<array>", "<array>",
               "<array>", "<array>", "<array>", "<array>",
               "<array>", "</array>",
               "<array>", "</array>",
               "<array>", "</array>",
               "<array>", "</array>",
               "</array>", "</array>", "</array>", "</array>",
               "</array>", "</array>", "</array>", "</array>"});
}

TEST_F(ReaderTests, NestedObjects)
{
    assert_ok(R"({"a": {"b": {"c": {"d": {"e": {"f": {"g": {)"
              R"("h": {}, "i": {}, "j": {}, "k": {}}}}}}}}})",
              {"<object>", "a:<object>", "b:<object>", "c:<object>",
               "d:<object>", "e:<object>", "f:<object>", "g:<object>",
               "h:<object>", "</object>",
               "i:<object>", "</object>",
               "j:<object>", "</object>",
               "k:<object>", "</object>",
               "</object>", "</object>", "</object>", "</object>",
               "</object>", "</object>", "</object>", "</object>"});
}

TEST_F(ReaderTests, ObjectsAndArrays)
{
    assert_ok(R"([{"a": [{}, true]}, {"b": "2"}, ["c", "d", {"e": {"f":null}}]])",
              {"<array>", "<object>", "a:<array>", "<object>", "</object>", "<true>",
               "</array>", "</object>", "<object>", "b:2", "</object>", "<array>", "c",
               "d", "<object>", "e:<object>", "f:<null>", "</object>", "</object>",
               "</array>", "</array>"});
}

TEST_F(ReaderTests, RecognizesAllValueTypes)
{
    assert_ok(R"([null, false, true, 123, 4.56, "789", {}, []])",
              {"<array>", "<null>", "<false>", "<true>", integer_str(123),
               real_str(4.56), "789", "<object>", "</object>", "<array>",
               "</array>", "</array>"});
}

TEST_F(ReaderTests, BasicNumbers)
{
    assert_ok("[123,\n"
              " 1230,\n"
              " 12300,\n"
              " 123000,\n"
              " 1230000]",
              {"<array>",
               integer_str(123),
               integer_str(1230),
               integer_str(12300),
               integer_str(123000),
               integer_str(1230000),
               "</array>"});

    assert_ok("[0.0123,\n"
              " 0.1230,\n"
              " 1.2300,\n"
              " 12.300,\n"
              " 123.00]",
              {"<array>",
               real_str(0.0123),
               real_str(0.1230),
               real_str(1.2300),
               real_str(12.300),
               real_str(123.00),
               "</array>"});
}

TEST_F(ReaderTests, SmallIntegers)
{
    assert_ok(std::to_string(INT64_MIN), {integer_str(INT64_MIN)});
    assert_ok(std::to_string(INT64_MIN + 1), {integer_str(INT64_MIN + 1)});
    assert_ok(std::to_string(INT64_MIN + 2), {integer_str(INT64_MIN + 2)});
}

TEST_F(ReaderTests, LargeIntegers)
{
    assert_ok(std::to_string(INT64_MAX), {integer_str(INT64_MAX)});
    assert_ok(std::to_string(INT64_MAX - 1), {integer_str(INT64_MAX - 1)});
    assert_ok(std::to_string(INT64_MAX - 2), {integer_str(INT64_MAX - 2)});
}

TEST_F(ReaderTests, ValidExponentials)
{
    assert_ok("123e0", {real_str(123e0)});
    assert_ok("123e1", {real_str(123e1)});
    assert_ok("123e2", {real_str(123e2)});
    assert_ok("123e3", {real_str(123e3)});
    // '+' has no effect
    assert_ok("123e+0", {real_str(123e+0)});
    assert_ok("123e+1", {real_str(123e+1)});
    assert_ok("123e+2", {real_str(123e+2)});
    assert_ok("123e+3", {real_str(123e+3)});
    assert_ok("123e-0", {real_str(123e-0)});
    assert_ok("123e-1", {real_str(123e-1)});
    assert_ok("123e-2", {real_str(123e-2)});
    assert_ok("123e-3", {real_str(123e-3)});
}

TEST_F(ReaderTests, InvalidRealIntegralParts)
{
    assert_corrupted("01.23");
    assert_corrupted("02.34");
    assert_corrupted(".1");
    assert_corrupted(".12");
    assert_corrupted(".123");
}

TEST_F(ReaderTests, InvalidRealFractionalParts)
{
    // More than 1 dot
    assert_corrupted("1.23.");
    assert_corrupted("1.2.3");
    assert_corrupted("1..23");

    // Misc
    assert_corrupted("1.");
    assert_corrupted("12.");
    assert_corrupted("123.");
}

TEST_F(ReaderTests, InvalidRealExponentialParts)
{
    // Missing integral and/or fractional part
    assert_corrupted("-e2");
    assert_corrupted("-E2");
    assert_corrupted("-e+2");
    assert_corrupted("-e-2");
    assert_corrupted(".");
    assert_corrupted(".123");

    // Missing exponential part
    assert_corrupted("1e");
    assert_corrupted("1e-");
    assert_corrupted("1e+");
    assert_corrupted("1.2e");
    assert_corrupted("1.2e+");
    assert_corrupted("1.2e-");

    // Missing fractional part
    assert_corrupted("123.");
    assert_corrupted("123.e2");
    assert_corrupted("123.e+2");
    assert_corrupted("123.e-2");

    // Extra e or E
    assert_corrupted("1ee+2");
    assert_corrupted("1EE+2");
    assert_corrupted("1e+2e");

    // Extra sign
    assert_corrupted("1e++2");
    assert_corrupted("1e+2+");
    assert_corrupted("1e+2-");
    assert_corrupted("1e-2+");
    assert_corrupted("1e-2-");
    assert_corrupted("1e--2");

    // Extra dot
    assert_corrupted("1.0.e+2");
    assert_corrupted("1..0e+2");

    // Fractional power
    assert_corrupted("1e.");
    assert_corrupted("1e.2");
    assert_corrupted("1e2.");
    assert_corrupted("1e2.0");
}

TEST_F(ReaderTests, LeadingZerosAreNotAllowed)
{
    assert_corrupted("00");
    assert_corrupted("01");
    assert_corrupted("02");
}

TEST_F(ReaderTests, LowerBoundary)
{
    assert_ok("-9223372036854775809", {real_str(-9223372036854775809.0)});
    assert_ok("-92233720368547758080", {real_str(-92233720368547758080.0)});
}

TEST_F(ReaderTests, UpperBoundary)
{
    assert_ok("9223372036854775808", {real_str(9223372036854775808.0)});
    assert_ok("9223372036854775809", {real_str(9223372036854775809.0)});
    assert_ok("92233720368547758080", {real_str(92233720368547758080.0)});
}

TEST_F(ReaderTests, OverflowingIntegersBecomeReals)
{
    static constexpr uint64_t kOffset = 9223372036854775807ULL;
    for (uint64_t i = 1; i < 64; ++i) {
        assert_ok(std::to_string(i + kOffset), {real_str(static_cast<double>(i + kOffset))});
    }
}

TEST_F(ReaderTests, UnderflowingIntegersBecomeReals)
{
    Reader reader(m_handler);
    for (const auto *str : {"-9223372036854775809",
                            "-9223372036854775810",
                            "-9223372036854775908",
                            "-123456789012345678901234567890"}) {
        reset_test_state();
        ASSERT_OK(reader.read(str));
        ASSERT_EQ(m_handler.records[0].find("<real="), 0);
    }
}

TEST_F(ReaderTests, LargeRealsAreValidated)
{
    assert_corrupted("123456789012345678901234567890..");
    assert_corrupted("123456789012345678901234567890ee");
    assert_corrupted("123456789012345678901234567890e10.1");
}

class ReaderOOMTests : public ReaderTests
{
public:
    size_t m_num_allocations = 0;
    size_t m_max_allocations = 0;

    ~ReaderOOMTests() override = default;

    static auto should_next_allocation_fail(void *self) -> int
    {
        auto &s = *static_cast<ReaderOOMTests *>(self);
        if (s.m_num_allocations >= s.m_max_allocations) {
            return -1;
        }
        ++s.m_num_allocations;
        return 0;
    }

    void SetUp() override
    {
        DebugAllocator::set_hook(should_next_allocation_fail, this);
    }

    void TearDown() override
    {
        DebugAllocator::set_hook(nullptr, this);
    }
};

TEST_F(ReaderOOMTests, OOM)
{
    TEST_LOG << "ReaderOOMTests.OOM\n";
    Status s;
    Reader reader(m_handler);
    do {
        reset_test_state();
        s = reader.read(kExample2);
        ++m_max_allocations;
        m_num_allocations = 0;
    } while (s.is_no_memory());
    ASSERT_OK(s);

    ASSERT_EQ(m_handler.records, s_example_target_2);
    TEST_LOG << "Number of failures: " << m_max_allocations << '\n';
}

} // namespace calicodb::test
