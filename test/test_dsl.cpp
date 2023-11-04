// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/config.h"
#include "common.h"
#include "dsl.h"
#include "test.h"
#include <iomanip>

namespace calicodb::test
{

class DSLReaderTests : public testing::Test
{
public:
    std::vector<std::string> m_records;
    std::string m_current;
    uint32_t m_open_objects = 0;
    uint32_t m_closed_objects = 0;
    uint32_t m_open_arrays = 0;
    uint32_t m_closed_arrays = 0;

    ~DSLReaderTests() override = default;

    static auto integer_str(int64_t i) -> std::string
    {
        return "<integer=" + std::to_string(i) + '>';
    }

    static auto real_str(double r) -> std::string
    {
        return "<real=" + std::to_string(r) + '>';
    }

    auto register_actions(DSLReader &reader)
    {
        m_records.clear();
        m_open_objects = 0;
        m_closed_objects = 0;

        reader.register_action(kEventBeginObject, [](auto *self, const auto *) {
            auto *test = static_cast<DSLReaderTests *>(self);
            ++test->m_open_objects;
            test->m_records.push_back(test->m_current + "<object>");
            test->m_current.clear();
        });
        reader.register_action(kEventEndObject, [](auto *self, const auto *) {
            auto *test = static_cast<DSLReaderTests *>(self);
            if (!test->m_current.empty()) {
                test->m_records.push_back(test->m_current);
                test->m_current.clear();
            }
            test->m_records.emplace_back("</object>");
            ++test->m_closed_objects;
        });
        reader.register_action(kEventBeginArray, [](auto *self, const auto *) {
            auto *test = static_cast<DSLReaderTests *>(self);
            ++test->m_open_arrays;
            test->m_records.push_back(test->m_current + "<array>");
            test->m_current.clear();
        });
        reader.register_action(kEventEndArray, [](auto *self, const auto *) {
            auto *test = static_cast<DSLReaderTests *>(self);
            if (!test->m_current.empty()) {
                test->m_records.push_back(test->m_current);
                test->m_current.clear();
            }
            test->m_records.emplace_back("</array>");
            ++test->m_closed_arrays;
        });
        reader.register_action(kEventKey, [](auto *self, const auto *output) {
            static_cast<DSLReaderTests *>(self)
                ->m_current = output->string.to_string() + ':';
        });
        reader.register_action(kEventValueString, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            test->m_records.push_back(test->m_current + output->string.to_string());
            test->m_current.clear();
        });
        reader.register_action(kEventValueInteger, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            test->m_records.push_back(test->m_current + integer_str(output->integer));
            test->m_current.clear();
        });
        reader.register_action(kEventValueReal, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            test->m_records.push_back(test->m_current + real_str(output->real));
            test->m_current.clear();
        });
        reader.register_action(kEventValueNull, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            ASSERT_EQ(output->null, nullptr);
            test->m_records.push_back(test->m_current + "<null>");
            test->m_current.clear();
        });
        reader.register_action(kEventValueBoolean, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            test->m_records.push_back(test->m_current + (output->boolean ? "<true>" : "<false>"));
            test->m_current.clear();
        });
    }

    auto run_example_test(const std::vector<std::string> &target, size_t num_objects, size_t num_arrays, const Slice &input)
    {
        DSLReader reader;
        register_actions(reader);
        ASSERT_OK(reader.read(input, this));
        ASSERT_EQ(m_records, target);
        ASSERT_EQ(m_open_objects, num_objects);
        ASSERT_EQ(m_closed_objects, num_objects);
        ASSERT_EQ(m_open_arrays, num_arrays);
        ASSERT_EQ(m_closed_arrays, num_arrays);
    }

    auto assert_ok(const Slice &input, const std::vector<std::string> &target)
    {
        DSLReader reader;
        register_actions(reader);
        ASSERT_OK(reader.read(input, this));
        ASSERT_EQ(m_open_objects, m_closed_objects);
        ASSERT_EQ(m_open_arrays, m_closed_arrays);
        ASSERT_EQ(m_records, target);
    }

    auto assert_corrupted(const Slice &input)
    {
        DSLReader reader;
        register_actions(reader);
        const auto s = reader.read(input, this);
        ASSERT_TRUE(s.is_corruption()) << input.to_string();
    }
};

TEST_F(DSLReaderTests, MissingEvents)
{
    DSLReader reader;
    ASSERT_OK(reader.read("[1,true,null]", this));
}

// Just objects and strings
TEST_F(DSLReaderTests, Example1)
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
    "ppu:" + DSLReaderTests::real_str(0.55),
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

TEST_F(DSLReaderTests, Example2)
{

    // Example 5 from https://opensource.adobe.com/Spry/samples/data_region/JSONDataSetSample.html,
    // shortened, with whitespace stripped.
    run_example_test(s_example_target_2, 13, 3, kExample2);
}

TEST_F(DSLReaderTests, ValidInput)
{
    // Single value
    assert_ok(R"("")", {""});
    assert_ok(R"(true)", {"<true>"});
    assert_ok(R"(false)", {"<false>"});
    assert_ok(R"(null)", {"<null>"});
    assert_ok(R"(42)", {integer_str(42)});
    assert_ok(R"(12.3)", {real_str(12.3)});

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

TEST_F(DSLReaderTests, OnlyAllowsSingleValue)
{
    assert_corrupted(R"(0, 1)");
    assert_corrupted(R"([], {})");
    assert_corrupted(R"({}, [])");
    assert_corrupted(R"([0], {})");
    assert_corrupted(R"({}, [0])");
    assert_corrupted(R"([0, 1], {})");
    assert_corrupted(R"({}, [0, 1])");
}

TEST_F(DSLReaderTests, TrailingCommasAreNotAllowed)
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

TEST_F(DSLReaderTests, HandlesMissingQuotes)
{
    assert_corrupted(R"({"k:"v"})");
    assert_corrupted(R"({k":"v"})");
    assert_corrupted(R"({"k":"v})");
    assert_corrupted(R"({"k":v"})");
    assert_corrupted(R"(["v])");
    assert_corrupted(R"([v"])");
}

TEST_F(DSLReaderTests, HandlesMissingSeparators)
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

TEST_F(DSLReaderTests, HandlesExcessiveNesting)
{
    std::string input;
    for (int i = 0; i < 50'000; ++i) {
        input.append(R"({"a":)");
    }
    // No need to close objects: the parser should exceed the maximum allowed object
    // nesting way before it gets that far.
    assert_corrupted(input);
}

TEST_F(DSLReaderTests, InvalidInput1)
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

TEST_F(DSLReaderTests, InvalidInput2)
{
    assert_corrupted(R"(,[])");
    assert_corrupted(R"(,{})");
    assert_corrupted(R"({"k"})");
    assert_corrupted(R"({"k":})");
    assert_corrupted(R"({:"v"})");
    assert_corrupted(R"({"k": "v",})");
}

TEST_F(DSLReaderTests, InvalidInput3)
{
    assert_corrupted(R"([[null]]abc)");
    assert_corrupted(R"({{"k":"v"})");
    assert_corrupted(R"({"k":"v"}})");
    assert_corrupted(R"([true)");
    assert_corrupted(R"(null])");
    assert_corrupted(R"([["v"])");
    assert_corrupted(R"(["v"]])");
}

TEST_F(DSLReaderTests, SkipsComments1)
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

TEST_F(DSLReaderTests, SkipsComments2)
{
    assert_ok(R"({"k"/*the key*/: "v" /*the value*/})",
              {"<object>", "k:v", "</object>"});
    assert_ok(R"({"k"/*the*/ /*key*/: "v" /*the*//*value*/})",
              {"<object>", "k:v", "</object>"});
    assert_ok(R"(/*the*/{/*key*/"k":"v"/*the*/}/*value*/)",
              {"<object>", "k:v", "</object>"});
}

TEST_F(DSLReaderTests, InvalidComments)
{
    assert_corrupted(R"({/})");
    assert_corrupted(R"({/*})");
    assert_corrupted(R"({/**})");
    assert_corrupted(R"({/*comment*})");
}

TEST_F(DSLReaderTests, InvalidLiterals)
{
    for (const auto *literal : {"true", "false", "null"}) {
        for (size_t i = 1, n = std::strlen(literal); i < n; ++i) {
            char buffer[5];
            std::strncpy(buffer, literal, i);
            assert_corrupted(Slice(buffer, i));
        }
    }
}

TEST_F(DSLReaderTests, ValidEscapes)
{
    assert_ok(R"(["\/"])", {"<array>", "/", "</array>"});
    assert_ok(R"(["\\"])", {"<array>", "\\", "</array>"});
    assert_ok(R"(["\b"])", {"<array>", "\b", "</array>"});
    assert_ok(R"(["\f"])", {"<array>", "\f", "</array>"});
    assert_ok(R"(["\n"])", {"<array>", "\n", "</array>"});
    assert_ok(R"(["\r"])", {"<array>", "\r", "</array>"});
    assert_ok(R"(["\t"])", {"<array>", "\t", "</array>"});
}

TEST_F(DSLReaderTests, InvalidEscapes)
{
    assert_corrupted(R"(["\"])");
    assert_corrupted(R"(["\z"])");
    assert_corrupted(R"(["\0"])");
}

TEST_F(DSLReaderTests, ValidUnicodeEscapes)
{
    assert_ok(R"({"\u006b": "\u0076"})", {"<object>", "k:v", "</object>"});
    assert_ok(R"(["\u007F"])", {"<array>", "\u007F", "</array>"});
    assert_ok(R"(["\u07FF"])", {"<array>", "\u07FF", "</array>"});
    assert_ok(R"(["\uFFFF"])", {"<array>", "\uFFFF", "</array>"});
}

TEST_F(DSLReaderTests, InvalidUnicodeEscapes1)
{
    assert_corrupted(R"(["\u.000"])");
    assert_corrupted(R"(["\u0.00"])");
    assert_corrupted(R"(["\u00.0"])");
    assert_corrupted(R"(["\u000."])");
}

TEST_F(DSLReaderTests, InvalidUnicodeEscapes2)
{
    assert_corrupted(R"(["\u"])");
    assert_corrupted(R"(["\u0"])");
    assert_corrupted(R"(["\u00"])");
    assert_corrupted(R"(["\u000"])");
}

TEST_F(DSLReaderTests, ControlCharactersAreNotAllowed)
{
    assert_corrupted("[\"\x01\"]");
    assert_corrupted("[\"\x02\"]");
    assert_corrupted("[\"\x1E\"]");
    assert_corrupted("[\"\x1F\"]");
}

TEST_F(DSLReaderTests, 0x20IsAllowed)
{
    // U+0020 is the Unicode "Space" character.
    assert_ok("[\"\x20\"]", {"<array>", " ", "</array>"});
}

TEST_F(DSLReaderTests, ValidSurrogatePairs)
{
    assert_ok(R"(["\uD800\uDC00"])", {"<array>", "\U00010000", "</array>"});
    assert_ok(R"(["\uDBFF\uDFFF"])", {"<array>", "\U0010FFFF", "</array>"});
}

TEST_F(DSLReaderTests, InvalidSurrogatePairs1)
{
    // High surrogate (U+D800–U+DBFF) by itself.
    assert_corrupted(R"({"k": "\uD800")");
    assert_corrupted(R"({"k": "\uDBFE")");
}

TEST_F(DSLReaderTests, InvalidSurrogatePairs2)
{
    // High surrogate followed by an invalid codepoint.
    assert_corrupted(R"({"k": "\uD800\")");
    assert_corrupted(R"({"k": "\uD800\u")");
    assert_corrupted(R"({"k": "\uD800\u0")");
}

TEST_F(DSLReaderTests, InvalidSurrogatePairs3)
{
    // High surrogate followed by a codepoint that isn't a low surrogate (U+DC00–U+DFFF).
    assert_corrupted(R"({"k": "\uD800\uDBFE")"); // High, high
    assert_corrupted(R"({"k": "\uDBFE\uE000")"); // High, non-surrogate
}

TEST_F(DSLReaderTests, InvalidSurrogatePairs4)
{
    // Low surrogate by itself.
    assert_corrupted(R"({"k": "\uDC00")");
}

TEST_F(DSLReaderTests, NestedArrays)
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

TEST_F(DSLReaderTests, NestedObjects)
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

TEST_F(DSLReaderTests, ObjectsAndArrays)
{
    assert_ok(R"([{"a": [{}, true]}, {"b": "2"}, ["c", "d", {"e": {"f":null}}]])",
              {"<array>", "<object>", "a:<array>", "<object>", "</object>", "<true>",
               "</array>", "</object>", "<object>", "b:2", "</object>", "<array>", "c",
               "d", "<object>", "e:<object>", "f:<null>", "</object>", "</object>",
               "</array>", "</array>"});
}

TEST_F(DSLReaderTests, RecognizesAllValueTypes)
{
    assert_ok(R"([null, false, true, 123, 4.56, "789", {}, []])",
              {"<array>", "<null>", "<false>", "<true>", integer_str(123),
               real_str(4.56), "789", "<object>", "</object>", "<array>",
               "</array>", "</array>"});
}

TEST_F(DSLReaderTests, BasicNumbers)
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

TEST_F(DSLReaderTests, SmallIntegers)
{
    assert_ok(std::to_string(INT64_MIN), {integer_str(INT64_MIN)});
    assert_ok(std::to_string(INT64_MIN + 1), {integer_str(INT64_MIN + 1)});
    assert_ok(std::to_string(INT64_MIN + 2), {integer_str(INT64_MIN + 2)});
}

TEST_F(DSLReaderTests, LargeIntegers)
{
    assert_ok(std::to_string(INT64_MAX), {integer_str(INT64_MAX)});
    assert_ok(std::to_string(INT64_MAX - 1), {integer_str(INT64_MAX - 1)});
    assert_ok(std::to_string(INT64_MAX - 2), {integer_str(INT64_MAX - 2)});
}

TEST_F(DSLReaderTests, ValidExponentials)
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

TEST_F(DSLReaderTests, InvalidExponentials)
{
    // Missing integral part
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

    // Missing fractional part
    assert_corrupted("123.");
    assert_corrupted("123.e2");

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

    // Fractional power
    assert_corrupted("1e.");
    assert_corrupted("1e.2");
    assert_corrupted("1e2.");
    assert_corrupted("1e2.0");

    // Misc.
    assert_corrupted("10.0e20000000000000");
}

TEST_F(DSLReaderTests, LeadingZerosAreNotAllowed)
{
    assert_corrupted("01");
    assert_corrupted("01");
}

TEST_F(DSLReaderTests, LowerBoundary)
{
    assert_ok("-9223372036854775809", {real_str(-9223372036854775809.0)});
    assert_ok("-92233720368547758080", {real_str(-92233720368547758080.0)});
}

TEST_F(DSLReaderTests, UpperBoundary)
{
    assert_ok("9223372036854775808", {real_str(9223372036854775808.0)});
    assert_ok("9223372036854775809", {real_str(9223372036854775809.0)});
    assert_ok("92233720368547758080", {real_str(92233720368547758080.0)});
}

TEST_F(DSLReaderTests, OverflowingIntegersBecomeReals)
{
    static constexpr uint64_t kOffset = 9223372036854775807ULL;
    for (uint64_t i = 1; i < 64; ++i) {
        assert_ok(std::to_string(i + kOffset), {real_str(static_cast<double>(i + kOffset))});
    }
}

TEST_F(DSLReaderTests, UnderflowingIntegersBecomeReals)
{
    DSLReader reader;
    register_actions(reader);
    for (const auto *str : {"-9223372036854775809",
                            "-9223372036854775810",
                            "-9223372036854775908",
                            "-123456789012345678901234567890"}) {
        m_current.clear();
        m_records.clear();
        ASSERT_OK(reader.read(str, this));
        ASSERT_EQ(m_records[0].find("<real="), 0);
    }
}

TEST_F(DSLReaderTests, LargeRealsAreValidated)
{
    assert_corrupted("123456789012345678901234567890..");
    assert_corrupted("123456789012345678901234567890ee");
    assert_corrupted("123456789012345678901234567890e10.1");
}

class DSLReaderOOMTests : public DSLReaderTests
{
public:
    size_t m_num_allocations = 0;
    size_t m_max_allocations = 0;

    ~DSLReaderOOMTests() override = default;

    static auto should_next_allocation_fail(void *self) -> int
    {
        auto &s = *static_cast<DSLReaderOOMTests *>(self);
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

TEST_F(DSLReaderOOMTests, OOM)
{
    TEST_LOG << "DSLReaderOOMTests.OOM\n";
    Status s;
    DSLReader reader;
    register_actions(reader);
    do {
        m_current.clear();
        m_records.clear();
        s = reader.read(kExample2, this);
        ++m_max_allocations;
        m_num_allocations = 0;
    } while (s.is_no_memory());
    ASSERT_OK(s);

    ASSERT_EQ(m_records, s_example_target_2);
    TEST_LOG << "Number of failures: " << m_max_allocations << '\n';
}

class DSLWriterTests : public testing::Test
{
public:
    Buffer<char> m_buffer;
    OutputStream<decltype(m_buffer)> m_os;
    DSLWriter<decltype(m_os)> m_writer;
    DSLReader m_reader;

    explicit DSLWriterTests()
        : m_os(m_buffer),
          m_writer(m_os)
    {
    }

    ~DSLWriterTests() override = default;

    void SetUp() override
    {
        m_reader.register_action(kEventBeginObject, [](auto *self, const auto *) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.begin_object();
        });
        m_reader.register_action(kEventEndObject, [](auto *self, const auto *) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.end_object();
        });
        m_reader.register_action(kEventBeginArray, [](auto *self, const auto *) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.begin_array();
        });
        m_reader.register_action(kEventEndArray, [](auto *self, const auto *) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.end_array();
        });
        m_reader.register_action(kEventKey, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_key(output->string);
        });
        m_reader.register_action(kEventValueString, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_string(output->string);
        });
        m_reader.register_action(kEventValueInteger, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_integer(output->integer);
        });
        m_reader.register_action(kEventValueReal, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_real(output->real);
        });
        m_reader.register_action(kEventValueNull, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_null();
        });
        m_reader.register_action(kEventValueBoolean, [](auto *self, const auto *output) {
            static_cast<DSLWriterTests *>(self)
                ->m_writer.write_boolean(output->boolean);
        });
    }

    auto read_and_write(const Slice &data)
    {
        m_os.reset();
        m_writer.reset();
        m_buffer.reset();
        EXPECT_OK(m_reader.read(data, this));
        EXPECT_FALSE(m_buffer.is_empty());
        return std::string(m_buffer.data(), m_os.size());
    }

    void run_test(const Slice &data)
    {
        const auto json1 = read_and_write(data);
        const auto json2 = read_and_write(data);
        ASSERT_EQ(json1, json2);
    }
};

TEST_F(DSLWriterTests, Write)
{
    run_test(R"({"a":[1,"b",true],"c":[{"d":234,"e":null}],)"
             R"("f":{"g":false,"h":[{},{},[{"i":[56,7,8,90,"j","k","lmnop"]}]]}})");
}

} // namespace calicodb::test

#include <filesystem>
#include <fstream>
TEST(SpeedTest, XYZ)
{
    using namespace calicodb;
    AllocatorConfig config = {
        std::malloc,
        std::realloc,
        std::free,
    };
    ASSERT_OK(configure(kReplaceAllocator, &config));
    size_t total_bytes = 0;
    for (int idx = 0; idx < 100; ++idx) {
        std::string base = "/Users/andy-byers/CLionProjects/CalicoDB/data";
        for (const auto &itr : std::filesystem::directory_iterator(base)) {
            std::ifstream ifs(itr.path());
            ASSERT_TRUE(ifs.is_open());
            ifs.seekg(0, std::ios::end);
            const auto sz = ifs.tellg();
            std::string data(static_cast<size_t>(sz), '\0');
            ifs.seekg(0, std::ios::beg);
            ifs.read(data.data(), sz);

            DSLReader reader;
            ASSERT_OK(reader.read(data, nullptr));
            total_bytes += data.size();
        }
    }
    std::cerr << total_bytes << " bytes parsed\n";
    ASSERT_OK(configure(kRestoreAllocator, nullptr));
}
