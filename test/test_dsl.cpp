// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/config.h"
#include "dsl.h"
#include "test.h"

namespace calicodb::test
{

class DSLReaderTests : public testing::Test
{
public:
    std::vector<std::string> m_records;
    uint32_t m_open_buckets = 0;
    uint32_t m_closed_buckets = 0;

    auto register_events(DSLReader &reader)
    {
        m_records.clear();
        m_open_buckets = 0;
        m_closed_buckets = 0;

        reader.register_event(DSLReader::kBeginObject, [](auto *self, const auto *output) {
            auto *test = static_cast<DSLReaderTests *>(self);
            ++test->m_open_buckets;
            test->m_records.push_back(output->to_string() + ":<bucket>");
        });
        reader.register_event(DSLReader::kEndObject, [](auto *self, const auto *) {
            ++static_cast<DSLReaderTests *>(self)->m_closed_buckets;
        });
        reader.register_event(DSLReader::kReadKeyValue, [](auto *self, const auto *output) {
            static_cast<DSLReaderTests *>(self)
                ->m_records.push_back(output[0].to_string() + ':' +
                                      output[1].to_string());
        });
    }

    auto run_example_test(const Slice &input)
    {
        const std::vector<std::string> targets = {
            ":<bucket>", // Toplevel bucket
            "browsers:<bucket>",
            "firefox:<bucket>",
            "name:Firefox",
            "pref_url:about:config",
            "releases:<bucket>",
            "1:<bucket>",
            "release_date:2004-11-09",
            "status:retired",
            "engine:Gecko",
            "engine_version:1.7"};

        DSLReader reader;
        register_events(reader);
        ASSERT_OK(reader.read(input, this));
        ASSERT_EQ(m_records, targets);
        ASSERT_EQ(m_open_buckets, 5);
        ASSERT_EQ(m_closed_buckets, 5);
    }

    auto assert_ok(const Slice &input, const std::vector<std::string> &targets)
    {
        DSLReader reader;
        register_events(reader);
        ASSERT_OK(reader.read(input, this));
        ASSERT_EQ(m_open_buckets, m_closed_buckets);
        ASSERT_EQ(m_records, targets);
    }

    auto assert_corrupted(const Slice &input)
    {
        DSLReader reader;
        register_events(reader);
        const auto s = reader.read(input, this);
        ASSERT_TRUE(s.is_corruption()) << s.message();
    }
};

TEST_F(DSLReaderTests, Examples)
{
    // Example from https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/JSON
    // with whitespace stripped.
    run_example_test(R"({"browsers":{"firefox":{"name":"Firefox","pref_url":"about:config","releases":{"1":{"release_date":"2004-11-09","status":"retired","engine":"Gecko","engine_version":"1.7"}}}}})");

    // Original text.
    run_example_test(R"({
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

TEST_F(DSLReaderTests, ValidInput)
{
    assert_ok(R"({})",
              {":<bucket>"});
    assert_ok(R"( {
                    } )",
              {":<bucket>"});
    assert_ok(R"({"":""})",
              {":<bucket>", ":"});
    assert_ok(R"({"k":"v"})",
              {":<bucket>", "k:v"});
}

TEST_F(DSLReaderTests, InvalidInput1)
{
    assert_corrupted(R"()");
    assert_corrupted(R"({)");
    assert_corrupted(R"(})");
    assert_corrupted(R"(")");
    assert_corrupted(R"(a)");
    assert_corrupted(R"( )");
}

TEST_F(DSLReaderTests, InvalidInput2)
{
    assert_corrupted(R"({"a})");
    assert_corrupted(R"({a"})");
    assert_corrupted(R"({"a"})");
    assert_corrupted(R"({"k":})");
    assert_corrupted(R"({:"v"})");
    assert_corrupted(R"({"a",})");
    assert_corrupted(R"({"a""b"})");
}

TEST_F(DSLReaderTests, InvalidInput3)
{
    std::string input;
    for (int i = 0; i < 50'000; ++i) {
        input.append(R"({"a":)");
    }
    // No need to close objects: the parser should exceed the maximum allowed object
    // nesting way before it gets that far.
    assert_corrupted(input);
    assert_corrupted(R"({}})");
}

TEST_F(DSLReaderTests, SkipsComments1)
{
    assert_ok(R"({/*comment*/})",
              {":<bucket>"});
    assert_ok(R"({/*
                    comment
                           */})",
              {":<bucket>"});
    assert_ok(R"(/*comment*/{})",
              {":<bucket>"});
    assert_ok(R"({}/*comment*/)",
              {":<bucket>"});
    assert_ok(R"({ /*c/o*m/m*e/n*t*/ })",
              {":<bucket>"});
}

TEST_F(DSLReaderTests, SkipsComments2)
{
    assert_ok(R"({"k"/*the key*/: "v" /*the value*/})",
              {":<bucket>", "k:v"});
    assert_ok(R"({"k"/*the*/ /*key*/: "v" /*the*//*value*/})",
              {":<bucket>", "k:v"});
    assert_ok(R"(/*the*/{/*key*/"k":"v"/*the*/}/*value*/)",
              {":<bucket>", "k:v"});
}

TEST_F(DSLReaderTests, InvalidComments)
{
    assert_corrupted(R"({/})");
    assert_corrupted(R"({/*})");
    assert_corrupted(R"({/**})");
    assert_corrupted(R"({/*comment*})");
}

TEST_F(DSLReaderTests, ValidEscapes)
{
    assert_ok(R"({"k": "\\xZZ"})", {":<bucket>", "k:\\xZZ"});
    assert_ok(R"({"k": "\/"})", {":<bucket>", "k:/"});
    assert_ok(R"({"k": "\b"})", {":<bucket>", "k:\b"});
    assert_ok(R"({"k": "\f"})", {":<bucket>", "k:\f"});
    assert_ok(R"({"k": "\n"})", {":<bucket>", "k:\n"});
    assert_ok(R"({"k": "\r"})", {":<bucket>", "k:\r"});
    assert_ok(R"({"k": "\t"})", {":<bucket>", "k:\t"});
}

TEST_F(DSLReaderTests, InvalidEscapes)
{
    assert_corrupted(R"({"k": "\"})");
    assert_corrupted(R"({"k": "\z"})");
    assert_corrupted(R"({"k": "\0"})");
}

TEST_F(DSLReaderTests, ValidEscapesX)
{
    assert_ok(R"({"k": "\x00"})",
              {":<bucket>", std::string("k:\x00", 3)});
    assert_ok(R"({"\x01\x23": "\xAb\xcD"})",
              {":<bucket>", "\x01\x23:\xAB\xCD"});
    assert_ok(R"({"k": "\xABCD"})",
              {":<bucket>", "k:\xAB" /* Whitespace is needed here */ "CD"});
}

TEST_F(DSLReaderTests, InvalidEscapesX)
{
    assert_corrupted(R"({"k": "\x.0"})");
    assert_corrupted(R"({"k": "\x0."})");
    assert_corrupted(R"({"k": "\xF\"})");
}

TEST_F(DSLReaderTests, ValidEscapesU)
{
    assert_ok(R"({"\u006b": "\u0076"})",
              {":<bucket>", "k:v"});
    assert_ok(R"({"k": "\u007F"})",
              {":<bucket>", "k:\u007F"});
    assert_ok(R"({"k": "\u07FF"})",
              {":<bucket>", "k:\u07FF"});
    assert_ok(R"({"k": "\uFFFF"})",
              {":<bucket>", "k:\uFFFF"});
}

TEST_F(DSLReaderTests, InvalidEscapesU1)
{
    assert_corrupted(R"({"k": "\u.000"})");
    assert_corrupted(R"({"k": "\u0.00"})");
    assert_corrupted(R"({"k": "\u00.0"})");
    assert_corrupted(R"({"k": "\u000."})");
}

TEST_F(DSLReaderTests, InvalidEscapesU2)
{
    assert_corrupted(R"({"k": "\u"})");
    assert_corrupted(R"({"k": "\u0"})");
    assert_corrupted(R"({"k": "\u00"})");
    assert_corrupted(R"({"k": "\u000"})");
}

TEST_F(DSLReaderTests, ControlCharactersAreNotAllowed)
{
    assert_corrupted(R"({"k": ")"
                     "\x01\"}");
    assert_corrupted(R"({"k": ")"
                     "\x02\"}");
    assert_corrupted(R"({"k": ")"
                     "\x1E\"}");
    assert_corrupted(R"({"k": ")"
                     "\x1F\"}");
}

TEST_F(DSLReaderTests, 0x20IsAllowed)
{
    // U+0020 is the Unicode "Space" character.
    assert_ok(R"({"k": ")"
              "\x20\"}",
              {":<bucket>", "k: "});
}

TEST_F(DSLReaderTests, ValidSurrogatePairs)
{
    assert_ok(R"({"k": "\uD800\uDC00"})",
              {":<bucket>", "k:\U00010000"});
    assert_ok(R"({"k": "\uDBFF\uDFFF"})",
              {":<bucket>", "k:\U0010FFFF"});
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

} // namespace calicodb::test
