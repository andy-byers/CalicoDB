// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

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
}

} // namespace calicodb::test
