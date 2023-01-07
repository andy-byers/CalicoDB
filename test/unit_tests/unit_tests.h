#ifndef CALICO_TEST_UNIT_TESTS_H
#define CALICO_TEST_UNIT_TESTS_H

#include "calico/status.h"
#include "storage/posix_storage.h"
#include "utils/utils.h"
#include "fakes.h"
#include "tools.h"
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>

namespace Calico {

static constexpr auto EXPECTATION_MATCHER = "^expectation";

#define EXPECT_OK(expr) \
    do { \
        const auto &expect_ok_status = (expr); \
        EXPECT_TRUE(expect_ok_status.is_ok()) << get_status_name(expect_ok_status) << ": " << expect_ok_status.what().data(); \
    } while (0)

#define ASSERT_OK(expr) \
    do { \
        const auto &assert_ok_status = (expr); \
        ASSERT_TRUE(assert_ok_status.is_ok()) << get_status_name(assert_ok_status) << ": " << assert_ok_status.what().data(); \
    } while (0)


[[nodiscard]]
inline auto expose_message(const Status &s)
{
    EXPECT_TRUE(s.is_ok()) << "Unexpected " << get_status_name(s) << " status: " << s.what().data();
    return s.is_ok();
}

class TestOnHeap : public testing::Test {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    TestOnHeap()
        : store {std::make_unique<HeapStorage>()}
    {
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestOnHeap() override = default;

    std::unique_ptr<Storage> store;
};

template<class ...Param>
class TestOnHeapWithParam : public testing::TestWithParam<Param...> {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    TestOnHeapWithParam()
        : store {std::make_unique<HeapStorage>()}
    {
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestOnHeapWithParam() override = default;

    std::unique_ptr<Storage> store;
};

class TestOnDisk : public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_test__";
    static constexpr auto PREFIX = "/tmp/__calico_test__/";

    TestOnDisk()
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
        store = std::make_unique<PosixStorage>();
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestOnDisk() override
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
    }

    std::unique_ptr<Storage> store;
};

} // namespace Calico

#endif // CALICO_TEST_UNIT_TESTS_H
