#ifndef CALICO_TEST_UNIT_TESTS_H
#define CALICO_TEST_UNIT_TESTS_H

#include "calico/status.h"
#include "store/disk.h"
#include "store/heap.h"
#include "utils/utils.h"
#include <gtest/gtest.h>
#include <iomanip>
#include <sstream>

namespace calico {

static constexpr auto EXPECTATION_MATCHER = "^expectation";

[[nodiscard]]
inline auto expose_message(const Status &s)
{
    EXPECT_TRUE(s.is_ok()) << "Unexpected " << get_status_name(s) << " status: " << s.what();
    return s.is_ok();
}

class TestOnHeap : public testing::Test {
public:
    static constexpr auto ROOT = "test/";

    TestOnHeap()
        : store {std::make_unique<HeapStorage>()}
    {
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestOnHeap() override = default;

    std::unique_ptr<Storage> store;
};

class TestOnDisk : public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_test__/";

    TestOnDisk()
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
        store = std::make_unique<DiskStorage>();
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestOnDisk() override
    {
        std::error_code ignore;
        std::filesystem::remove_all(ROOT, ignore);
    }

    std::unique_ptr<Storage> store;
};

} // namespace calico

#endif // CALICO_TEST_UNIT_TESTS_H
