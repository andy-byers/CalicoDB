#ifndef CALICO_TEST_UNIT_TESTS_H
#define CALICO_TEST_UNIT_TESTS_H

#include "calico/status.h"
#include "store/disk.h"
#include "store/heap.h"
#include "utils/utils.h"
#include "fakes.h"
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

// TODO: Make sure test fixtures inheriting from this class that create databases are using the storage object created here.
class TestOnDisk : public testing::Test {
public:
    static constexpr auto ROOT = "/tmp/__calico_test__";
    static constexpr auto PREFIX = "/tmp/__calico_test__/";

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

class TestWithMock : public testing::Test {
public:
    static constexpr auto ROOT = "test";
    static constexpr auto PREFIX = "test/";

    TestWithMock()
        : store {std::make_unique<testing::NiceMock<MockStorage>>()}
    {
        mock_store().delegate_to_real();
        CALICO_EXPECT_TRUE(expose_message(store->create_directory(ROOT)));
    }

    ~TestWithMock() override = default;

    [[nodiscard]]
    auto mock_store() -> MockStorage&
    {
        return dynamic_cast<MockStorage&>(*store);
    }

    [[nodiscard]]
    auto mock_store() const -> const MockStorage&
    {
        return dynamic_cast<const MockStorage&>(*store);
    }

    std::unique_ptr<Storage> store;
};

} // namespace calico

#endif // CALICO_TEST_UNIT_TESTS_H
