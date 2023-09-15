// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "test.h"

#include "calicodb/config.h"
#include "config_internal.h"
#include "encoding.h"
#include "logging.h"
#include "status_internal.h"

namespace calicodb::test
{

class AllocTests : public testing::Test
{
public:
    static constexpr size_t kFakeAllocationSize = 1'024;
    alignas(uint64_t) static char s_fake_allocation[kFakeAllocationSize];
    static void *s_alloc_data_ptr;

    explicit AllocTests() = default;
    ~AllocTests() override = default;

    auto SetUp() -> void override
    {
        ASSERT_EQ(DebugAllocator::bytes_used(), 0);
    }

    auto TearDown() -> void override
    {
        ASSERT_EQ(DebugAllocator::bytes_used(), 0);
        DebugAllocator::set_limit(0);
        DebugAllocator::set_hook(nullptr, nullptr);
        ASSERT_OK(configure(kSetAllocator, DebugAllocator::config()));
    }
};

// The wrapper functions in alloc.cpp add a header of 8 bytes to each allocation, which is
// used to store the number of bytes in the rest of the allocation.
alignas(uint64_t) char AllocTests::s_fake_allocation[kFakeAllocationSize];
void *AllocTests::s_alloc_data_ptr = s_fake_allocation;

static constexpr AllocatorConfig kFakeConfig = {
    [](auto) -> void * {
        return AllocTests::s_fake_allocation;
    },
    [](auto *old_ptr, auto) -> void * {
        EXPECT_EQ(old_ptr, AllocTests::s_fake_allocation);
        return AllocTests::s_fake_allocation;
    },
    [](auto *ptr) {
        EXPECT_EQ(ptr, AllocTests::s_fake_allocation);
    },
};

static constexpr AllocatorConfig kFaultyConfig = {
    [](auto) -> void * {
        return nullptr;
    },
    [](auto *, auto) -> void * {
        return nullptr;
    },
    [](auto *) {},
};

TEST_F(AllocTests, Configure)
{
    AllocatorConfig saved;
    ASSERT_OK(configure(kGetAllocator, &saved));
    auto *ptr = saved.malloc(42);
    ASSERT_OK(configure(kSetAllocator, kFakeConfig));
    Mem::deallocate(Mem::reallocate(Mem::allocate(123), 42));
    ASSERT_OK(configure(kSetAllocator, saved));
    Mem::deallocate(ptr);

    ASSERT_OK(configure(kSetAllocator, AllocatorConfig{
                                           CALICODB_DEFAULT_MALLOC,
                                           CALICODB_DEFAULT_REALLOC,
                                           CALICODB_DEFAULT_FREE,
                                       }));
    Mem::deallocate(Mem::reallocate(Mem::allocate(123), 42));
}

TEST_F(AllocTests, Methods)
{
    auto *ptr = Mem::allocate(123);
    ASSERT_NE(ptr, nullptr);
    auto *new_ptr = Mem::reallocate(ptr, 321);
    ASSERT_NE(new_ptr, nullptr);

    Mem::deallocate(new_ptr);

    ASSERT_OK(configure(kSetAllocator, kFakeConfig));
    ASSERT_EQ(ptr = Mem::allocate(123), s_alloc_data_ptr);
    ASSERT_EQ(Mem::reallocate(ptr, 321), ptr);
    ASSERT_EQ(Mem::reallocate(ptr, 42), ptr);
    Mem::deallocate(nullptr);
    Mem::deallocate(ptr);

    ASSERT_OK(configure(kSetAllocator, kFaultyConfig));
    ASSERT_EQ(Mem::allocate(123), nullptr);
    ASSERT_EQ(Mem::reallocate(nullptr, 123), nullptr);
}

TEST_F(AllocTests, Limit)
{
    DebugAllocator::set_limit(100);
    auto *a = Mem::allocate(50 - sizeof(uint64_t));
    ASSERT_NE(a, nullptr);

    // 8-byte overhead causes this to exceed the limit.
    auto *b = Mem::allocate(50);
    ASSERT_EQ(b, nullptr);

    b = Mem::allocate(50 - sizeof(uint64_t));
    ASSERT_NE(b, nullptr);

    // 0 bytes available, fail to get 1 byte.
    auto *c = Mem::reallocate(a, 51 - sizeof(uint64_t));
    ASSERT_EQ(c, nullptr);

    c = Mem::reallocate(a, 20 - sizeof(uint64_t));
    ASSERT_NE(c, nullptr);

    ASSERT_EQ(DebugAllocator::set_limit(1), 0);
    ASSERT_NE(DebugAllocator::set_limit(0), 0);

    // a was realloc'd.
    Mem::deallocate(b);
    Mem::deallocate(c);
}

TEST_F(AllocTests, AllocationHook)
{
    struct HookArg {
        int rc = 0;
    } hook_arg;

    const auto hook = [](auto *arg) {
        return static_cast<const HookArg *>(arg)->rc;
    };

    DebugAllocator::set_hook(hook, &hook_arg);

    void *ptr;
    ASSERT_NE(ptr = Mem::allocate(123), nullptr);
    ASSERT_NE(ptr = Mem::reallocate(ptr, 321), nullptr);
    Mem::deallocate(ptr);

    ptr = nullptr;
    hook_arg.rc = -1;
    ASSERT_EQ(Mem::allocate(123), nullptr);
    ASSERT_EQ(Mem::reallocate(ptr, 321), nullptr);
}

TEST_F(AllocTests, LargeAllocations)
{
    // Don't actually allocate anything.
    ASSERT_OK(configure(kSetAllocator, kFakeConfig));

    void *p;
    ASSERT_EQ(nullptr, Mem::allocate(kMaxAllocation + 1));
    ASSERT_NE(nullptr, p = Mem::allocate(kMaxAllocation));
    ASSERT_EQ(nullptr, Mem::reallocate(p, kMaxAllocation + 1));
    ASSERT_NE(nullptr, p = Mem::reallocate(p, kMaxAllocation));
    Mem::deallocate(p);
}

TEST_F(AllocTests, ReallocSameSize)
{
    static constexpr size_t kSize = 42;

    void *ptr;
    ASSERT_NE(ptr = Mem::allocate(kSize), nullptr);
    ASSERT_NE(ptr = Mem::reallocate(ptr, kSize), nullptr);
    Mem::deallocate(ptr);
}

TEST_F(AllocTests, SpecialCases)
{
    void *ptr;

    // NOOP, returns nullptr.
    ASSERT_EQ(Mem::allocate(0), nullptr);
    ASSERT_EQ(DebugAllocator::bytes_used(), 0);
    Mem::deallocate(nullptr);

    // NOOP, same
    ASSERT_EQ(Mem::reallocate(nullptr, 0), nullptr);
    ASSERT_EQ(DebugAllocator::bytes_used(), 0);

    // Equivalent to Mem::malloc(1).
    ASSERT_NE(ptr = Mem::reallocate(nullptr, 1), nullptr);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(DebugAllocator::bytes_used(), DebugAllocator::size_of(ptr));

    // Equivalent to Mem::free(ptr), but returns nullptr.
    ASSERT_EQ(Mem::reallocate(ptr, 0), nullptr);
    ASSERT_EQ(DebugAllocator::bytes_used(), 0);
}

TEST_F(AllocTests, HeapObject)
{
    struct CustomObject : public HeapObject {
        int data[42];
    };

    auto *obj = new (std::nothrow) CustomObject;
    ASSERT_GE(DebugAllocator::bytes_used(), DebugAllocator::size_of(obj));
    delete obj;
    ASSERT_EQ(DebugAllocator::bytes_used(), 0);
}

#ifndef NDEBUG
TEST_F(AllocTests, DeathTest)
{
    void *ptr;
    ptr = Mem::allocate(1);

    auto *size_ptr = reinterpret_cast<uint64_t *>(ptr) - 1;
    const auto saved_value = *size_ptr;
    // Give back more memory than was allocated in-total. If more than 1 byte were already allocated, this
    // corruption would go undetected.
    *size_ptr = saved_value + 1;
    ASSERT_DEATH(Mem::deallocate(ptr), "Assert");
    ASSERT_DEATH((void)Mem::reallocate(ptr, 123), "Assert");
    // Actual allocations must not be zero-length. Mem::malloc() returns a nullptr if 0 bytes are
    // requested.
    *size_ptr = saved_value - 1;
    ASSERT_DEATH(Mem::deallocate(ptr), "Assert");
    ASSERT_DEATH((void)Mem::reallocate(ptr, 123), "Assert");

    *size_ptr = saved_value;
    Mem::deallocate(ptr);
}
#endif // NDEBUG

TEST(UniquePtr, PointerWidth)
{
    static_assert(sizeof(UniquePtr<int, DefaultDestructor>) == sizeof(void *));
    static_assert(sizeof(UniquePtr<int, ObjectDestructor>) == sizeof(void *));
    static_assert(sizeof(UniquePtr<int, UserObjectDestructor>) == sizeof(void *));

    struct Destructor1 {
        auto operator()(void *) -> void
        {
        }
    };

    static_assert(sizeof(UniquePtr<int, Destructor1>) == sizeof(void *));

    struct Destructor2 {
        uint8_t u8;
        auto operator()(void *) -> void
        {
        }
    };

    // Object gets padded out to the size of 2 pointers.
    static_assert(sizeof(UniquePtr<int, Destructor2>) == sizeof(void *) * 2);
}

TEST(UniquePtr, DestructorIsCalled)
{
    int destruction_count = 0;

    struct Destructor {
        int *const count;

        explicit Destructor(int &count)
            : count(&count)
        {
        }

        auto operator()(int *ptr) const -> void
        {
            // Ignore calls that result in "delete nullptr".
            *count += ptr != nullptr;
            delete ptr;
        }
    } destructor(destruction_count);

    {
        UniquePtr<int, Destructor> ptr(new int(123), destructor);
        (void)ptr;
    }
    ASSERT_EQ(destruction_count, 1);

    UniquePtr<int, Destructor> ptr(new int(123), destructor);
    ptr.reset();
    ASSERT_EQ(destruction_count, 2);

    ptr.reset(new int(123));
    ASSERT_EQ(destruction_count, 2);

    delete ptr.release();
    ASSERT_EQ(destruction_count, 2);

    UniquePtr<int, Destructor> ptr2(new int(42), destructor);
    ptr = std::move(ptr2);
    ASSERT_EQ(destruction_count, 2);

    auto ptr3 = std::move(ptr);
    ASSERT_EQ(*ptr3, 42);
    ASSERT_EQ(destruction_count, 2);

    ptr3.reset();
    ASSERT_EQ(destruction_count, 3);
}

TEST(Encoding, Fixed32)
{
    std::string s;
    for (uint32_t v = 0; v < 100000; v++) {
        s.resize(s.size() + sizeof(uint32_t));
        put_u32(s.data() + s.size() - sizeof(uint32_t), v);
    }

    const char *p = s.data();
    for (uint32_t v = 0; v < 100000; v++) {
        uint32_t actual = get_u32(p);
        ASSERT_EQ(v, actual);
        p += sizeof(uint32_t);
    }
}

TEST(Coding, Fixed64)
{
    std::string s;
    for (int power = 0; power <= 63; power++) {
        uint64_t v = static_cast<uint64_t>(1) << power;
        s.resize(s.size() + sizeof(uint64_t) * 3);
        put_u64(s.data() + s.size() - sizeof(uint64_t) * 3, v - 1);
        put_u64(s.data() + s.size() - sizeof(uint64_t) * 2, v + 0);
        put_u64(s.data() + s.size() - sizeof(uint64_t) * 1, v + 1);
    }

    const char *p = s.data();
    for (int power = 0; power <= 63; power++) {
        uint64_t v = static_cast<uint64_t>(1) << power;
        uint64_t actual;
        actual = get_u64(p);
        ASSERT_EQ(v - 1, actual);
        p += sizeof(uint64_t);

        actual = get_u64(p);
        ASSERT_EQ(v + 0, actual);
        p += sizeof(uint64_t);

        actual = get_u64(p);
        ASSERT_EQ(v + 1, actual);
        p += sizeof(uint64_t);
    }
}

// Test that encoding routines generate little-endian encodings
TEST(Encoding, EncodingOutput)
{
    std::string dst(4, '\0');
    put_u32(dst.data(), 0x04030201);
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));

    dst.resize(8);
    put_u64(dst.data(), 0x0807060504030201ull);
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));
    ASSERT_EQ(0x05, static_cast<int>(dst[4]));
    ASSERT_EQ(0x06, static_cast<int>(dst[5]));
    ASSERT_EQ(0x07, static_cast<int>(dst[6]));
    ASSERT_EQ(0x08, static_cast<int>(dst[7]));
}

auto append_varint(std::string *s, uint32_t v) -> void
{
    const auto len = varint_length(v);
    s->resize(s->size() + len);
    encode_varint(s->data() + s->size() - len, v);
}

TEST(Coding, Varint32)
{
    std::string s;
    for (uint32_t i = 0; i < (32 * 32); i++) {
        uint32_t v = (i / 32) << (i % 32);
        append_varint(&s, v);
    }

    const char *p = s.data();
    const char *limit = p + s.size();
    for (uint32_t i = 0; i < (32 * 32); i++) {
        uint32_t expected = (i / 32) << (i % 32);
        uint32_t actual;
        const char *start = p;
        p = decode_varint(p, limit, actual);
        ASSERT_TRUE(p != nullptr);
        ASSERT_EQ(expected, actual);
        ASSERT_EQ(varint_length(actual), p - start);
    }
    ASSERT_EQ(p, s.data() + s.size());
}

TEST(Coding, Varint32Overflow)
{
    uint32_t result;
    std::string input("\x81\x82\x83\x84\x85\x11");
    ASSERT_TRUE(decode_varint(input.data(), input.data() + input.size(),
                              result) == nullptr);
}

TEST(Coding, Varint32Truncation)
{
    uint32_t large_value = (1u << 31) + 100;
    std::string s;
    append_varint(&s, large_value);
    uint32_t result;
    for (size_t len = 0; len < s.size() - 1; len++) {
        ASSERT_TRUE(decode_varint(s.data(), s.data() + len, result) == nullptr);
    }
    ASSERT_TRUE(decode_varint(s.data(), s.data() + s.size(), result) !=
                nullptr);
    ASSERT_EQ(large_value, result);
}

TEST(Status, StatusMessages)
{
    ASSERT_EQ(Slice("OK"), Status::ok().message());
    ASSERT_EQ(Slice("I/O error"), Status::io_error().message());
    ASSERT_EQ(Slice("corruption"), Status::corruption().message());
    ASSERT_EQ(Slice("invalid argument"), Status::invalid_argument().message());
    ASSERT_EQ(Slice("not supported"), Status::not_supported().message());
    ASSERT_EQ(Slice("busy"), Status::busy().message());
    ASSERT_EQ(Slice("aborted"), Status::aborted().message());

    ASSERT_EQ(Slice("busy: retry"), Status::retry().message());
    ASSERT_EQ(Slice("aborted: no memory"), Status::no_memory().message());

    static constexpr auto kMsg = "message";
    ASSERT_EQ(Slice(kMsg), Status::io_error(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::corruption(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::invalid_argument(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::not_supported(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::busy(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::aborted(kMsg).message());

    ASSERT_EQ(Slice(kMsg), Status::retry(kMsg).message());
    ASSERT_EQ(Slice(kMsg), Status::no_memory(kMsg).message());
}

TEST(Status, StatusBuilderMessages)
{
    static constexpr auto kFmt = "message %d %s...";
    static constexpr auto kExpected = "message 42 hello...";
    ASSERT_EQ(Slice(kExpected), StatusBuilder::io_error(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::corruption(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::invalid_argument(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::not_supported(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::busy(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::aborted(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::retry(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(kExpected), StatusBuilder::no_memory(kFmt, 42, "hello").message());
}

TEST(Status, StatusBuilderFallback)
{
    // StatusBuilder should fail to allocate memory for the error message and return an inline Status with
    // the requested Status::Code and Status::SubCode.
    DebugAllocator::set_limit(1);

    static constexpr auto kFmt = "message %d %s...";
    ASSERT_EQ(Slice(Status::io_error().message()), StatusBuilder::io_error(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::corruption().message()), StatusBuilder::corruption(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::invalid_argument().message()), StatusBuilder::invalid_argument(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::not_supported().message()), StatusBuilder::not_supported(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::busy().message()), StatusBuilder::busy(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::aborted().message()), StatusBuilder::aborted(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::retry().message()), StatusBuilder::retry(kFmt, 42, "hello").message());
    ASSERT_EQ(Slice(Status::no_memory().message()), StatusBuilder::no_memory(kFmt, 42, "hello").message());

    // Reset memory limit back to the default.
    DebugAllocator::set_limit(0);
}

TEST(Status, StatusCodes)
{
#define CHECK_CODE(_Label, _Code)                \
    ASSERT_TRUE(Status::_Label().is_##_Label()); \
    ASSERT_EQ(Status::_Label().code(), Status::_Code)
#define CHECK_SUBCODE(_Label, _Code, _SubCode)         \
    ASSERT_TRUE(Status::_Label().is_##_Label());       \
    ASSERT_EQ(Status::_Label().code(), Status::_Code); \
    ASSERT_EQ(Status::_Label().subcode(), Status::_SubCode)

    CHECK_CODE(ok, kOK);

    CHECK_CODE(invalid_argument, kInvalidArgument);
    CHECK_CODE(io_error, kIOError);
    CHECK_CODE(not_supported, kNotSupported);
    CHECK_CODE(corruption, kCorruption);
    CHECK_CODE(not_found, kNotFound);
    CHECK_CODE(busy, kBusy);
    CHECK_CODE(aborted, kAborted);

    CHECK_SUBCODE(retry, kBusy, kRetry);
    CHECK_SUBCODE(no_memory, kAborted, kNoMemory);

#undef CHECK_CODE
#undef CHECK_SUBCODE
}

TEST(Status, Copy)
{
    const auto s = Status::invalid_argument("status message");
    const auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.message(), Slice("status message"));

    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.message(), Slice("status message"));

    // Pointer comparison. Status cannot allocate memory in its copy constructor/assignment operator.
    // A refcount is increased instead.
    ASSERT_EQ(s.message(), t.message());
}

TEST(Status, CopyReleasesMemory)
{
    {
        auto s = Status::invalid_argument("status message");
        const auto s_bytes_used = DebugAllocator::bytes_used();
        ASSERT_GT(s_bytes_used, 0);

        const auto t = Status::no_memory("status message 2");
        const auto t_bytes_used = DebugAllocator::bytes_used() - s_bytes_used;
        ASSERT_GT(t_bytes_used, 0);

        // s should release the memory it held and increase the refcount for the memory block
        // held by t.
        s = t;
        ASSERT_TRUE(s.is_no_memory());
        ASSERT_EQ(s.message(), Slice("status message 2"));
        ASSERT_EQ(DebugAllocator::bytes_used(), t_bytes_used);

        const auto u = t;
        ASSERT_TRUE(u.is_no_memory());
        ASSERT_EQ(u.message(), Slice("status message 2"));
        ASSERT_EQ(DebugAllocator::bytes_used(), t_bytes_used);
    }
    ASSERT_EQ(DebugAllocator::bytes_used(), 0);
}

TEST(Status, Reassign)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.is_ok());

    s = Status::invalid_argument("status message");
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.message(), Slice("status message"));

    s = Status::not_supported("status message");
    ASSERT_TRUE(s.is_not_supported());
    ASSERT_EQ(s.message(), Slice("status message"));

    s = Status::ok();
    ASSERT_TRUE(s.is_ok());
}

TEST(Status, MoveConstructor)
{
    {
        Status ok = Status::ok();
        Status ok2 = std::move(ok);

        ASSERT_TRUE(ok2.is_ok());
    }

    {
        Status status = Status::not_found("custom kNotFound status message");
        Status status2 = std::move(status);

        ASSERT_TRUE(status2.is_not_found());
        ASSERT_EQ(Slice("custom kNotFound status message"), status2.message());
    }

    {
        Status self_moved = Status::io_error("custom kIOError status message");

        // Needed to bypass compiler warning about explicit move-assignment.
        Status &self_moved_reference = self_moved;
        self_moved_reference = std::move(self_moved);
    }
}

TEST(Status, CopyInline)
{
    const auto s = Status::no_memory();
    const auto t = s;
    ASSERT_TRUE(t.is_no_memory());
    ASSERT_EQ(t.message(), Slice("aborted: no memory"));

    ASSERT_TRUE(s.is_no_memory());
    ASSERT_EQ(s.message(), Slice("aborted: no memory"));

    auto u = Status::ok();
    u = t;

    ASSERT_TRUE(u.is_no_memory());
    ASSERT_EQ(u.message(), Slice("aborted: no memory"));
}

TEST(Status, ReassignInline)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.is_ok());

    s = Status::no_memory();
    ASSERT_TRUE(s.is_no_memory());
    ASSERT_EQ(s.message(), Slice("aborted: no memory"));

    s = Status::aborted();
    ASSERT_TRUE(s.is_aborted());
    ASSERT_EQ(s.message(), Slice("aborted"));

    s = Status::ok();
    ASSERT_TRUE(s.is_ok());
}

TEST(Status, MoveConstructorInline)
{
    {
        Status status = Status::no_memory();
        Status status2 = std::move(status);

        ASSERT_TRUE(status2.is_no_memory());
        ASSERT_EQ(Slice("aborted: no memory"), status2.message());
    }

    {
        Status self_moved = Status::io_error();

        // Needed to bypass compiler warning about explicit move-assignment.
        Status &self_moved_reference = self_moved;
        self_moved_reference = std::move(self_moved);
    }
}

TEST(Status, RefcountOverflow)
{
    std::vector<Status> statuses;
    auto s = Status::not_found("not inline");
    for (size_t i = 1; i < std::numeric_limits<uint16_t>::max(); ++i) {
        statuses.push_back(s);
    }
    ASSERT_EQ(statuses.back().message(), s.message());
    statuses.push_back(s);
    ASSERT_NE(statuses.back().message(), s.message());
}

#ifndef NDEBUG
TEST(Status, InlineStatusHasNoRefcount)
{
    std::vector<Status> statuses;
    auto s = Status::not_found();
    for (size_t i = 1; i < std::numeric_limits<uint16_t>::max(); ++i) {
        statuses.push_back(s);
    }
    // If there was a refcount attached to s, it would have overflowed just now, causing an
    // assertion to trip. Must be tested with assertions enabled.
    statuses.push_back(s);
}
#endif // NDEBUG

void ConsumeDecimalNumberRoundtripTest(uint64_t number,
                                       const std::string &padding = "")
{
    std::string decimal_number = std::to_string(number);
    std::string input_string = decimal_number + padding;
    auto input = to_slice(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_TRUE(consume_decimal_number(output, &result));
    ASSERT_EQ(number, result);
    ASSERT_EQ(decimal_number.size(), output.data() - input.data());
    ASSERT_EQ(padding.size(), output.size());
}

TEST(Logging, ConsumeDecimalNumberRoundtrip)
{
    ConsumeDecimalNumberRoundtripTest(0);
    ConsumeDecimalNumberRoundtripTest(1);
    ConsumeDecimalNumberRoundtripTest(9);

    ConsumeDecimalNumberRoundtripTest(10);
    ConsumeDecimalNumberRoundtripTest(11);
    ConsumeDecimalNumberRoundtripTest(19);
    ConsumeDecimalNumberRoundtripTest(99);

    ConsumeDecimalNumberRoundtripTest(100);
    ConsumeDecimalNumberRoundtripTest(109);
    ConsumeDecimalNumberRoundtripTest(190);
    ConsumeDecimalNumberRoundtripTest(123);
    ASSERT_EQ("12345678", std::to_string(12345678));

    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
        ConsumeDecimalNumberRoundtripTest(large_number);
    }
}

TEST(Logging, ConsumeDecimalNumberRoundtripWithPadding)
{
    ConsumeDecimalNumberRoundtripTest(0, " ");
    ConsumeDecimalNumberRoundtripTest(1, "abc");
    ConsumeDecimalNumberRoundtripTest(9, "x");

    ConsumeDecimalNumberRoundtripTest(10, "_");
    ConsumeDecimalNumberRoundtripTest(11, std::string("\0\0\0", 3));
    ConsumeDecimalNumberRoundtripTest(19, "abc");
    ConsumeDecimalNumberRoundtripTest(99, "padding");

    ConsumeDecimalNumberRoundtripTest(100, " ");

    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
        ConsumeDecimalNumberRoundtripTest(large_number, "pad");
    }
}

void ConsumeDecimalNumberOverflowTest(const std::string &input_string)
{
    auto input = to_slice(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_EQ(false, consume_decimal_number(output, &result));
}

TEST(Logging, ConsumeDecimalNumberOverflow)
{
    static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
                  "Test consistency check");
    ConsumeDecimalNumberOverflowTest("18446744073709551616");
    ConsumeDecimalNumberOverflowTest("18446744073709551617");
    ConsumeDecimalNumberOverflowTest("18446744073709551618");
    ConsumeDecimalNumberOverflowTest("18446744073709551619");
    ConsumeDecimalNumberOverflowTest("18446744073709551620");
    ConsumeDecimalNumberOverflowTest("18446744073709551621");
    ConsumeDecimalNumberOverflowTest("18446744073709551622");
    ConsumeDecimalNumberOverflowTest("18446744073709551623");
    ConsumeDecimalNumberOverflowTest("18446744073709551624");
    ConsumeDecimalNumberOverflowTest("18446744073709551625");
    ConsumeDecimalNumberOverflowTest("18446744073709551626");

    ConsumeDecimalNumberOverflowTest("18446744073709551700");

    ConsumeDecimalNumberOverflowTest("99999999999999999999");
}

void ConsumeDecimalNumberNoDigitsTest(const std::string &input_string)
{
    auto input = to_slice(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_EQ(false, consume_decimal_number(output, &result));
    ASSERT_EQ(input.data(), output.data());
    ASSERT_EQ(input.size(), output.size());
}

TEST(Logging, ConsumeDecimalNumberNoDigits)
{
    ConsumeDecimalNumberNoDigitsTest("");
    ConsumeDecimalNumberNoDigitsTest(" ");
    ConsumeDecimalNumberNoDigitsTest("a");
    ConsumeDecimalNumberNoDigitsTest(" 123");
    ConsumeDecimalNumberNoDigitsTest("a123");
    ConsumeDecimalNumberNoDigitsTest(std::string("\000123", 4));
    ConsumeDecimalNumberNoDigitsTest(std::string("\177123", 4));
    ConsumeDecimalNumberNoDigitsTest(std::string("\377123", 4));
}

TEST(Logging, AppendFmtString)
{
    String str;
    ASSERT_EQ(0, append_format_string(str, "hello %d %s", 42, "goodbye"));
    const std::string long_str(128, '*');
    ASSERT_EQ(0, append_format_string(str, "%s", long_str.data()));
    ASSERT_EQ(0, append_format_string(str, "empty"));
    ASSERT_EQ(str.c_str(), "hello 42 goodbye" + long_str + "empty");
}

TEST(Slice, Construction)
{
    const auto *p = "123";
    ASSERT_EQ(p, Slice(p));
    ASSERT_EQ(p, Slice(p, 3));
}

TEST(Slice, StartsWith)
{
    Slice slice("Hello, world!");
    ASSERT_TRUE(slice.starts_with(""));
    ASSERT_TRUE(slice.starts_with("Hello"));
    ASSERT_TRUE(slice.starts_with("Hello, world!"));
    ASSERT_FALSE(slice.starts_with(" Hello"));
    ASSERT_FALSE(slice.starts_with("ello"));
    ASSERT_FALSE(slice.starts_with("Hello, world! "));
}

TEST(Slice, Comparisons)
{
    Slice slice("Hello, world!");
    const auto shorter = slice.range(0, slice.size() - 1);
    ASSERT_LT(shorter, slice);

    ASSERT_TRUE(Slice("10") > Slice("01"));
    ASSERT_TRUE(Slice("01") < Slice("10"));
    ASSERT_TRUE(Slice("10") >= Slice("01"));
    ASSERT_TRUE(Slice("01") <= Slice("10"));
}

TEST(Slice, Ranges)
{
    Slice slice("Hello, world!");
    ASSERT_TRUE(slice.range(0, 0).is_empty());
    ASSERT_EQ(slice.range(7, 5), Slice("world"));
    ASSERT_EQ(slice, slice.range(0));
    ASSERT_EQ(slice, slice.range(0, slice.size()));
}

TEST(Slice, Advance)
{
    Slice slice("Hello, world!");
    auto copy = slice;
    slice.advance(0);
    ASSERT_EQ(slice, copy);

    slice.advance(5);
    ASSERT_EQ(slice, ", world!");

    slice.advance(slice.size());
    ASSERT_TRUE(slice.is_empty());
}

TEST(Slice, Truncate)
{
    Slice slice("Hello, world!");
    auto copy = slice;
    slice.truncate(slice.size());
    ASSERT_TRUE(slice == copy);

    slice.truncate(5);
    ASSERT_EQ(slice, "Hello");

    slice.truncate(0);
    ASSERT_TRUE(slice.is_empty());
}

TEST(Slice, Clear)
{
    Slice slice("42");
    slice.clear();
    ASSERT_TRUE(slice.is_empty());
    ASSERT_EQ(0, slice.size());
}

static constexpr auto constexpr_slice_test(Slice s, Slice answer) -> int
{
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] != answer[i]) {
            return -1;
        }
    }

    (void)s.starts_with(answer);
    (void)s.data();
    (void)s.range(0, 0);
    (void)s.is_empty();
    s.advance(0);
    s.truncate(s.size());
    return 0;
}

TEST(Slice, ConstantExpressions)
{
    static constexpr Slice s1("42");
    static constexpr Slice s2("42", 2);
    ASSERT_EQ(0, constexpr_slice_test(s1, s2));
}

TEST(Slice, NonPrintableSlice)
{
    {
        const std::string s("\x00\x01", 2);
        ASSERT_EQ(2, to_slice(s).size());
    }
    {
        const std::string s("\x00", 1);
        ASSERT_EQ(0, to_slice(s).compare(to_slice(s)));
    }
    {
        std::string s("\x00\x00", 2);
        std::string t("\x00\x01", 2);
        ASSERT_LT(to_slice(s).compare(to_slice(t)), 0);
    }
    {
        std::string u("\x0F", 1);
        std::string v("\x00", 1);
        v[0] = static_cast<char>(0xF0);

        // Signed comparison. 0xF0 overflows a signed byte and becomes negative.
        ASSERT_LT(v[0], u[0]);

        // Unsigned comparison should come out the other way.
        ASSERT_LT(to_slice(u).compare(to_slice(v)), 0);
    }
}

#if not NDEBUG
TEST(Expect, DeathTest)
{
    ASSERT_DEATH(CALICODB_EXPECT_TRUE(false), "");
}

TEST(Slice, DeathTest)
{
    Slice slice("Hello, world!");
    const auto oob = slice.size() + 1;

    ASSERT_DEATH(slice.advance(oob), "Assert");
    ASSERT_DEATH(slice.truncate(oob), "Assert");
    ASSERT_DEATH((void)slice.range(oob, 1), "Assert");
    ASSERT_DEATH((void)slice.range(0, oob), "Assert");
    ASSERT_DEATH((void)slice.range(oob / 2, oob - 1), "Assert");
    ASSERT_DEATH((void)slice.range(oob), "Assert");
    ASSERT_DEATH((void)slice[oob], "Assert");
    ASSERT_DEATH(Slice(nullptr), "Assert");
    ASSERT_DEATH(Slice(nullptr, 123), "Assert");
}
#endif // not NDEBUG

class StringBuilderTests : public testing::Test
{
public:
    auto build_string() -> String
    {
        String str;
        EXPECT_EQ(m_builder.build(str), 0);
        return str;
    }

    StringBuilder m_builder;
};

TEST_F(StringBuilderTests, InitialStateIsEmpty)
{
    const auto str = build_string();
    ASSERT_EQ(str.size(), 0);
}

TEST_F(StringBuilderTests, Append)
{
    const std::string msg_a;
    const std::string msg_b("abc");
    const char msg_c = 'd';

    m_builder
        .append(to_slice(msg_a))
        .append(to_slice(msg_b))
        .append(msg_c);

    const auto str = build_string();
    ASSERT_EQ(str.size(), (msg_a + msg_b + msg_c).size());
    ASSERT_EQ(Slice(str.c_str(), str.size()), to_slice(msg_a + msg_b + msg_c));
}

TEST_F(StringBuilderTests, AppendFormat)
{
    const std::string long_str(512, '*');
    m_builder.append_format("hello %d %s", 42, "goodbye")
        .append_format("%s", long_str.data())
        .append_format("empty");
    const auto lhs = build_string();
    const auto rhs = "hello 42 goodbye" + long_str + "empty";
    ASSERT_EQ(Slice(lhs.c_str()), to_slice(rhs));
}

TEST_F(StringBuilderTests, AppendEscaped)
{
    const std::string long_str(512, '*');
    m_builder.append_format("hello %d %s", 42, "goodbye")
        .append_format("%s", long_str.data())
        .append_format("empty");
    const auto lhs = build_string();
    const auto rhs = "hello 42 goodbye" + long_str + "empty";
    ASSERT_EQ(Slice(lhs.c_str()), to_slice(rhs));
}

static constexpr const char *kTestMessages[] = {
    "aa%d",
    "bb%dbb%f",
    "cc%dcc%fcccc%p",
    "dd%ddd%fdddd%pdddddddd%u",
    "ee%dee%feeee%peeeeeeee%ueeeeeeeeeeeeeeee%x",
    "ff%dff%fffff%pffffffff%uffffffffffffffff%xffffffffffffffffffffffffffffffff%s",
};

TEST_F(StringBuilderTests, AppendMultiple)
{
    std::string answer;
    for (size_t i = 0; i < 512; ++i) {
        const auto r = static_cast<size_t>(rand()) % ARRAY_SIZE(kTestMessages);
        answer.append(kTestMessages[r]);
        m_builder.append(Slice(kTestMessages[r]));
    }
    const auto str = build_string();
    ASSERT_EQ(Slice(str.c_str()), to_slice(answer));
}

TEST_F(StringBuilderTests, AppendFormatMultiple)
{
    char buffer[4'096];
    std::string answer;
    for (size_t i = 0; i < 512; ++i) {
        const auto r = static_cast<size_t>(rand()) % ARRAY_SIZE(kTestMessages);
        const auto *fmt = kTestMessages[r];
        switch (r) {
            case 0:
                std::snprintf(buffer, sizeof(buffer), fmt, i);
                m_builder.append_format(fmt, i);
                break;
            case 1:
                std::snprintf(buffer, sizeof(buffer), fmt, i, static_cast<double>(i));
                m_builder.append_format(fmt, i, static_cast<double>(i));
                break;
            case 2:
                std::snprintf(buffer, sizeof(buffer), fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i));
                m_builder.append_format(fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i));
                break;
            case 3:
                std::snprintf(buffer, sizeof(buffer), fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i);
                m_builder.append_format(fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i);
                break;
            case 4:
                std::snprintf(buffer, sizeof(buffer), fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i, i);
                m_builder.append_format(fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i, i);
                break;
            default:
                std::snprintf(buffer, sizeof(buffer), fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i, i, "Hello, world!");
                m_builder.append_format(fmt, i, static_cast<double>(i), reinterpret_cast<void *>(i), i, i, "Hello, world!");
                break;
        }
        answer.append(buffer);
    }
    const auto str = build_string();
    ASSERT_EQ(Slice(str.c_str()), to_slice(answer));
}

} // namespace calicodb::test
