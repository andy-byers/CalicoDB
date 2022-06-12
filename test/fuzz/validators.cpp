#include "validators.h"

#include "cub/database.h"
#include "cub/reader.h"
#include "utils/expect.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

namespace cub::fuzz {

auto validate_ordering(Database &db) -> void
{
    auto left = db.get_cursor();
    auto right = db.get_cursor();

    if (!left.has_record())
        return;

    CUB_EXPECT_TRUE(right.has_record());
    if (!right.increment())
        return;

    for (; ; ) {
        CUB_EXPECT_TRUE(left.key() < right.key());
        CUB_EXPECT_TRUE(left.increment());
        if (!right.increment())
            return;
    }
}

} // cub::fuzz

//int main()
//{
//
//    return 0;
//}