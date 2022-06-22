#include "validators.h"

#include "calico/database.h"
#include "calico/cursor.h"
#include "utils/expect.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

namespace calico::fuzz {

auto validate_ordering(Database &db) -> void
{
    auto left = db.get_cursor();
    auto right = db.get_cursor();

    if (!left.has_record())
        return;

    CALICO_EXPECT_TRUE(right.has_record());
    if (!right.increment())
        return;

    for (; ; ) {
        CALICO_EXPECT_TRUE(left.key() < right.key());
        CALICO_EXPECT_TRUE(left.increment());
        if (!right.increment())
            return;
    }
}

} // calico::fuzz