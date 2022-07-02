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
    auto lhs = db.find_minimum();
    auto rhs = db.find_minimum();

    if (!lhs.is_valid())
        return;
    CALICO_EXPECT_TRUE(rhs.is_valid());
    CALICO_EXPECT_TRUE(rhs.increment());

    for (; rhs.is_valid(); lhs++, rhs++) {
        CALICO_EXPECT_TRUE(lhs.is_valid());
        CALICO_EXPECT_TRUE(lhs.key() < rhs.key());
    }
}

} // calico::fuzz