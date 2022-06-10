#include "integration.h"

#include "cub/cursor.h"
#include "cub/database.h"

namespace cub {

auto reader_task(Cursor cursor) -> void*
{
    cursor.find_minimum();
    while (cursor.increment())
        (void)cursor.value();
    return nullptr;
}

auto writer_task(Database *db, Size n, Size _0_to_10) -> void*
{
    CUB_EXPECT_LE(_0_to_10, 10);
    for (const auto &[key, value]: RecordGenerator::generate(n, {}))
        db->insert(_b(key), _b(value));
    if (_0_to_10 == 0)
        db->commit();
    return nullptr;
}

} // cub