#ifndef CUB_TEST_INTEGRATION_HPP
#define CUB_TEST_INTEGRATION_HPP

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "common.h"
#include "tools.h"

namespace cub {

using namespace cub;

constexpr auto TEST_PATH = "/tmp/cub_test";

class Cursor;
class Database;

auto reader_task(Cursor cursor) -> void*;
auto writer_task(Database*, Size, Size) -> void*;

} // cub

#endif // CUB_TEST_INTEGRATION_HPP
