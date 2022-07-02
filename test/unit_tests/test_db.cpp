
#include <array>
#include <filesystem>
#include <vector>
#include <unordered_set>

#include <gtest/gtest.h>

#include "db/database_impl.h"
#include "file/system.h"
#include "pool/interface.h"
#include "tree/tree.h"

#include "fakes.h"
#include "tools.h"

#include "file/file.h"
#include "utils/logging.h"

namespace {

using namespace calico;

//TEST(A, B)
//{
//    Options options;
//    options.log_path = "/tmp/calico_log";
//    options.log_level = 1;
//    options.page_size = 0x400;
//
//    auto db = Database::temp(options);
//
//    RecordGenerator::Parameters param;
//    param.mean_key_size = 20;
//    param.mean_value_size = 20;
//    param.spread = 15;
//    RecordGenerator generator {param};
//    Random random {0};
//
//    for (Index iteration {}; iteration < 10; ++iteration) {
//        for (const auto &record: generator.generate(random, 500))
//            db.insert(record);
//
//        while (db.info().record_count()) {
//            const auto key = random_string(random, 10, 30);
//
//            if (key == "VwqFU42knuuK7fkcMX") {
//
//            }
//
//            if (auto c = db.find(stob(key), true); c.is_valid())
//                db.erase(c);
//        }
//    }
//}

} // <anonymous>
