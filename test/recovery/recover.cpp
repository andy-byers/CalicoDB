#include <filesystem>
#include <fstream>
#include <iostream>
#include "spdlog/fmt/fmt.h"
#include "calico/calico.h"
#include "utils/utils.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace Calico;
namespace fs = std::filesystem;

static constexpr Size KEY_WIDTH {12};

auto show_usage()
{
    std::cout << "usage: ensure_consistency_on_startup PATH N\n";
    std::cout << "  Parameters\n";
    std::cout << "==============\n";
    std::cout << "PATH: Path at which to look for the database\n";
    std::cout << "N: Number of records that the database should contain after recovery\n";
}

auto main(int argc, const char *argv[]) -> int
{
    if (argc != 3) {
        show_usage();
        return 1;
    }
    const fs::path path {argv[1]};
    const auto value_path = path / "values";
    const auto num_committed = std::stoul(argv[2]);

    if (!fs::exists(value_path)) {
        fmt::print(stderr, "cannot run recovery: database from `fail` does not exist (run `fail` first)\n");
        return 1;
    }

    std::vector<std::string> values;
    {
        std::string line;
        std::ifstream ifs {value_path};
        CALICO_EXPECT_TRUE(ifs.is_open());
        while (std::getline(ifs, line)) {
            values.emplace_back(line);
        }
    }
    Database *db;
    CALICO_EXPECT_TRUE(Database::open(path.string(), {}, &db).is_ok());
    auto record_count = std::stoi(db->get_property("calico.count.records"));

    // The database should contain exactly `num_committed` records.
    CALICO_EXPECT_EQ(record_count, num_committed);

    Size key_counter {};
    auto *cursor = db->new_cursor();
    for (const auto &value: values) {
        const auto key = Tools::integral_key<KEY_WIDTH>(key_counter++);
        cursor->seek(key);
        CALICO_EXPECT_TRUE(cursor->is_valid());
        CALICO_EXPECT_EQ(cursor->key().to_string(), key);
        CALICO_EXPECT_EQ(cursor->value(), value);
        CALICO_EXPECT_TRUE(db->erase(key).is_ok());
    }
    CALICO_EXPECT_TRUE(db->commit().is_ok());
    delete cursor;

    // All records should have been reached and removed.
    record_count = std::stoi(db->get_property("calico.count.records"));
    CALICO_EXPECT_EQ(key_counter, num_committed);
    CALICO_EXPECT_EQ(record_count, 0);
    CALICO_EXPECT_TRUE(Database::destroy(path.string(), {}).is_ok());
    return 0;
}