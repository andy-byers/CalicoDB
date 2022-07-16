#include <filesystem>
#include <fstream>
#include <sstream>
#include <spdlog/fmt/fmt.h>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace cco;
namespace fs = std::filesystem;

static constexpr Size KEY_WIDTH {12};

auto show_usage()
{
    std::cout << "usage: recover PATH N\n";
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
        fmt::print("cannot run recovery: database from `fail` does not exist (run `fail` first)\n");
        return 1;
    }

    std::vector<std::string> values;
    {
        std::string line;
        std::ifstream ifs {value_path};
        CCO_EXPECT_TRUE(ifs.is_open());
        while (std::getline(ifs, line))
            values.emplace_back(line);
    }

    auto db = Database::open(path, {}).value();
    const auto info = db.info();

    // The database should contain exactly `num_committed` records.
    CCO_EXPECT_EQ(info.record_count(), num_committed);

    Index key_counter {};
    for (const auto &value: values) {
        const auto key = make_key<KEY_WIDTH>(key_counter++);
        const auto cursor = db.find_exact(stob(key));
        CCO_EXPECT_TRUE(cursor.is_valid());
        CCO_EXPECT_EQ(btos(cursor.key()), key);
        CCO_EXPECT_EQ(cursor.value(), value);
        CCO_EXPECT_TRUE(db.erase(stob(key)).value());
    }

    // All records should have been reached and removed.
    CCO_EXPECT_EQ(key_counter, num_committed);
    CCO_EXPECT_EQ(info.record_count(), 0);
    CCO_EXPECT_TRUE(Database::destroy(std::move(db)).has_value());
    return 0;
}