#include <filesystem>
#include <fstream>
#include <sstream>
#include "spdlog/fmt/fmt.h"
#include "calico/calico.h"
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
        fmt::print("cannot run recovery: database from `fail` does not exist (run `fail` first)\n");
        return 1;
    }

    std::vector<std::string> values;
    {
        std::string line;
        std::ifstream ifs {value_path};
        CALICO_EXPECT_TRUE(ifs.is_open());
        while (std::getline(ifs, line))
            values.emplace_back(line);
    }
    Database db;
    expect_ok(db.open(path.string()));
    const auto info = db.statistics();

    // The database should contain exactly `num_committed` records.
    CALICO_EXPECT_EQ(info.record_count(), num_committed);

    Size key_counter {};
    auto xact = db.transaction();
    for (const auto &value: values) {
        const auto key = make_key<KEY_WIDTH>(key_counter++);
        const auto cursor = db.find_exact(key);
        CALICO_EXPECT_TRUE(cursor.is_valid());
        CALICO_EXPECT_EQ(cursor.key().to_string(), key);
        CALICO_EXPECT_EQ(cursor.value(), value);
        expect_ok(db.erase(key));
    }
    CALICO_EXPECT_TRUE(xact.commit().is_ok());

    // All records should have been reached and removed.
    CALICO_EXPECT_EQ(key_counter, num_committed);
    CALICO_EXPECT_EQ(info.record_count(), 0);
    expect_ok(std::move(db).destroy());
    return 0;
}