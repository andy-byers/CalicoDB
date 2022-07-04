
#include <filesystem>
#include <fstream>
#include <sstream>
#include <spdlog/fmt/fmt.h>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace calico;

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
    using namespace calico;
    namespace fs = std::filesystem;

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

    auto db = Database::open(path, {});
    const auto info = db.info();

    // The database should contain exactly `num_committed` records.
    CALICO_EXPECT_EQ(info.record_count(), num_committed);

    Index key_counter {};
    for (const auto &value: values) {
        const auto key = make_key<KEY_WIDTH>(key_counter++);
        const auto cursor = db.find(stob(key));
        CALICO_EXPECT_TRUE(cursor.is_valid());
        CALICO_EXPECT_EQ(btos(cursor.key()), key);
        CALICO_EXPECT_EQ(cursor.value(), value);
        CALICO_EXPECT_TRUE(db.erase(stob(key)));
    }

    // All records should have been reached and removed.
    CALICO_EXPECT_EQ(key_counter, num_committed);
    CALICO_EXPECT_EQ(info.record_count(), 0);
    Database::destroy(std::move(db));
    return 0;
}