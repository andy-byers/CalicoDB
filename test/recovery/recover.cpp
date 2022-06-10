
#include <filesystem>
#include <fstream>
#include <sstream>
#include "cub/cub.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace cub;

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
    using namespace cub;

    if (argc != 3) {
        show_usage();
        return 1;
    }
    const std::string path {argv[1]};
    const auto value_path = path + "_values";
    const auto num_committed = std::stoul(argv[2]);

    std::vector<std::string> values;
    {
        std::string line;
        std::ifstream ifs {value_path};
        CUB_EXPECT_TRUE(ifs.is_open());
        while (std::getline(ifs, line))
            values.emplace_back(line);
    }

    auto db = Database::open(path, {});
    const auto info = db.get_info();
    auto cursor = db.get_cursor();
    Index key_counter {};

    // The database should contain exactly `num_committed` records.
    CUB_EXPECT_EQ(info.record_count(), num_committed);
    CUB_EXPECT_EQ(cursor.has_record(), num_committed != 0);

    if (num_committed > 0) {
        cursor.find_minimum();
        auto itr = values.begin();
        do {
            const auto k = make_key<KEY_WIDTH>(key_counter++);
            CUB_EXPECT_EQ(_s(cursor.key()), k);
            CUB_EXPECT_EQ(cursor.value(), *itr);
            itr++;
        } while (cursor.increment());
    }
    // All records should have been reached.
    CUB_EXPECT_EQ(key_counter, num_committed);
    std::filesystem::remove(path);
    std::filesystem::remove(get_wal_path(path));
    std::filesystem::remove(value_path);
    return 0;
}