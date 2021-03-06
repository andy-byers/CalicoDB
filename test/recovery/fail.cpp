#include <filesystem>
#include <fstream>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace cco;
namespace fs = std::filesystem;

static constexpr Size KEY_WIDTH {12};
static constexpr Size LIMIT {10'000'000};

auto show_usage()
{
    std::cout << "usage: fail PATH N SEED\n";
    std::cout << "  Parameters\n";
    std::cout << "==============\n";
    std::cout << "PATH: Path at which to create the database\n";
    std::cout << "N: Number of records committed to the database\n";
    std::cout << "SEED: Seed for generating the modifications\n";
}

auto main(int argc, const char *argv[]) -> int
{
    if (argc != 4) {
        show_usage();
        return 1;
    }
    const fs::path path {argv[1]};
    const auto value_path = path / "values";
    const auto num_committed = std::stoul(argv[2]);
    const auto max_database_size = num_committed * 5;
    Random random {static_cast<Random::Seed>(std::stoi(argv[3]))};

    std::error_code ignore;
    std::filesystem::remove_all(path, ignore);

    // Use small pages and few frames to cause lots of stealing.
    Options options;
    options.page_size = 0x200;
    options.path = path;
    Database db {options};
    CCO_EXPECT_TRUE(db.open().is_ok());
    {
        std::ofstream ofs {value_path, std::ios::trunc};
        CCO_EXPECT_TRUE(ofs.is_open());
        for (Index i {}; i < num_committed; ++i) {
            const auto key = make_key<KEY_WIDTH>(i);
            const auto value = random_string(random, 2, 15);
            CCO_EXPECT_TRUE(db.insert(stob(key), stob(value)).is_ok());
            ofs << value << '\n';
        }
    }

    puts(value_path.c_str());
    fflush(stdout);
    Batch batch;

    // Modify the database until we receive a signal or hit the operation limit.
    for (Index i {}; i < LIMIT; ++i) {
        const auto key = std::to_string(random.next_int(num_committed * 2));
        const auto value = random_string(random, 0, options.page_size / 2);
        CCO_EXPECT_TRUE(batch.insert(stob(key), stob(value)).is_ok());

        // Keep the database from getting too large.
        if (const auto info = db.info(); info.record_count() > max_database_size) {
            while (info.record_count() >= max_database_size / 2) {
                const auto cursor = db.find_minimum();
                CCO_EXPECT_TRUE(cursor.is_valid());
                CCO_EXPECT_TRUE(batch.erase(cursor.key()).is_ok());
            }
        }
    }
    // We should never get here.
    return 1;
}
