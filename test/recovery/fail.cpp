
#include <filesystem>
#include <fstream>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace calico;

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
    using namespace calico;

    if (argc != 4) {
        show_usage();
        return 1;
    }
    const std::string path {argv[1]};
    const auto value_path = path + "_values";
    const auto num_committed = std::stoul(argv[2]);
    const auto max_database_size = num_committed * 5;
    Random random {static_cast<Random::Seed>(std::stoi(argv[3]))};

    std::filesystem::remove(path);
    std::filesystem::remove(get_wal_path(path));

    // Use small pages and few frames to cause lots of stealing.
    Options options;
    options.page_size = 0x200;
    options.block_size = 0x200;

    auto db = Database::open(path, options);
    {
        std::ofstream ofs {value_path, std::ios::trunc};
        CALICO_EXPECT_TRUE(ofs.is_open());
        for (Index i {}; i < num_committed; ++i) {
            const auto key = make_key<KEY_WIDTH>(i);
            const auto value = random_string(random, 2, 15);
            db.insert(stob(key), stob(value));
            ofs << value << '\n';
        }
        db.commit();
    }

    puts(value_path.c_str());
    fflush(stdout);

    // Modify the database until we receive a signal or hit the operation limit.
    for (Index i {}; i < LIMIT; ++i) {
        const auto key = std::to_string(random.next_int(num_committed * 2));
        const auto value = random_string(random, 0, options.page_size / 2);
        db.insert(stob(key), stob(value));

        // Keep the database from getting too large.
        if (const auto info = db.info(); info.record_count() > max_database_size) {
            while (info.record_count() >= max_database_size / 2) {
                const auto cursor = db.find_minimum();
                CALICO_EXPECT_TRUE(cursor.is_valid());
                db.erase(cursor.key());
            }
        }
    }
    // We should never get here.
    return 1;
}
