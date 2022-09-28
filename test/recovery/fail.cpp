#include <filesystem>
#include <fstream>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace calico;
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
    static constexpr Size XACT_SIZE {100};

    if (argc != 4) {
        show_usage();
        return 1;
    }
    const fs::path path {argv[1]};
    const auto value_path = path / "values";
    const auto num_committed = std::stoul(argv[2]);
    const auto max_database_size = num_committed * 5;
    Random random {static_cast<std::uint32_t>(std::stoul(argv[3]))};

    std::error_code ignore;
    std::filesystem::remove_all(path, ignore);

    // Use small pages and few frames to cause lots of stealing.
    Options options;
    options.page_size = 0x200;
    options.frame_count = 16;
//    options.log_level = spdlog::level::info;
    Database db;
    expect_ok(db.open(path.string(), options));
    {
        std::ofstream ofs {value_path, std::ios::trunc};
        CALICO_EXPECT_TRUE(ofs.is_open());

        for (Size i {}; i < num_committed; i += XACT_SIZE) {
            auto xact = db.transaction();
            for (Size j {}; j < XACT_SIZE; ++j) {
                const auto key = make_key<KEY_WIDTH>(i + j);
                const auto value = random.get<std::string>('a', 'z', random.get(10UL, 100UL));
                expect_ok(db.insert(stob(key), stob(value)));
                ofs << value << '\n';
            }
            expect_ok(xact.commit());
        }
    }

    puts(value_path.c_str());
    fflush(stdout);

    // Modify the database until we receive a signal or hit the operation limit.
    auto xact = db.transaction();
    for (Size i {}; i < LIMIT; ++i) {
        const auto key = std::to_string(random.get(num_committed * 2));
        const auto value = random.get<std::string>('\x00', '\xFF', random.get(options.page_size / 2));
        expect_ok(db.insert(stob(key), stob(value)));

        // Keep the database from getting too large.
        if (const auto info = db.info(); info.record_count() > max_database_size) {
            while (info.record_count() >= max_database_size / 2) {
                const auto cursor = db.first();
                CALICO_EXPECT_TRUE(cursor.is_valid());
                expect_ok(db.erase(cursor.key()));
            }
        }
    }
    // We should never get here.
    return 1;
}
