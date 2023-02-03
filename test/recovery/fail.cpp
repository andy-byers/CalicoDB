#include <filesystem>
#include <fstream>
#include "calico/calico.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace Calico;
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

    Options options;
    options.page_size = 0x200;
    options.page_cache_size = 64 * options.page_size;
    options.wal_buffer_size = 64 * options.page_size;

    Database *db;
    expect_ok(Database::open(path.string(), options, &db));
    {
        std::ofstream ofs {value_path, std::ios::trunc};
        CALICO_EXPECT_TRUE(ofs.is_open());

        for (Size i {}; i < num_committed; i += XACT_SIZE) {
            for (Size j {}; j < XACT_SIZE; ++j) {
                const auto key = make_key<KEY_WIDTH>(i + j);
                const auto value = random.get<std::string>('a', 'z', random.get(10UL, 100UL));
                expect_ok(db->put(key, value));
                ofs << value << '\n';
            }
            expect_ok(db->commit());
        }
    }

    puts(value_path.c_str());
    fflush(stdout);

    // Modify the database until we receive a signal or hit the operation limit.
    for (Size i {}; i < LIMIT; ++i) {
        const auto key = std::to_string(random.get(num_committed * 2));
        const auto value = random.get<std::string>('\x00', '\xFF', random.get(options.page_size / 2));
        expect_ok(db->put(key, value));

        // Keep the database from getting too large.
        if (const auto property = db->get_property("record_count"); !property.empty()) {
            const auto record_count = std::stoi(property);
            if (record_count > max_database_size) {
                auto *cursor = db->new_cursor();
                for (Size j {}; j < record_count / 2; ++j) {
                    cursor->seek_first();
                    CALICO_EXPECT_TRUE(cursor->is_valid());
                    expect_ok(db->erase(cursor->key()));
                }
                delete cursor;
            }
        }
    }
    // We should never get here.
    delete db;
    return 1;
}
