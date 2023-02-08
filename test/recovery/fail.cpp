#include <filesystem>
#include <fstream>
#include <iostream>
#include "calico/calico.h"
#include "utils/utils.h"
#include "tools.h"

#ifdef NDEBUG
#  error "This test must run with assertions enabled"
#endif

using namespace Calico;
namespace fs = std::filesystem;

static constexpr Size KEY_WIDTH {12};
static constexpr Size LIMIT {10'000'000};

static auto show_usage()
{
    std::cout << "usage: fail PATH N\n";
    std::cout << "  Parameters\n";
    std::cout << "==============\n";
    std::cout << "PATH: Path at which to create the database\n";
    std::cout << "N: Number of records committed to the database\n";
}

static auto generate_lowercase(const Tools::RandomGenerator &random, Size n)
{
    auto data = random.Generate(n).to_string();
    for (auto &chr: data) {
        if (chr < 'a' || 'z' < chr) {
            chr = char('a' + std::uint8_t(chr)%('z'-'a'));
        }
    }
    return data;
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
    Tools::RandomGenerator random {2 * 1'024 * 1'024};

    std::error_code ignore;
    std::filesystem::remove_all(path, ignore);

    Options options;
    options.page_size = 0x200;
    options.page_cache_size = 32 * options.page_size;
    options.wal_buffer_size = 32 * options.page_size;

    Database *db;
    CALICO_EXPECT_TRUE(Database::open(path.string(), options, &db).is_ok());
    {
        std::ofstream ofs {value_path, std::ios::trunc};
        CALICO_EXPECT_TRUE(ofs.is_open());

        for (Size i {}; i < num_committed; i += XACT_SIZE) {
            for (Size j {}; j < XACT_SIZE; ++j) {
                const auto key = Tools::integral_key<KEY_WIDTH>(i + j);
                const auto value = generate_lowercase(random, random.Next<Size>(10, 100));
                CALICO_EXPECT_TRUE(db->put(key, value).is_ok());
                ofs << value << '\n';
            }
            CALICO_EXPECT_TRUE(db->commit().is_ok());
        }
    }

    puts(value_path.c_str());
    fflush(stdout);

    // Modify the database until we receive a signal or hit the operation limit.
    for (Size i {}; i < LIMIT; ++i) {
        const auto key = std::to_string(random.Next<Size>(num_committed * 2));
        const auto value = generate_lowercase(random, random.Next<Size>(options.page_size / 2));
        CALICO_EXPECT_TRUE(db->put(key, value).is_ok());

        // Keep the database from getting too large.
        const auto property = db->get_property("calico.count.records");
        CALICO_EXPECT_FALSE(property.empty());
        const auto record_count = std::stoi(property);
        if (record_count > max_database_size) {
            auto *cursor = db->new_cursor();
            for (Size j {}; j < record_count / 2; ++j) {
                cursor->seek_first();
                CALICO_EXPECT_TRUE(cursor->is_valid());
                CALICO_EXPECT_TRUE(db->erase(cursor->key()).is_ok());
            }
            delete cursor;
        }
    }
    // We should never get here.
    delete db;
    return 1;
}
