
#include <filesystem>
#include <vector>
#include "calico/calico.h"
#include "../src/utils/utils.h"

using namespace calico;
static constexpr Size PAGE_SIZE {0x200};
static constexpr Size NUM_RECORDS {50'000};

auto run(Database db)
{
    for (Index i {}; i < NUM_RECORDS; ++i) {
        const auto s = std::to_string(i);
        db.write(stob(s), stob(s + s));
//        if (i % 25'000 == 0)
//            db.commit();
    }
//    for (Index i {}; i < NUM_RECORDS; ++i) {
//        const auto s = std::to_string(i);
//        db.erase(stob(s));
//        if (i % 25'000 == 0)
//            db.commit();
//    }
//    assert(db.get_info().record_count() == 0);
}

auto main(int, const char*[]) -> int
{
    try {
        Options options;
        options.use_transactions = false;
        options.log_path = "/tmp/calico_logger";
        options.log_level = 1;
        std::filesystem::remove("/tmp/calico_example");
        std::filesystem::remove(get_wal_path("/tmp/calico_example"));
        auto db = Database::open("/tmp/calico_example", options);
        run(std::move(db));
    } catch (const CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const IOError &error) {
        printf("IOError: %s\n", error.what());
    } catch (const std::invalid_argument &error) {
        printf("std::invalid_argument: %s\n", error.what());
    } catch (const std::system_error &error) {
        printf("std::system_error: %s\n", error.what());
    } catch (const std::exception &error) {
        printf("std::exception: %s\n", error.what());
    } catch (...) {
        puts("\n");
    }
    return 0;
}