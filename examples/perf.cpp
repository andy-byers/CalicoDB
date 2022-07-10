
#include "calico/calico.h"
#include "random.h"
#include "tools.h"
#include <filesystem>
#include <vector>

namespace {

constexpr auto PATH = "/tmp/calico_perf";
namespace cco = calico;

auto run_inserts(cco::Database &db, const std::vector<cco::Record> &records)
{
    for (const auto &record: records)
        db.insert(record);

    printf("%lf\n", db.info().cache_hit_ratio());
}

} // namespace

auto main(int, const char *[]) -> int
{
    std::error_code error;
    std::filesystem::remove_all(PATH, error);

    try {
        cco::Options options;
        options.frame_count = 512;
        cco::RecordGenerator::Parameters param;
        param.mean_key_size = 12;
        param.mean_value_size = 800;
        param.spread = 4;
        param.is_sequential = true;
        cco::RecordGenerator generator {param};
        cco::Random random;
        auto records = generator.generate(random, 1'000'000);
        auto db = cco::Database::temp(options);
        run_inserts(db, records);

    } catch (const cco::CorruptionError &error) {
        printf("CorruptionError: %s\n", error.what());
    } catch (const std::invalid_argument &error) {
        printf("std::invalid_argument: %s\n", error.what());
    } catch (const std::system_error &error) {
        printf("std::system_error (errno=%d): %s\n", error.code().value(), error.what());
    } catch (const std::exception &error) {
        printf("std::exception: %s\n", error.what());
    } catch (...) {
        puts("...");
    }
    return 0;
}
