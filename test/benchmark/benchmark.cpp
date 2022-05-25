
#include <filesystem>
#include <chrono>
#include "cub.h"
#include "tools.h"

namespace {
using namespace cub;
constexpr auto PATH = "/tmp/cub_benchmark";
constexpr Size PAGE_SIZE = 0x8000;
constexpr Size MEAN_KEY_SIZE = 5;
constexpr Size MEAN_VALUE_SIZE = 20;

auto run_benchmark(Database db, Size n)
{
    RecordGenerator::Parameters param;
    param.min_key_size = MEAN_KEY_SIZE - MEAN_KEY_SIZE/2;
    param.max_key_size = MEAN_KEY_SIZE + MEAN_KEY_SIZE/2;
    param.min_key_size += param.min_key_size == 0;
    param.min_value_size = MEAN_VALUE_SIZE - MEAN_VALUE_SIZE/2;
    param.max_value_size = MEAN_VALUE_SIZE + MEAN_VALUE_SIZE/2;
    auto records = RecordGenerator::generate(n, param);

    using namespace std::chrono;
    const auto t0 = system_clock::now();
    for (const auto &[key, value]: records) {
//        printf("insert size = %zu\n", key.size()+value.size()+Cell::MAX_HEADER_SIZE+CELL_POINTER_SIZE);
        db.insert(_b(key), _b(value));
    }
    db.commit();
    const auto t1 = system_clock::now();
    const std::chrono::duration<double> dt = t1 - t0;
    printf("%lf", static_cast<double>(n) / dt.count());
}

} // <anonymous>



auto main(int argc, const char *argv[]) -> int
{
    using namespace cub;

    Size n {500'000};
    if (argc == 2)
        n = static_cast<Size>(std::stoi(argv[1]));

    cub::Options options;
    options.page_size = PAGE_SIZE;
    options.block_size = PAGE_SIZE;
    std::filesystem::remove(PATH);

    try {
        auto db = cub::Database::open(PATH, options);
        run_benchmark(std::move(db), n);
    } catch (const cub::SystemError &error) {
        // ...
    } catch (const cub::CorruptionError &error) {
        // ...
    } catch (const cub::Exception &error) {
        // ...
    } catch (...) {
        // ...
    }
    return 0;
}