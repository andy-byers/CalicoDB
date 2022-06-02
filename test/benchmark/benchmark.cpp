
#include <filesystem>
#include <locale>
#include <chrono>
#include <thread> 
#include "cub.h"
#include "tools.h"

namespace {

using namespace cub;

constexpr auto PATH = "/tmp/cub_benchmark";
constexpr Size FIELD_WIDTH {20};

struct BenchmarkParameters {
    Size num_replicants {};
    Size num_warmup_rounds {};
};

using Clock = std::chrono::system_clock;
using Work = std::vector<Record>;

struct InstanceParameters {
    std::function<void(Database&)> build;
    std::function<void(Database&)> setup;
    std::function<void(Database&)> run;
    std::string name;
    Size num_elements {};
};

struct InstanceResults {
    std::string name;
    double mean_elapsed {};
    Size num_elements {};
};

struct Comma: public std::numpunct<char>
{
    [[nodiscard]] auto do_grouping() const -> std::string override
    {
        return "\003";
    }
};

auto create(Options options)
{
    std::filesystem::remove(PATH);
    std::filesystem::remove(get_wal_path(PATH));
    return Database::open(PATH, options);
}

auto create_temp(Size page_size)
{
    return Database::temp(page_size);
}

auto report(const InstanceResults &results)
{
    const auto ops = static_cast<double>(results.num_elements) / results.mean_elapsed;
    std::cout << "| " << std::setw(FIELD_WIDTH) << std::left << results.name
              << " | " << std::setw(FIELD_WIDTH) << std::right << static_cast<Size>(ops) << " |\n";
}

auto build_common(Work &records, bool is_sequential)
{
    if (is_sequential)
        std::sort(records.begin(), records.end());
}

auto build_reads(Database &db, Work &records, bool is_sorted, bool is_reversed)
{
    if (db.get_info().record_count())
        return;

    build_common(records, is_sorted);

    if (is_reversed)
        std::reverse(records.begin(), records.end());

    for (const auto &[key, value]: records)
        db.insert(_b(key), _b(value));
}

auto build_erases(Database &db, Work &records, bool is_sequential)
{
    build_reads(db, records, is_sequential, false);
    db.commit();
}

auto run_baseline(Database &db)
{
    // Sleep for 1/10 seconds. The benchmark should report a little less than num_elements*10 operations per second.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

auto run_writes(Database &db, const Work &work)
{
    for (const auto &[key, value]: work)
        db.insert(_b(key), _b(value));
}

auto run_erases(Database &db, const Work &work)
{
    for (const auto &[key, value]: work)
        db.remove(_b(key));
}

auto run_read_rand(Database &db, const Work &work)
{
    auto cursor = db.get_cursor();
    for (const auto &[key, value]: work) {
        CUB_EXPECT_TRUE(cursor.find(_b(key)));
        (void)cursor.value();
    }
}

auto run_read_seq(Database &db, const Work &work)
{
    auto cursor = db.get_cursor();
    cursor.find_minimum();
    for (const auto &_: work) {
        (void)cursor.value();
        cursor.increment();
    }
}

auto run_read_rev(Database &db, const Work &work)
{
    auto cursor = db.get_cursor();
    cursor.find_maximum();
    for (const auto &_: work) {
        (void)cursor.value();
        cursor.decrement();
    }
}

auto setup_common(Database &db)
{
    db.abort();
}

class Runner {
public:
    explicit Runner(BenchmarkParameters param)
        : m_param {param} {}

    auto run(Database db, const InstanceParameters &param)
    {
        const auto [N, W] = m_param;
        double total {};

        param.build(db);

        for (Index r {}; r < N + W; ++r) {
            param.setup(db);
            const auto t1 = Clock::now();
            param.run(db);
            const auto t2 = Clock::now();

            if (r >= W) {
                std::chrono::duration<double> dt = t2 - t1;
                total += dt.count();
            }
        }
        total /= static_cast<double>(N);
        return InstanceResults {param.name, total, param.num_elements};
    }

private:
    BenchmarkParameters m_param;
};

} // <anonymous>

auto main(int argc, const char *argv[]) -> int
{
    using namespace cub;

    static constexpr auto num_warmup_rounds = 2;
    static constexpr auto num_replicants = 8;
    static constexpr auto num_elements = 20'000;
    Options options;

    auto records = RecordGenerator::generate_unique(num_elements);

    const InstanceParameters baseline {
        [](Database&) {},
        [](Database&) {},
        [](Database &db) {run_baseline(db);},
        "baseline (<200,000)",
        num_elements,
    };

    const InstanceParameters write_rand {
        [](Database&) {},
        [](Database &db) {setup_common(db);},
        [&records](Database &db) {run_writes(db, records);},
        "write_random",
        num_elements,
    };

    const InstanceParameters write_seq {
        [&records](Database&) {build_common(records, true);},
        [](Database &db) {setup_common(db);},
        [&records](Database &db) {run_writes(db, records);},
        "write_sequential",
        num_elements,
    };

    const InstanceParameters erase_rand {
        [&records](Database &db) {build_erases(db, records, false);},
        [](Database &db) {setup_common(db);},
        [&records](Database &db) {run_erases(db, records);},
        "erase_rand",
        num_elements,
    };

    const InstanceParameters erase_seq {
        [&records](Database &db) {build_erases(db, records, true);},
        [](Database &db) {setup_common(db);},
        [&records](Database &db) {run_erases(db, records);},
        "erase_seq",
        num_elements,
    };

    const InstanceParameters read_rand {
        [&records](Database &db) {build_reads(db, records, false, false);},
        [](Database&) {},
        [&records](Database &db) {run_read_rand(db, records);},
        "read_rand",
        num_elements,
    };

    const InstanceParameters read_seq {
        [&records](Database &db) {build_reads(db, records, true, false);},
        [](Database&) {},
        [&records](Database &db) {run_read_seq(db, records);},
        "read_seq",
        num_elements,
    };

    const InstanceParameters read_rev {
        [&records](Database &db) {build_reads(db, records, true, true);},
        [](Database&) {},
        [&records](Database &db) {run_read_rev(db, records);},
        "read_rev",
        num_elements,
    };

    BenchmarkParameters param {
        num_replicants,
        num_warmup_rounds,
    };
    Runner runner {param};

    Comma facet {};
    std::cout.imbue(std::locale(std::cout.getloc(), &facet));

    std::cout << "| Benchmark (Real DB)  | Result (ops/second)  |\n";
    std::cout << "|----------------------|----------------------|\n";

    report(runner.run(create(options), baseline));
    report(runner.run(create(options), write_rand));
    report(runner.run(create(options), write_seq));
    report(runner.run(create(options), read_rand));
    report(runner.run(create(options), read_seq));
    report(runner.run(create(options), read_rev));
    report(runner.run(create(options), erase_rand));
    report(runner.run(create(options), erase_seq));

    std::cout << '\n';
    std::cout << "| Benchmark (Temp DB)  | Result (ops/second)  |\n";
    std::cout << "|----------------------|----------------------|\n";

    report(runner.run(create_temp(options.page_size), write_rand));
    report(runner.run(create_temp(options.page_size), write_seq));
    report(runner.run(create_temp(options.page_size), read_rand));
    report(runner.run(create_temp(options.page_size), read_seq));
    report(runner.run(create_temp(options.page_size), read_rev));
    report(runner.run(create_temp(options.page_size), erase_rand));
    report(runner.run(create_temp(options.page_size), erase_seq));

    return 0;
}