
#include <filesystem>
#include <chrono>
#include <thread>
#include <spdlog/fmt/fmt.h>
#include "calico/calico.h"
#include "tools.h"


namespace {

using namespace calico;
constexpr auto PATH = "/tmp/calico_benchmark";
constexpr Size BASELINE_MULTIPLIER {10};
constexpr Size PAGE_SIZE {0x8000};
constexpr Size CACHE_SIZE {0x400000};
constexpr Size KEY_SIZE {16};
constexpr Size VALUE_SIZE {100};
constexpr Size LARGE_VALUE_SIZE {100'000};
Options options {
    PAGE_SIZE,              // Page size
    PAGE_SIZE,              // Block size
    CACHE_SIZE / PAGE_SIZE, // Frame count
    0666,                   // Permissions
    false,                  // Use transactions (can be changed commandline)
    false,                  // Use direct I/O
    "/dev/null",
    0,
};

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

auto create()
{
    std::filesystem::remove(PATH);
    std::filesystem::remove(get_wal_path(PATH));
    return Database::open(PATH, options);
}

auto create_temp()
{
    return Database::temp(options);
}

auto build_common(Work &records, bool is_sequential)
{
    if (is_sequential)
        std::sort(begin(records), end(records));
}

auto build_reads(Database &db, Work &records, bool is_sorted, bool is_reversed)
{
    if (db.info().record_count() == records.size())
        return;

    build_common(records, is_sorted);

    if (is_reversed)
        std::reverse(begin(records), end(records));

    for (const auto &[key, value]: records)
        db.insert(stob(key), stob(value));
}

auto build_erases(Database &db, Work &records, bool is_sequential)
{
    build_reads(db, records, is_sequential, false);
}

auto run_baseline(Database&)
{
    // Sleep for 1/BASELINE_MULTIPLIER seconds. The benchmark should report a little less than num_elements*10 operations per second.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000 / BASELINE_MULTIPLIER));
}

auto run_writes(Database &db, const Work &work)
{
    for (const auto &[key, value]: work)
        CALICO_EXPECT_TRUE(db.insert(stob(key), stob(value)));
    db.commit();
}

auto run_erases(Database &db, const Work &work)
{
    for (const auto &[key, value]: work)
        CALICO_EXPECT_TRUE(db.erase(stob(key)));
    db.commit();
}

auto run_read_rand(Database &db, const Work &work)
{
    std::string k, v;
    for (const auto &[key, value]: work) {
        auto cursor = db.find(stob(key));
        CALICO_EXPECT_TRUE(cursor.is_valid());
        k = btos(cursor.key());
        v = cursor.value();
    }
}

auto run_read_seq(Database &db, const Work &work)
{
    std::string k, v;
    auto cursor = db.find_minimum();
    for (const auto &w: work) {
        (void)w;
        k = btos(cursor.key());
        v = cursor.value();
        cursor.increment();
    }
}

auto run_read_rev(Database &db, const Work &work)
{
    std::string k, v;
    auto cursor = db.find_maximum();
    for (const auto &w: work) {
        (void)w;
        k = btos(cursor.key());
        v = cursor.value();
        cursor.decrement();
    }
}

auto setup_common(Database &db)
{
    const auto is_temp = db.info().is_temp();
    db.~Database();
    db = is_temp ? create_temp() : create();
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

auto report(const InstanceResults &results)
{
    const auto ops = static_cast<double>(results.num_elements) / results.mean_elapsed;
    fmt::print("| {:<32} | {:>32d} |\n", results.name, static_cast<Size>(ops));
}

} // <anonymous>

auto show_usage()
{
    fmt::print("usage: benchmark [-rt]\n");
    fmt::print("\n");
    fmt::print(" Parameters\n");
    fmt::print("============\n");
    fmt::print("  -r: Show only the database benchmarks\n");
    fmt::print("  -t: Show only the in-memory database benchmarks\n");
    fmt::print("  -b: Show the baselines\n");
}

auto main(int argc, const char *argv[]) -> int
{
    using namespace calico;

    bool real_only {};
    bool temp_only {};
    bool uses_xact {};
    bool show_baseline {};

    for (int i {1}; i < argc; ++i) {
        const std::string arg {argv[i]};
        if (arg == "-b") {
            show_baseline = true;
        } else if (arg == "-r") {
            real_only = true;
        } else if (arg == "-t") {
            temp_only = true;
        } else if (arg == "-T") {
            options.use_transactions = true;
            uses_xact = true;
        }
    }
    if (real_only && temp_only) {
        fmt::print(stderr, "Error: '-r' and 't' arguments are mutually exclusive\n");
        show_usage();
        return 1;
    }

    static constexpr auto num_warmup_rounds = 2;
    static constexpr auto num_replicants = 8;
    static constexpr auto num_elements = 40'000;
//    static constexpr auto num_large_elements = 1'000;

    CALICO_EXPECT_STATIC(num_elements * (KEY_SIZE+VALUE_SIZE) > CACHE_SIZE,
                      "Use more or larger records. Benchmark is unfair.");

    const auto seed = Clock::now().time_since_epoch().count();
    Random random {static_cast<unsigned>(seed)};

    RecordGenerator::Parameters generator_param;
    generator_param.mean_key_size = KEY_SIZE;
    generator_param.mean_value_size = VALUE_SIZE;
    generator_param.spread = 0;
    RecordGenerator generator {generator_param};
    auto records = generator.generate(random, num_elements);

//    generator_param.mean_key_size = KEY_SIZE;
//    generator_param.mean_value_size = LARGE_VALUE_SIZE;
//    generator_param.spread = 0;
//    RecordGenerator large_generator {generator_param};
//    auto large_records = generator.generate(random, num_large_elements);
//    (void)large_records;
    (void)LARGE_VALUE_SIZE;

    // We only erase half of the records for one group of tests. The remove() routine gets faster when the tree is small,
    // so we expect those tests to produce less operations per second than their counterparts that empty out the tree.
    const std::vector<Record> half_records {records.begin(), records.begin() + num_elements/2};

    const InstanceParameters baseline {
        [](Database&) {},
        [](Database&) {},
        [](Database &db) {run_baseline(db);},
        "<baseline>",
        num_elements,
    };

    std::vector<InstanceParameters> instances {
        {
            [](Database&) {},
            [](Database &db) {setup_common(db);},
            [&records](Database &db) {run_writes(db, records);},
            "write_rand",
            num_elements,
        },
        {
            [&records](Database&) {build_common(records, true);},
            [](Database &db) {setup_common(db);},
            [&records](Database &db) {run_writes(db, records);},
            "write_seq",
            num_elements,
        },
        {
            [&records](Database &db) {build_reads(db, records, false, false);},
            [](Database&) {},
            [&records](Database &db) {run_read_rand(db, records);},
            "read_rand",
            num_elements,
        },
        {
            [&records](Database &db) {build_reads(db, records, true, false);},
            [](Database&) {},
            [&records](Database &db) {run_read_seq(db, records);},
            "read_seq",
            num_elements,
        },
        {
            [&records](Database &db) {build_reads(db, records, true, true);},
            [](Database&) {},
            [&records](Database &db) {run_read_rev(db, records);},
            "read_rev",
            num_elements,
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, false);},
            [&half_records](Database &db) {run_erases(db, half_records);},
            "erase_rand",
            half_records.size(),
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, true);},
            [&half_records](Database &db) {run_erases(db, half_records);},
            "erase_seq",
            half_records.size(),
        },
    };

    BenchmarkParameters param {
        num_replicants,
        num_warmup_rounds,
    };
    Runner runner {param};

    const auto field_1a = "Name";
    const auto field_1b = "Name (In-Memory DB)";

    const auto print_filler_row = []() {
        return fmt::print("|{:-<34}|{:->34}|\n", ':', ':');
    };

    const auto print_header_row = [&field_1a, &field_1b](bool is_temp) {
        return fmt::print("| {:<32} | {:>32} |\n", is_temp ? field_1b : field_1a, "Result (ops/sec)");
    };

    const auto run_real_db_benchmarks = [&] {
        print_header_row(false);
        print_filler_row();
        for (const auto &instance: instances) {
            report(runner.run(create(), instance));

            // Attempt to mess up branch prediction.
            random.shuffle(records);
        }
    };

    const auto run_temp_db_benchmarks = [&] {
        print_header_row(true);
        print_filler_row();
        for (const auto &instance: instances) {
            report(runner.run(create_temp(), instance));
            random.shuffle(records);
        }
    };

    if (show_baseline) {
        instances.insert(instances.begin(), baseline);
        instances.emplace_back(baseline);
        fmt::print("Baseline should be <= {}\n", num_elements * BASELINE_MULTIPLIER);
    }

    if (!temp_only) {
        fmt::print("### Benchmark Results {}\n", uses_xact ? "" : "(w/o Transactions)");
        run_real_db_benchmarks();
        putchar('\n');
    }

    if (!real_only) {
        fmt::print("### Benchmark Results (In-Memory Database{})\n", uses_xact ? "" : " w/o Transactions");
        run_temp_db_benchmarks();
        putchar('\n');
    }

    return 0;
}
