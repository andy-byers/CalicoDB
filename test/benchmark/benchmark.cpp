
#include <filesystem>
#include <locale>
#include <chrono>
#include <thread> 
#include "cub/cub.h"
#include "tools.h"

namespace {

using namespace cub;

constexpr auto PATH = "/tmp/cub_benchmark";
constexpr Size FIELD_WIDTH {24};
constexpr Size BASELINE_MULTIPLIER {10};
Options options;

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

auto create()
{
    std::filesystem::remove(PATH);
    std::filesystem::remove(get_wal_path(PATH));
    options.frame_count = 128;
    options.page_size = 0x4000;
    options.block_size = options.page_size * 2;
    return Database::open(PATH, options);
}

auto create_temp(Size page_size)
{
    return Database::temp(page_size);
}

auto build_common(Work &records, bool is_sequential)
{
    if (is_sequential)
        std::sort(records.begin(), records.end());
}

auto build_reads(Database &db, Work &records, bool is_sorted, bool is_reversed)
{
    if (db.get_info().record_count() == records.size())
        return;

    build_common(records, is_sorted);

    if (is_reversed)
        std::reverse(records.begin(), records.end());

    auto batch = db.get_batch();
    for (const auto &[key, value]: records)
        batch.write(_b(key), _b(value));
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
        CUB_EXPECT_TRUE(db.write(_b(key), _b(value)));
    db.commit();
}

auto run_batch_writes(Database &db, const Work &work)
{
    auto batch = db.get_batch();
    for (const auto &[key, value]: work)
        CUB_EXPECT_TRUE(batch.write(_b(key), _b(value)));
}

auto run_erases(Database &db, const Work &work)
{
    for (const auto &[key, value]: work)
        CUB_EXPECT_TRUE(db.erase(_b(key)));
    db.commit();
}

auto run_batch_erases(Database &db, const Work &work)
{
    auto batch = db.get_batch();
    for (const auto &[key, value]: work)
        CUB_EXPECT_TRUE(batch.erase(_b(key)));
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
    for (const auto &w: work) {
        (void)w;
        (void)cursor.value();
        cursor.increment();
    }
}

auto run_read_rev(Database &db, const Work &work)
{
    auto cursor = db.get_cursor();
    cursor.find_maximum();
    for (const auto &w: work) {
        (void)w;
        (void)cursor.value();
        cursor.decrement();
    }
}

auto setup_common(Database &db)
{
    db.~Database();
    db = create();
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

auto make_row(char cap)
{
    return std::string(1, cap) + '\n';
}

template<class ...Rest> auto make_row(char left_cap, const std::string &field, char separator, Rest ...rest)
{
    return left_cap + field + make_row(separator, rest...);
}

auto report(const InstanceResults &results)
{
    const auto ops = static_cast<double>(results.num_elements) / results.mean_elapsed;
    std::cout << "| " << std::setw(FIELD_WIDTH) << std::left << results.name
              << " | " << std::setw(FIELD_WIDTH) << std::right << static_cast<Size>(ops) << " |\n";
}

} // <anonymous>

auto show_usage()
{
    std::cout << "usage: benchmark [-rt]\n";
    std::cout << "\n";
    std::cout << " Parameters\n";
    std::cout << "============\n";
    std::cout << "  -r: Show only the database benchmarks\n";
    std::cout << "  -t: Show only the in-memory database benchmarks\n";
    std::cout << "  -b: Show the baselines\n";
}

auto main(int argc, const char *argv[]) -> int
{
    using namespace cub;

    bool real_only {};
    bool temp_only {};
    bool show_baseline {};

    for (int i {1}; i < argc; ++i) {
        const std::string arg {argv[i]};
        if (arg == "-r") {
            real_only = true;
        } else if (arg == "-t") {
            temp_only = true;
        } else if (arg == "-b") {
            show_baseline = true;
        }
    }
    if (real_only && temp_only) {
        std::cerr << "Error: '-r' and 't' arguments are mutually exclusive\n";
        show_usage();
        return 1;
    }

    static constexpr auto num_warmup_rounds = 2;
    static constexpr auto num_replicants = 8;
    static constexpr auto num_elements = 10'000;

    const auto seed = Clock::now().time_since_epoch().count();
    Random random {static_cast<unsigned>(seed)};

    RecordGenerator::Parameters generator_param;
    generator_param.mean_key_size = 15;
    generator_param.mean_value_size = 100;
    generator_param.spread = 5;
    RecordGenerator generator {generator_param};
    auto records = generator.generate(random, num_elements);

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
            [](Database&) {},
            [](Database &db) {setup_common(db);},
            [&records](Database &db) {run_batch_writes(db, records);},
            "batch_write_rand",
            num_elements,
        },
        {
            [&records](Database&) {build_common(records, true);},
            [](Database &db) {setup_common(db);},
            [&records](Database &db) {run_batch_writes(db, records);},
            "batch_write_seq",
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
            [&records](Database &db) {run_erases(db, records);},
            "erase_all_rand",
            num_elements,
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, true);},
            [&records](Database &db) {run_erases(db, records);},
            "erase_all_seq",
            num_elements,
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, false);},
            [&half_records](Database &db) {run_erases(db, half_records);},
            "erase_half_rand",
            half_records.size(),
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, true);},
            [&half_records](Database &db) {run_erases(db, half_records);},
            "erase_half_seq",
            half_records.size(),
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, false);},
            [&records](Database &db) {run_batch_erases(db, records);},
            "batch_erase_all_rand",
            num_elements,
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, true);},
            [&records](Database &db) {run_batch_erases(db, records);},
            "batch_erase_all_seq",
            num_elements,
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, false);},
            [&half_records](Database &db) {run_batch_erases(db, half_records);},
            "batch_erase_half_rand",
            half_records.size(),
        },
        {
            [](Database&) {},
            [&records](Database &db) {build_erases(db, records, true);},
            [&half_records](Database &db) {run_batch_erases(db, half_records);},
            "batch_erase_half_seq",
            half_records.size(),
        },
    };

    BenchmarkParameters param {
        num_replicants,
        num_warmup_rounds,
    };
    Runner runner {param};

    Comma facet {};
    std::cout.imbue(std::locale(std::cout.getloc(), &facet));

    static constexpr auto make_field_name = [](const std::string &name) {
        CUB_EXPECT_LT(name.size(), FIELD_WIDTH);
        return " " + name + std::string(FIELD_WIDTH - name.size() + 1, ' ');
    };
    const auto field_1a {make_field_name("Name")};
    const auto field_1b {make_field_name("Name (In-Memory DB)")};
    const auto field_2 {make_field_name("Result (ops/second)")};

    const auto make_filler_row = [&field_2](const std::string &first, char c) {
        return make_row(c, std::string(first.size(), '-'), c, std::string(field_2.size(), '-'), c);
    };

    const auto make_header_row = [&field_2](const std::string &first) {
        return make_row('|', first, '|', field_2, '|');
    };

    const auto run_real_db_benchmarks = [&] {
        std::cout << make_filler_row(field_1a, '.');
        std::cout << make_header_row(field_1a);
        std::cout << make_filler_row(field_1a, '|');
        for (const auto &instance: instances) {
            report(runner.run(create(), instance));
            random.shuffle(records); // Attempt to mess up branch prediction.
        }
        std::cout << make_filler_row(field_1a, '\'') << '\n';
    };

    const auto run_temp_db_benchmarks = [&] {
        std::cout << make_filler_row(field_1b, '.');
        std::cout << make_header_row(field_1b);
        std::cout << make_filler_row(field_1b, '|');
        for (const auto &instance: instances) {
            report(runner.run(create_temp(options.page_size), instance));
            random.shuffle(records);
        }
        std::cout << make_filler_row(field_1a, '\'') << '\n';
    };

    if (show_baseline) {
        instances.insert(instances.begin(), baseline);
        instances.emplace_back(baseline);
        std::cout << "Baseline should be <= " << num_elements * BASELINE_MULTIPLIER << "\n\n";
    }

    if (!temp_only)
        run_real_db_benchmarks();

    if (!real_only)
        run_temp_db_benchmarks();

    return 0;
}