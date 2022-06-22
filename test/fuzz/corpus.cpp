
#include <filesystem>
#include <fstream>
#include "fuzzers.h"

using namespace calico;
using namespace calico::fuzz;

auto generate_operation_seed(const std::string &path, Size num_operations)
{
    static constexpr Size LIMIT {80};
    OperationTransformer<LIMIT> transformer;
    Random random;

    decltype(transformer)::Decoded inputs(num_operations);
    for (auto &[key, value_size, operation]: inputs) {
        key = random.next_binary(random.next_int(1ULL, 255ULL));
        if (random.next_int(99ULL) < LIMIT) {
            operation = Operation::WRITE;
            value_size = random.next_int(0ULL, 255ULL);
        } else {
            operation = Operation::ERASE;
        }
    }

    std::ofstream ofs(path, std::ios::trunc);
    ofs << transformer.encode(inputs);
}

auto generate_wal_reader_seed(const std::string &path, Size block_size, Size num_records)
{
    const auto database_path = path + "_";
    {
        Options options;
        options.page_size = block_size;
        options.block_size = block_size;
        auto db = Database::open(database_path, options);
        for (Index i {}; i < num_records; ++i) {
            const auto data = std::to_string(i);
            db.write(stob(data), stob(data + data));
        }
        std::filesystem::copy(get_wal_path(database_path), path);
    }
    std::filesystem::remove(database_path);
}

auto main(int, const char*[]) -> int
{
    const std::string OPERATION_DIR {"operation_corpus"};
    const std::string WAL_READER_DIR {"wal_reader_corpus"};
    std::filesystem::create_directory(OPERATION_DIR);
    std::filesystem::create_directory(WAL_READER_DIR);
    for (int i {}; i < 100; ++i) {
        generate_operation_seed(OPERATION_DIR + "/" + std::to_string(i), 500);
        generate_wal_reader_seed(WAL_READER_DIR + "/" + std::to_string(i), 0x200, 500);
    }
}