
#include <vector>
#include <cub/cub.h>

using namespace cub;
static constexpr Size PAGE_SIZE {0x200};
static constexpr Size NUM_RECORDS {20'000};

auto run(Database db, const std::vector<Record> &records)
{
    for (const auto &record: records)
        db.write(record);
}

auto main(int, const char*[]) -> int
{
    std::vector<Record> records(NUM_RECORDS);
    for (auto &[key, value]: records) {
        key = std::to_string(rand());
        value = key;
        for (Index i {}; i < 10; ++i)
            value += key;
    }
    try {
        Options options;
        auto db = Database::temp(PAGE_SIZE);
        run(std::move(db), records);
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