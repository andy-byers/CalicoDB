/////**
//// *
//// */
////
////#include "calico/calico.h"
////#include "spdlog/fmt/fmt.h"
////
////namespace {
////
////namespace cco = calico;
////
////auto maybe_throw(const cco::Status &s) -> cco::Status
////{
////    if (!s.is_ok())
////        throw std::runtime_error {s.what()};
////    return s;
////}
////
////class PartitionDatabase {
////public:
////    PartitionDatabase(const std::string &path, cco::Size label_width)
////        : m_label_width {label_width}
////    {
////        maybe_throw(m_db.open(path));
////
////        const auto max_key_size = m_db.info().maximum_key_size();
////        if (label_width >= max_key_size)
////            throw std::invalid_argument {"maximum label width is too large"};
////    }
////
////    [[nodiscard]]
////    auto maximum_key_size() const -> cco::Size
////    {
////        return m_db.info().maximum_key_size() - m_label_width;
////    }
////
////private:
////    cco::Database m_db;
////    cco::Size m_label_width {};
////};
////
////class Partition {
////public:
////
////private:
////
////};
////
////} // namespace
////
////
//
//
//#include "calico/calico.h"
//#include <chrono>
//#include <filesystem>
//#include <vector>
//#include <spdlog/fmt/fmt.h>
//
//namespace {
//
//constexpr auto PATH = "/tmp/calico_usage";
//
//#define USAGE_ASSERT_OK(status, message) \
//    do { \
//        if (!(status).is_ok()) { \
//            fmt::print(__FILE__ ": {}\n", (message)); \
//            fmt::print(__FILE__ ": (reason) {}\n", (status).what()); \
//            std::exit(EXIT_FAILURE); \
//        } \
//    } while (0)
//
//auto bytes_objects()
//{
//    auto function_taking_a_bytes_view = [](calico::BytesView) {};
//
//    std::string data {"Hello, world!"};
//
//    // Construct slices from a string. The string still owns the memory, the slices just refer
//    // to it.
//    calico::Bytes b {data.data(), data.size()};
//    calico::BytesView v {data.data(), data.size()};
//
//    // Convenience conversion from a string.
//    const auto from_string = calico::stob(data);
//
//    // Convenience conversion back to a string. This operation may allocate heap memory.
//    assert(calico::btos(from_string) == data);
//
//    // Implicit conversions from `Bytes` to `BytesView` are allowed.
//    function_taking_a_bytes_view(b);
//
//    // advance() moves the start of the slice forward and truncate moves the end of the slice
//    // backward.
//    b.advance(7).truncate(5);
//
//    // Comparisons.
//    assert(calico::compare_three_way(b, v) != calico::ThreeWayComparison::EQ);
//    assert(b == calico::stob("world"));
//    assert(b.starts_with(calico::stob("wor")));
//
//    // Bytes objects can modify the underlying string, while BytesView objects cannot.
//    b[0] = '\xFF';
//    assert(data[7] == '\xFF');
//}
//
//auto reads_and_writes(calico::Database &db)
//{
//    static constexpr auto setup_error_message = "cannot setup \"reads_and_writes\" example";
//    USAGE_ASSERT_OK(db.insert("2000-10-23 14:23:05", ""), setup_error_message);
//    USAGE_ASSERT_OK(db.insert("2000-04-09 09:03:34", ""), setup_error_message);
//    USAGE_ASSERT_OK(db.insert("2000-11-01 21:15:45", ""), setup_error_message);
//    USAGE_ASSERT_OK(db.insert("2000-09-17 02:54:32", ""), setup_error_message);
//
//    // To insert a new record, we write the following:
//    auto s = db.insert("2000-02-06 12:32:19", "");
//
//    // Keys are unique in a Calico DB database, so inserting a record that already exists
//    // will overwrite the current value.
//
//    //
//}
//
//auto updating_a_database(calico::Database &db)
//{
//    struct Record {
//        std::string key;
//        std::string value;
//    };
//    std::vector<Record> records {
//        {"bengal", "short;spotted,marbled,rosetted"},
//        {"turkish vankedisi", "long;white"},
//        {"moose", "???"},
//        {"abyssinian", "short;ticked tabby"},
//        {"russian blue", "short;blue"},
//        {"american shorthair", "short;all"},
//        {"badger", "???"},
//        {"manx", "short,long;all"},
//        {"chantilly-tiffany", "long;solid,tabby"},
//        {"cyprus", "..."},
//    };
//
//    // Insert some records.
//    for (const auto &[key, value]: records)
//        assert(db.insert(key, value).is_ok());
//
//    // Keys are unique, so inserting a record with an existing key will modify the
//    // existing value.
//    assert(db.insert("cyprus", "all;all").is_ok());
//
//    // Erase a record by key.
//    assert(db.erase("badger").is_ok());
//
//    // Erase a record using a cursor (see "Querying a Database" below).
//    assert(db.erase(db.find_exact("moose")).is_ok());
//}
//
//auto querying_a_database(calico::Database &db)
//{
//    static constexpr auto target = "russian blue";
//    const auto key = calico::stob(target);
//
//    // find_exact() looks for a record that compares equal to the given key and returns a cursor
//    // pointing to it.
//    auto cursor = db.find_exact(key);
//
//    // If the cursor is valid (i.e. is_valid() returns true) we are safe to use any of the getter
//    // methods.
//    assert(cursor.is_valid());
//    assert(cursor.key() == key);
//    assert(cursor.value() == "short;blue");
//
//    // If we cannot find an exact match, an invalid cursor will be returned.
//    assert(not db.find_exact("not found").is_valid());
//
//    // If a cursor encounters an error at any point, it will also become invalidated. In this case,
//    // it will modify its status (returned by cursor.status()) to contain information about the error.
//    assert(db.find_exact("").status().is_invalid_argument());
//
//    // find() returns a cursor on the first record that does not compare less than the given key.
//    const auto prefix = key.copy().truncate(key.size() / 2);
//    assert(db.find(prefix).key() == cursor.key());
//
//    // Cursors can be used for range queries. They can traverse the database in sequential order,
//    // or in reverse sequential order.
//    for (auto c = db.first(); c.is_valid(); ++c) {}
//    for (auto c = db.last(); c.is_valid(); --c) {}
//
//    // They also support equality comparison.
//    if (const auto boundary = db.find_exact(key); boundary.is_valid()) {
//        for (auto c = db.first(); c.is_valid() && c != boundary; ++c) {}
//        for (auto c = db.last(); c.is_valid() && c != boundary; --c) {}
//    }
//}
//
//auto deleting_a_database(calico::Database db)
//{
//    // We can delete a database by passing ownership to the following static method.
//    assert(calico::Database::destroy(std::move(db)).is_ok());
//}
//
//auto open_database() -> calico::Database
//{
//    calico::Options options;
//    options.page_size = 0x2000;
//    options.frame_count = 128;
//    calico::Database db;
//
//    if (const auto s = db.open(PATH, options); !s.is_ok()) {
//        fmt::print("(1/2) cannot open database\n");
//        fmt::print("(2/2) (reason) {}\n", s.what());
//        std::exit(EXIT_FAILURE);
//    }
//    return db;
//}
//
//} // namespace
//
auto main(int, const char *[]) -> int
{
//    namespace cco = calico;
//
//    std::error_code error;
//    std::filesystem::remove_all(PATH, error);
//
//    bytes_objects();
//    auto db = open_database();
//
//    const auto make_record = [](const auto i) {
//        auto x = std::to_string(i);
//        auto k = std::string(16 - x.size(), '0') + x;
//        auto v = k;
//        v.resize(100);
//        return cco::Record {k, v};
//    };
//
//    std::vector<cco::Record> records;
//    for (int i {}; i < 100'000; ++i)
//        records.emplace_back(make_record(i));
//    for (const auto &record: records)
//        assert(db.insert(record).is_ok());
//    for (const auto &[k, v]: records) {
//        const auto c = db.find(k);
//        assert(c.is_valid() and c.value() == v);
//    }
//    assert(db.close().is_ok());
//    exit(0);
//
//    //    reads_and_writes(db);
//    //    updating_a_database(db);
//    //    querying_a_database(db);
//    //    deleting_a_database(std::move(db));
    return 0;
}
