
#include "calico/calico.h"
#include "calico/transaction.h"
#include <chrono>
#include <filesystem>
#include <spdlog/fmt/fmt.h>
#include <vector>

namespace {

constexpr auto PATH = "/tmp/__calico_usage";
namespace cco = calico;

class Cat final {
public:
    Cat(const std::string &name, cco::Database &db)
        : m_buffer {name},
          m_db {&db},
          m_name_width {name.size()}
    {
        m_buffer.resize(m_db->info().maximum_key_size());
        m_buffer[m_name_width] = '\xFF';
    }

    [[nodiscard]]
    auto insert(const std::string &key, const std::string &value) -> cco::Status
    {
        return m_db->insert(tagged_key(key), cco::stob(value));
    }

    [[nodiscard]]
    auto erase(const std::string &key) -> cco::Status
    {
        return m_db->erase(tagged_key(key));
    }

private:
    [[nodiscard]]
    auto tagged_key(const std::string &key) -> cco::BytesView
    {
        std::copy(cbegin(key), cend(key), begin(m_buffer) + static_cast<long>(m_name_width) + 1);
        return cco::stob(m_buffer).truncate(m_name_width + key.size() + 1);
    }

    std::string m_buffer;
    cco::Database *m_db {};
    cco::Size m_name_width {};
};

/**
 * A database for storing information about specific cats!
 */
class CatDB final {
public:
    static constexpr auto HOME = "/tmp/__cat_db";

    CatDB()
    {
        calico::Options options;
        options.page_size = 0x1000;
        options.frame_count = 64;
        options.log_level = spdlog::level::trace;

        if (auto s = m_db.open(HOME, options); !s.is_ok())
            throw std::runtime_error {s.what()};

        assert(m_db.is_open());
    }

    ~CatDB()
    {
        (void)m_db.close();
    }

    [[nodiscard]]
    auto fetch(const std::string &name) -> Cat
    {

    }

private:
    cco::Database m_db;
};

} // namespace

auto main(int, const char *[]) -> int
{
    calico::Database db;
    calico::Options options;
    options.wal_path = "/tmp/cats_wal";
    options.page_size = 0x2000;
    options.frame_count = 128;
    options.log_level = spdlog::level::info;

    // Open the database connection.
    if (auto s = db.open("/tmp/cats", options); !s.is_ok()) {
        fmt::print(stderr, "{}\n", s.what());
        std::exit(EXIT_FAILURE);
    }
    // This will be true until db.close() is called.
    assert(db.is_open());
    return 0;
}
