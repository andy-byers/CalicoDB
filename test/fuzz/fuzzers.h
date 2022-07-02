#ifndef CALICO_TEST_FUZZ_FUZZERS_H
#define CALICO_TEST_FUZZ_FUZZERS_H

#include <array>
#include <vector>
#include "validators.h"
#include "calico/calico.h"
#include "calico/database.h"
#include "page/cell.h"
#include "page/node.h"
#include "page/page.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "../tools/fakes.h"
#include "../tools/tools.h"

namespace calico::fuzz {

template<class T, class R> class Fuzzer {
public:
    using Transformer = T;
    using Runner = R;

    explicit Fuzzer(Transformer &&transformer)
        : m_transformer{std::forward<Transformer>(transformer)} {}

    virtual ~Fuzzer() = default;

    auto operator()(const uint8_t *data, Size size) -> void
    {
        const BytesView bytes {reinterpret_cast<const Byte*>(data), size};
        m_runner(m_transformer.decode(bytes));
    }

private:
    Transformer m_transformer;
    Runner m_runner;
};

enum class Operation: unsigned {
    WRITE,
    ERASE,
};

static constexpr Size VALUE_MULTIPLIER {3};

struct OperationInput {
    std::string key;
    Size value_size {};
    Operation operation {};
};

template<Size Ratio = 80> struct OperationTransformer {
    using Decoded = std::vector<OperationInput>;
    using Encoded = std::string;

    [[nodiscard]] auto decode(BytesView in) const -> Decoded
    {
        CALICO_EXPECT_STATIC(50 <= Ratio && Ratio <= 100, "Ratio must be in [50, 100]");

        static constexpr Size MIN_INFO_SIZE {2};
        static constexpr Size MAX_INFO_SIZE {3};
        Decoded decoded;

        while (in.size() >= MAX_INFO_SIZE) {
            const uint8_t code = uint8_t(in[0]) % 100;
            const auto operation = code < Ratio ? Operation::WRITE : Operation::ERASE;
            const auto has_value = operation == Operation::WRITE;
            const auto key_size = uint8_t(in[1]);
            const Size value_size = has_value ? uint8_t(in[2]) * VALUE_MULTIPLIER : 0;

            in.advance(MIN_INFO_SIZE + has_value);
            if (in.size() < key_size)
                break;

            const auto key = btos(in.range(0, key_size));
            decoded.emplace_back(OperationInput {key, value_size, operation});
            in.advance(key_size);
        }
        return decoded;
    }

    [[nodiscard]] auto encode(const Decoded &decoded) const -> Encoded
    {
        CALICO_EXPECT_STATIC(50 <= Ratio && Ratio <= 100, "Ratio must be in [50, 100]");

        std::string encoded;
        Random random;

        for (const auto &[key, value_size, operation]: decoded) {
            CALICO_EXPECT_LT(value_size, 0x100);
            Byte info_byte, size_byte {};
            if (operation == Operation::WRITE) {
                info_byte = static_cast<Byte>(random.next_int(Ratio - 1));
                size_byte = static_cast<Byte>(value_size / VALUE_MULTIPLIER);
            } else {
                info_byte = static_cast<Byte>(random.next_int(Ratio, Size {99}));
            }
            encoded += info_byte;
            encoded += static_cast<Byte>(key.size());
            encoded += size_byte;
            encoded += key;
        }
        return encoded;
    }

};

template<Size PageSize, bool IsInMemory> struct DatabaseProvider {
    static constexpr auto PATH = "/tmp/calico_fuzz_database";

    auto operator()() const -> Database
    {
        Options options;
        options.page_size = PageSize;
        options.block_size = PageSize;
        options.use_transactions = true;

        if constexpr (IsInMemory) {
            return Database::temp(options);
        } else {
            std::filesystem::remove(PATH);
            std::filesystem::remove(get_wal_path(PATH));
            return Database::open(PATH, options);
        }
    }
};

template<class Provider> class OperationRunner {
public:
    using Input = OperationTransformer<>::Decoded;

    OperationRunner()
        : m_db {Provider()()} {}

    auto operator()(Input&& input) -> void
    {
        for (const auto &[key, value_size, operation]: input) {
            if (operation == Operation::WRITE) {
                std::string value(value_size, '*');
                m_db.insert({key, value});
            } else if (auto c = m_db.find(stob(key), true); c.is_valid()) {
                m_db.erase(c);
            }
        }
        m_db.commit();
        fuzz::validate_ordering(m_db);
    }

    auto database() -> Database&
    {
        return m_db;
    }

private:
    Database m_db;
};

using InMemoryOpsFuzzer = Fuzzer<
    OperationTransformer<80>,
    OperationRunner<DatabaseProvider<0x200, true>>
>;

using OpsFuzzer = Fuzzer<
    OperationTransformer<80>,
    OperationRunner<DatabaseProvider<0x200, false>>
>;

class PassThroughTransformer {
public:
    using Decoded = BytesView;
    using Encoded = std::string;
    
    [[nodiscard]] auto decode(BytesView in) const -> Decoded
    {
        return in;
    }
    
    [[nodiscard]] auto encode(Decoded in) const -> Encoded
    {
        return btos(in);
    }
};

template<Size BlockSize> class WALReaderRunner {
public:
    using Input = PassThroughTransformer::Decoded;

    auto operator()(Input &&input) -> void
    {
        auto file = std::make_unique<ReadOnlyMemory>();
        auto backing = file->memory();
        backing.memory() = btos(input);

        WALReader reader {{"", std::move(file), logging::create_sink("", 0), BlockSize}};
        while (reader.increment()) {}
        while (reader.decrement()) {}
    }

    auto wal_reader() -> WALReader&
    {
        return *m_wal_reader;
    }

private:
    std::unique_ptr<WALReader> m_wal_reader;
};

using WALReaderFuzzer = Fuzzer<
    PassThroughTransformer,
    WALReaderRunner<0x100>
>;

struct FuzzerNode {
    std::string backing;
    Node node;
};

template<Index PageID> struct NodeProvider {

    auto operator()(std::string &backing) const -> Node
    {
        Page page {{
            PID {PageID},
            stob(backing),
            nullptr,
            true,
            false,
        }};
        page.set_type(PageType::EXTERNAL_NODE);
        return Node {std::move(page), true};
    }
};

template<class Provider> class NodeOperationRunner {
public:
    static constexpr Size PAGE_SIZE {0x200};
    using Input = OperationTransformer<>::Decoded;

    NodeOperationRunner()
        : m_backing(PAGE_SIZE, '\x00')
        , m_node {Provider {}(m_backing)} {}

    auto operator()(Input&& input) -> void
    {
        for (const auto &[key, value_size, operation]: input) {
            // Nodes use assertions for invalid keys. We don't want to trip them. TODO: Only generate valid keys?
            if (!is_key_valid(key))
                continue;
            const auto [index, found_eq] = m_node.find_ge(stob(key));

            if (found_eq)
                m_node.remove(stob(key));

            if (operation == Operation::WRITE) {
                std::string value(value_size, '*');
                m_node.insert(make_external_cell(stob(key), stob(value), PAGE_SIZE));
                if (m_node.is_overflowing())
                    (void)m_node.take_overflow_cell();
            } else if (!found_eq && index < m_node.cell_count()) {
                m_node.remove(stob(key));
            }
        }
    }

    auto node() -> Node&
    {
        return m_node;
    }

private:
    auto is_key_valid(const std::string &key)
    {
        return !key.empty() && key.size() <= get_max_local(PAGE_SIZE);
    }

    std::string m_backing;
    Node m_node;
};

using NodeOpsFuzzer = Fuzzer<
    OperationTransformer<80>,
    NodeOperationRunner<NodeProvider<2>>
>;

} // calico::fuzz

#endif // CALICO_TEST_FUZZ_FUZZERS_H
