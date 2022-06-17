#ifndef CUB_TEST_FUZZ_FUZZERS_H
#define CUB_TEST_FUZZ_FUZZERS_H

#include <array>
#include <vector>
#include "validators.h"
#include "cub/cub.h"
#include "cub/database.h"
#include "wal/wal_reader.h"
#include "wal/wal_writer.h"
#include "../tools/fakes.h"
#include "../tools/tools.h"

namespace cub::fuzz {

template<class D, class R> class Fuzzer {
public:
    using Decoder = D;
    using Runner = R;

    explicit Fuzzer(Decoder &&decoder)
        : m_decoder {std::forward<Decoder>(decoder)} {}

    virtual ~Fuzzer() = default;

    auto operator()(const uint8_t *data, Size size) -> void
    {
        const BytesView bytes {reinterpret_cast<const Byte*>(data), size};
        m_runner(m_decoder(bytes));
    }

private:
    Decoder m_decoder;
    Runner m_runner;
};

enum class Operation: unsigned {
    WRITE,
    ERASE,
};

static constexpr Size OPERATION_CHANCES[4] {80, 10, 5, 5};
static constexpr Size VALUE_MULTIPLIER {3};

struct OperationInput {
    std::string key;
    Size value_size {};
    Operation operation {};
};

template<uint8_t Ratio> class OperationDecoder {
public:
    using Decoded = std::vector<OperationInput>;

    auto operator()(BytesView in) -> Decoded
    {
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

            const auto key = _s(in.range(0, key_size));
            decoded.emplace_back(OperationInput {key, value_size, operation});
            in.advance(key_size);
        }
        return decoded;
    }
};

template<unsigned Ratio> class OperationEncoder {
public:
    using Decoded = std::vector<OperationInput>;

    auto operator()(const Decoded &decoded) -> std::string
    {
        CUB_EXPECT_STATIC(50 <= Ratio && Ratio <= 100, "Ratio must be in [50, 100]");

        Random random {0};
        std::string encoded;

        for (const auto &[key, value_size, operation]: decoded) {
            CUB_EXPECT_LT(value_size, 0x100);
            Byte info_byte, size_byte {};
            if (operation == Operation::WRITE) {
                info_byte = static_cast<Byte>(random.next_int(Ratio - 1));
                size_byte = static_cast<Byte>(value_size / VALUE_MULTIPLIER);
            } else {
                info_byte = static_cast<Byte>(random.next_int(Ratio, 99U));
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
    static constexpr auto PATH = "/tmp/cub_fuzz_database";

    auto operator()() -> Database
    {
        if constexpr (IsInMemory) {
            return Database::temp(PageSize, true);
        } else {
            std::filesystem::remove(PATH);
            std::filesystem::remove(get_wal_path(PATH));
            Options options;
            options.page_size = PageSize;
            options.block_size = PageSize;
            options.use_transactions = true;
            return Database::open(PATH, options);
        }
    }
};

template<class Provider> class OperationRunner {
public:
    using Input = std::vector<OperationInput>;

    OperationRunner()
        : m_db {Provider()()} {}

    auto operator()(Input&& input) -> void
    {
        for (const auto &[key, value_size, operation]: input) {
            if (operation == Operation::WRITE) {
                const auto value = std::string(value_size, '*');
                m_db.write(_b(key), _b(value));
            } else if (const auto record = m_db.read(_b(key), Ordering::GE)) {
                m_db.erase(_b(record->key));
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
    OperationDecoder<80>,
    OperationRunner<DatabaseProvider<0x200, true>>
>;

using OpsFuzzer = Fuzzer<
    OperationDecoder<80>,
    OperationRunner<DatabaseProvider<0x200, false>>
>;

class PassThroughDecoder {
public:
    auto operator()(BytesView in) -> BytesView
    {
        return in;
    }
};

class PassThroughEncoder {
public:
    auto operator()(BytesView in) -> std::string
    {
        return _s(in);
    }
};

template<Size BlockSize> class WALReaderRunner {
public:
    using Input = BytesView;

    auto operator()(Input &&input) -> void
    {
        auto file = std::make_unique<ReadOnlyMemory>();
        auto backing = file->memory();
        backing.memory() = _s(input);

        WALReader reader {std::move(file), BlockSize};
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
    PassThroughDecoder,
    WALReaderRunner<0x100>
>;

} // cub::fuzz

#endif // CUB_TEST_FUZZ_FUZZERS_H
