
#ifndef CALICO_TEST_TOOLS_H
#define CALICO_TEST_TOOLS_H

#include "calico/calico.h"
#include "calico/cursor.h"
#include "pager/framer.h"
#include "random.h"
#include "storage/posix_storage.h"
#include "tree/node.h"
#include "tree/header.h"
#include "utils/types.h"
#include "utils/utils.h"
#include "wal/record.h"
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <vector>

namespace Calico {

inline auto expect_ok(const Status &s) -> void
{
    if (!s.is_ok()) {
        fmt::print(stderr, "unexpected {} status: {}\n", get_status_name(s), s.what().data());
        std::abort();
    }
}

class WalRecordGenerator {
public:

    [[nodiscard]]
    auto setup_deltas(Span image) -> std::vector<PageDelta>
    {
        static constexpr Size MAX_WIDTH {30};
        static constexpr Size MAX_SPREAD {20};
        std::vector<PageDelta> deltas;

        for (Size offset {random.get(image.size() / 10)}; offset < image.size(); ) {
            const auto rest = image.size() - offset;
            const auto size = random.get(1, std::min(rest, MAX_WIDTH));
            deltas.emplace_back(PageDelta {offset, size});
            offset += size + random.get(1, MAX_SPREAD);
        }
        for (const auto &[offset, size]: deltas) {
            const auto replacement = random.get<std::string>('a', 'z', size);
            mem_copy(image.range(offset, size), Slice {replacement});
        }
        return deltas;
    }

private:
    Random random {123};
};

namespace tools {

    template<class T>
    auto get(T &t, const std::string &key, std::string &value) -> Status
    {
        return t.get(key, value);
    }

    template<class T>
    auto find(T &t, const std::string &key) -> Cursor
    {
        auto c = t.cursor();
        c.seek(key);
        return c;
    }

    template<class T>
    auto contains(T &t, const std::string &key) -> bool
    {
        std::string value;
        return get(t, key, value).is_ok();
    }

    template<class T>
    auto contains(T &t, const std::string &key, const std::string &value) -> bool
    {
        std::string val;
        if (auto s = get(t, key, val); s.is_ok()) {
            return val == value;
        }
        return false;
    }

    template<class T>
    auto expect_contains(T &t, const std::string &key, const std::string &value) -> void
    {
        std::string val;
        const auto MSG = fmt::format("expected record ({}, {})\n", key, value);
        if (auto s = get(t, key, val); s.is_ok()) {
            if (val != value) {
                fmt::print(stderr, "{}: value \"{}\" does not match\n", MSG, val);
                std::exit(EXIT_FAILURE);
            }
        } else {
            fmt::print(stderr, "{}: {}\n", MSG, "could not find key");
            std::exit(EXIT_FAILURE);
        }
    }

    template<class T>
    auto insert(T &t, const std::string &key, const std::string &value) -> void
    {
        auto s = t.put(key, value);
        if (!s.is_ok()) {
            fmt::print(stderr, "error: {}\n", s.what().data());
            CALICO_EXPECT_TRUE(false && "Error: insert() failed");
        }
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        auto s = t.erase(get(t, key));
        if (!s.is_ok() && !s.is_not_found()) {
            fmt::print(stderr, "error: {}\n", s.what().data());
            CALICO_EXPECT_TRUE(false && "Error: erase() failed");
        }
        return !s.is_not_found();
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(get(t, key));
        CALICO_EXPECT_TRUE(was_erased.has_value());
        if (was_erased.value())
            return true;
        auto cursor = t.first();
        CALICO_EXPECT_EQ(cursor.error(), std::nullopt);
        if (!cursor.is_valid())
            return false;
        was_erased = t.erase(cursor);
        CALICO_EXPECT_TRUE(was_erased.value());
        return true;
    }

    inline auto write_file(Storage &storage, const std::string &path, Slice in) -> void
    {
        RandomEditor *file;
        CALICO_EXPECT_TRUE(storage.open_random_editor(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in, 0).is_ok());
        delete file;
    }

    inline auto append_file(Storage &storage, const std::string &path, Slice in) -> void
    {
        AppendWriter *file;
        CALICO_EXPECT_TRUE(storage.open_append_writer(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in).is_ok());
        delete file;
    }

    inline auto read_file(Storage &storage, const std::string &path) -> std::string
    {
        RandomReader *file;
        std::string out;
        Size size;

        CALICO_EXPECT_TRUE(storage.file_size(path, size).is_ok());
        CALICO_EXPECT_TRUE(storage.open_random_reader(path, &file).is_ok());
        out.resize(size);

        Span temp {out};
        auto read_size = temp.size();
        CALICO_EXPECT_TRUE(file->read(temp.data(), read_size, 0).is_ok());
        CALICO_EXPECT_EQ(read_size, size);
        delete file;
        return out;
    }

    [[nodiscard]]
    inline auto snapshot(Storage &storage, Size page_size) -> std::string
    {
        static constexpr Size CODE {0x1234567887654321};

        Size file_size;
        CALICO_EXPECT_TRUE(storage.file_size("test/data", file_size).is_ok());

        std::unique_ptr<RandomReader> reader;
        {
            RandomReader *temp;
            expect_ok(storage.open_random_reader("test/data", &temp));
            reader.reset(temp);
        }

        std::string buffer(file_size, '\x00');
        auto read_size = file_size;
        expect_ok(reader->read(buffer.data(), read_size, 0));
        CALICO_EXPECT_EQ(read_size, file_size);

        CALICO_EXPECT_EQ(file_size % page_size, 0);

        auto offset = FileHeader::SIZE;
        for (Size i {}; i < file_size / page_size; ++i) {
            put_u64(buffer.data() + i*page_size + offset, CODE);
            offset = 0;
        }

        // Clear header fields that might be inconsistent, despite identical database contents.
        Page root {Id::root(),{buffer.data(), page_size}, true};
        FileHeader header {root};
        header.header_crc = 0;
        header.recovery_lsn.value = CODE;
        header.write(root);

        return buffer;
    }


} // tools

template<std::size_t Length = 20>
auto make_key(Size key) -> std::string
{
    auto key_string = std::to_string(key);
    if (key_string.size() == Length) {
        return key_string;
    } else if (key_string.size() > Length) {
        return key_string.substr(0, Length);
    } else {
        return std::string(Length - key_string.size(), '0') + key_string;
    }
}

[[maybe_unused]]
inline auto hexdump(std::ostream &os, const Byte *data, Size size, Size indent = 0) -> void
{
    constexpr auto chunk_size{0x10UL};
    const auto chunk_count{size / chunk_size};
    const auto rest_size{size % chunk_size};
    const std::string spaces(static_cast<Size>(indent), ' ');

    const auto emit_line = [&os, data, spaces](Size i, Size line_size) {
        if (line_size) {
            const auto offset = i * chunk_size;

            os
                << spaces
                << std::hex
                << std::setw(8)
                << std::setfill('0')
                << std::uppercase
                << offset
                << ": ";

            for (Size j {}; j < line_size; ++j) {
                os
                    << std::hex
                    << std::setw(2)
                    << std::setfill('0')
                    << std::uppercase
                    << static_cast<uint32_t>(data[offset + j])
                    << ' ';
            }
            os << '\n';
        }
    };
    Size i {};
    for (; i < chunk_count; ++i) {
        emit_line(i, chunk_size);
    }
    // Last line may be partially filled.
    emit_line(i, rest_size);
}

struct Record {
    inline auto operator<(const Record &rhs) const -> bool
    {
        return Slice {key} < Slice {rhs.key};
    }

    std::string key;
    std::string value;
};

class RecordGenerator {
public:
    static unsigned default_seed;

    struct Parameters {
        Size mean_key_size {12};
        Size mean_value_size {18};
        Size spread {4};
        bool is_sequential {};
        bool is_unique {};
        bool is_readable {true};
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(Random &, Size) -> std::vector<Record>;

private:
    Parameters m_param;
};

} // namespace Calico

namespace fmt {

namespace Cco = Calico;

template <>
struct formatter<Cco::FileHeader> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::FileHeader &header, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(header));
        out += fmt::format("magic_code: {}, ", header.magic_code);
        out += fmt::format("header_crc: {}, ", header.header_crc);
        out += fmt::format("page_count: {}, ", header.page_count);
        out += fmt::format("free_list_id: {}, ", header.free_list_id);
        out += fmt::format("record_count: {}, ", header.record_count);
        out += fmt::format("flushed_lsn: {}, ", header.recovery_lsn);
        out += fmt::format("page_size: {}", header.page_size);
        return format_to(ctx.out(), "FileHeader {}}}", out);
    }
};

template <>
struct formatter<Cco::Options> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::Options &options, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(options));
        out += fmt::format("wal_buffer_size: {}", options.wal_buffer_size);
        out += fmt::format("page_size: {}, ", options.page_size);
        out += fmt::format("page_cache_size: {}, ", options.page_cache_size);
        out += fmt::format("log_level: {}, ", static_cast<int>(options.log_level));
        out += fmt::format("storage: {}, ", static_cast<void*>(options.storage));
        return format_to(ctx.out(), "Options {}}}", out);
    }
};

template <>
struct formatter<Cco::PageType> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::PageType &type, FormatContext &ctx) {
        switch (type) {
            case Cco::PageType::EXTERNAL_NODE: return format_to(ctx.out(), "EXTERNAL_NODE");
            case Cco::PageType::INTERNAL_NODE: return format_to(ctx.out(), "INTERNAL_NODE");
            case Cco::PageType::FREELIST_LINK: return format_to(ctx.out(), "FREELIST_LINK");
            case Cco::PageType::OVERFLOW_LINK: return format_to(ctx.out(), "OVERFLOW_LINK");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

template <>
struct formatter<Cco::WalRecordHeader::Type> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::WalRecordHeader::Type &type, FormatContext &ctx) {
        switch (type) {
            case Cco::WalRecordHeader::FULL: return format_to(ctx.out(), "FULL");
            case Cco::WalRecordHeader::FIRST: return format_to(ctx.out(), "FIRST");
            case Cco::WalRecordHeader::MIDDLE: return format_to(ctx.out(), "MIDDLE");
            case Cco::WalRecordHeader::LAST: return format_to(ctx.out(), "LAST");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

template <>
struct formatter<Cco::WalRecordHeader> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::WalRecordHeader &header, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(header));
        out += fmt::format("crc: {}, ", header.crc);
        out += fmt::format("size: {}, ", header.size);
        out += fmt::format("type: {}, ", header.type);
        return format_to(ctx.out(), "WalRecordHeader {}}}", out);
    }
};

template <>
struct formatter<Cco::XactPayloadType> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const Cco::XactPayloadType &type, FormatContext &ctx) {
        switch (type) {
            case Cco::XactPayloadType::FULL_IMAGE: return format_to(ctx.out(), "FULL_IMAGE");
            case Cco::XactPayloadType::DELTA: return format_to(ctx.out(), "DELTA");
            case Cco::XactPayloadType::COMMIT: return format_to(ctx.out(), "COMMIT");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

}  // namespace fmt

auto operator>(const Calico::Record&, const Calico::Record&) -> bool;
auto operator<=(const Calico::Record&, const Calico::Record&) -> bool;
auto operator>=(const Calico::Record&, const Calico::Record&) -> bool;
auto operator==(const Calico::Record&, const Calico::Record&) -> bool;
auto operator!=(const Calico::Record&, const Calico::Record&) -> bool;

#endif // CALICO_TEST_TOOLS_H