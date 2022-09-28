
#ifndef CALICO_TEST_TOOLS_TOOLS_H
#define CALICO_TEST_TOOLS_TOOLS_H

#include "calico/calico.h"
#include "core/header.h"
#include "page/node.h"
#include "pager/framer.h"
#include "storage/posix_storage.h"
#include "random.h"
#include "utils/types.h"
#include "utils/utils.h"
#include "wal/record.h"
#include <iomanip>
#include <iostream>
#include <vector>
#include <unordered_map>

namespace calico {

inline auto expect_ok(const Status &s) -> void
{
    if (!s.is_ok()) {
        fmt::print(stderr, "unexpected {} status: {}\n", get_status_name(s), s.what());
        CALICO_EXPECT_TRUE(false && "expect_ok() failed");
    }
}

class DataFileInspector {
public:
    DataFileInspector(const std::string &path, Size page_size)
        : m_store {std::make_unique<PosixStorage>()},
          m_page_size {page_size}
    {
        RandomReader *file {};
        auto s = m_store->open_random_reader(path, &file);
        CALICO_EXPECT_TRUE(s.is_ok());
        m_file = std::unique_ptr<RandomReader> {file};

        Size file_size {};
        s = m_store->file_size(path, file_size);
        CALICO_EXPECT_TRUE(s.is_ok());
        CALICO_EXPECT_EQ(file_size % page_size, 0);
        m_data.resize(file_size);

        auto bytes = stob(m_data);
        s = m_file->read(bytes, 0);
        CALICO_EXPECT_EQ(bytes.size(), file_size);
    }

    [[nodiscard]]
    auto page_count() const -> Size
    {
        return m_data.size() / m_page_size;
    }

    [[nodiscard]]
    auto get_state() -> FileHeader
    {
        CALICO_EXPECT_GT(m_data.size(), sizeof(FileHeader));
        auto root = get_page(PageId::root());
        return read_header(root);
    }

    [[nodiscard]]
    auto get_page(PageId id) -> Page
    {
        const auto offset = id.as_index() * m_page_size;
        CALICO_EXPECT_LE(offset + m_page_size, m_data.size());

        return Page {{
            id,
            stob(m_data).range(offset, m_page_size),
            nullptr,
            false,
            false,
        }};
    }

private:
    std::unique_ptr<Storage> m_store;
    std::unique_ptr<RandomReader> m_file;
    std::string m_data;
    Size m_page_size {};
};

class WalRecordGenerator {
public:

    [[nodiscard]]
    auto setup_deltas(Bytes image) -> std::vector<PageDelta>
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
            mem_copy(image.range(offset, size), stob(replacement));
        }
        return deltas;
    }

//    [[nodiscard]]
//    auto setup_single_page_update(Bytes image)
//    {
//
//    }

private:
    Random random {123};
};

class BPlusTree;

namespace tools {

    template<class T>
    auto find_exact(T &t, const std::string &key) -> Cursor
    {
        return t.find_exact(key);
    }

    template<class T>
    auto find(T &t, const std::string &key) -> Cursor
    {
        return t.find(key);
    }

    template<class T>
    auto contains(T &t, const std::string &key) -> bool
    {
        return find_exact(t, key).is_valid();
    }

    template<class T>
    auto contains(T &t, const std::string &key, const std::string &value) -> bool
    {
        if (auto c = find_exact(t, key); c.is_valid())
            return c.value() == value;
        return false;
    }

    template<class T>
    auto expect_contains(T &t, const std::string &key, const std::string &value) -> void
    {
        const auto FMT = fmt::format("expected record ({}, {}): {{}}\n", key, value);
        if (auto c = find_exact(t, key); c.is_valid()) {
            if (c.value() != value) {
                fmt::print(stderr, FMT, "value \"{}\" does not match", c.value());
                CALICO_EXPECT_TRUE(false && "expect_contains() failed to match value");
            }
        } else {
            fmt::print(stderr, FMT, "could not find key");
            CALICO_EXPECT_TRUE(false && "expect_contains() failed to find key");
        }
    }

    template<class T>
    auto insert(T &t, const std::string &key, const std::string &value) -> void
    {
        auto s = t.insert(stob(key), stob(value));
        if (!s.is_ok()) {
            CALICO_EXPECT_TRUE(false && "Error: insert() failed");
        }
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        auto s = t.erase(find_exact(t, key));
        if (!s.is_ok() && !s.is_not_found()) {
            CALICO_EXPECT_TRUE(false && "Error: erase() failed");
        }
        return !s.is_not_found();
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(find_exact(t, key));
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

    inline auto write_file(Storage &store, const std::string &path, BytesView in) -> void
    {
        RandomEditor *file;
        CALICO_EXPECT_TRUE(store.open_random_editor(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in, 0).is_ok());
        delete file;
    }

    inline auto append_file(Storage &store, const std::string &path, BytesView in) -> void
    {
        AppendWriter *file;
        CALICO_EXPECT_TRUE(store.open_append_writer(path, &file).is_ok());
        CALICO_EXPECT_TRUE(file->write(in).is_ok());
        delete file;
    }

    inline auto read_file(Storage &store, const std::string &path) -> std::string
    {
        RandomReader *file;
        std::string out;
        Size size;

        CALICO_EXPECT_TRUE(store.file_size(path, size).is_ok());
        CALICO_EXPECT_TRUE(store.open_random_reader(path, &file).is_ok());
        out.resize(size);

        Bytes temp {out};
        CALICO_EXPECT_TRUE(file->read(temp, 0).is_ok());
        CALICO_EXPECT_EQ(temp.size(), size);
        delete file;
        return out;
    }

} // tools

template<std::size_t Length = 20>
auto make_key(Size key) -> std::string
{
    auto key_string = std::to_string(key);
    return std::string(Length - key_string.size(), '0') + key_string;
}

[[maybe_unused]]
inline auto hexdump(const Byte *data, Size size, Size indent = 0) -> void
{
    constexpr auto chunk_size{0x10UL};
    const auto chunk_count{size / chunk_size};
    const auto rest_size{size % chunk_size};
    const std::string spaces(static_cast<Size>(indent), ' ');

    auto emit_line = [data, spaces](Size i, Size line_size) {
        if (!line_size)
            return;
        auto offset{i * chunk_size};

        std::cout
            << spaces
            << std::hex
            << std::setw(8)
            << std::setfill('0')
            << std::uppercase
            << offset
            << ": ";

        for (Size j{}; j < line_size; ++j) {
            const auto byte{static_cast<uint8_t>(data[offset+j])};
            std::cout
                << std::hex
                << std::setw(2)
                << std::setfill('0')
                << std::uppercase
                << static_cast<uint32_t>(byte)
                << ' ';
        }
        std::cout << '\n';
    };
    Size i{};
    for (; i < chunk_count; ++i)
        emit_line(i, chunk_size);
    // Last line may be partially filled.
    emit_line(i, rest_size);
}

struct Record {
    inline auto operator<(const Record &rhs) const -> bool
    {
        return stob(key) < stob(rhs.key);
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
    };

    RecordGenerator() = default;
    explicit RecordGenerator(Parameters);
    auto generate(Random &, Size) -> std::vector<Record>;

private:
    Parameters m_param;
};

} // namespace calico

namespace fmt {

namespace cco = calico;

template <>
struct formatter<cco::FileHeader> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::FileHeader &header, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(header));
        out += fmt::format("magic_code: {}, ", header.magic_code);
        out += fmt::format("header_crc: {}, ", header.header_crc);
        out += fmt::format("page_count: {}, ", header.page_count);
        out += fmt::format("freelist_head: {}, ", header.freelist_head);
        out += fmt::format("record_count: {}, ", header.record_count);
        out += fmt::format("flushed_lsn: {}, ", header.flushed_lsn);
        out += fmt::format("page_size: {}", header.page_size);
        return format_to(ctx.out(), "FileHeader {}}}", out);
    }
};

template <>
struct formatter<cco::Options> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::Options &options, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(options));
        out += fmt::format("wal_limit: {}", options.wal_limit);
        out += fmt::format("page_size: {}, ", options.page_size);
        out += fmt::format("frame_count: {}, ", options.frame_count);
        out += fmt::format("log_level: {}, ", options.log_level);
        out += fmt::format("store: {}, ", static_cast<void*>(options.store));
        return format_to(ctx.out(), "Options {}}}", out);
    }
};

template <>
struct formatter<cco::PageType> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::PageType &type, FormatContext &ctx) {
        switch (type) {
            case cco::PageType::EXTERNAL_NODE: return format_to(ctx.out(), "EXTERNAL_NODE");
            case cco::PageType::INTERNAL_NODE: return format_to(ctx.out(), "INTERNAL_NODE");
            case cco::PageType::FREELIST_LINK: return format_to(ctx.out(), "FREELIST_LINK");
            case cco::PageType::OVERFLOW_LINK: return format_to(ctx.out(), "OVERFLOW_LINK");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

template <>
struct formatter<cco::WalRecordHeader::Type> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::WalRecordHeader::Type &type, FormatContext &ctx) {
        switch (type) {
            case cco::WalRecordHeader::FULL: return format_to(ctx.out(), "FULL");
            case cco::WalRecordHeader::FIRST: return format_to(ctx.out(), "FIRST");
            case cco::WalRecordHeader::MIDDLE: return format_to(ctx.out(), "MIDDLE");
            case cco::WalRecordHeader::LAST: return format_to(ctx.out(), "LAST");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

template <>
struct formatter<cco::WalRecordHeader> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::WalRecordHeader &header, FormatContext &ctx) {
        auto out = fmt::format("({} B) {{", sizeof(header));
        out += fmt::format("crc: {}, ", header.crc);
        out += fmt::format("size: {}, ", header.size);
        out += fmt::format("type: {}, ", header.type);
        return format_to(ctx.out(), "WalRecordHeader {}}}", out);
    }
};

template <>
struct formatter<cco::XactPayloadType> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::XactPayloadType &type, FormatContext &ctx) {
        switch (type) {
            case cco::XactPayloadType::FULL_IMAGE: return format_to(ctx.out(), "FULL_IMAGE");
            case cco::XactPayloadType::DELTAS: return format_to(ctx.out(), "DELTAS");
            case cco::XactPayloadType::COMMIT: return format_to(ctx.out(), "COMMIT");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

}  // namespace fmt

auto operator<(const calico::Record&, const calico::Record&) -> bool;
auto operator>(const calico::Record&, const calico::Record&) -> bool;
auto operator<=(const calico::Record&, const calico::Record&) -> bool;
auto operator>=(const calico::Record&, const calico::Record&) -> bool;
auto operator==(const calico::Record&, const calico::Record&) -> bool;
auto operator!=(const calico::Record&, const calico::Record&) -> bool;

#endif // CALICO_TEST_TOOLS_TOOLS_H
