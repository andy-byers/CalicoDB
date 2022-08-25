
#ifndef CALICO_TEST_TOOLS_TOOLS_H
#define CALICO_TEST_TOOLS_TOOLS_H

#include "calico/calico.h"
#include "core/header.h"
#include "page/node.h"
#include "pager/framer.h"
#include "store/disk.h"
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
        : m_store {std::make_unique<DiskStorage>()},
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

        for (Size offset {random.next_int(image.size() / 10)}; offset < image.size(); ) {
            const auto rest = image.size() - offset;
            const auto size = random.next_int(1UL, std::min(rest, MAX_WIDTH));
            deltas.emplace_back(PageDelta {offset, size});
            offset += size + random.next_int(1ULL, MAX_SPREAD);
        }
        for (const auto &[offset, size]: deltas) {
            const auto replacement = random.next_string(size);
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
//
//struct WalPayloadWrapper {
//    WalPayloadType type {};
//    std::vector<PageDelta> deltas;
//    BytesView image;
//};
//
//struct WalScenario {
//    std::vector<std::string> before_images;
//    std::vector<std::string> after_images;
//    std::vector<WalPayloadWrapper> payloads;
//};
//
//class WalScenarioGenerator {
//public:
//    explicit WalScenarioGenerator(Size page_size)
//        : m_page_size {page_size}
//    {}
//
//    [[nodiscard]]
//    auto generate_single_page(Size max_rounds) -> WalScenario
//    {
//        CALICO_EXPECT_GT(max_rounds, 0);
//        WalScenario scenario;
//        auto &[before_images, after_images, payloads] = scenario;
//
//        before_images.emplace_back(m_random.next_string(m_page_size));
//        after_images.emplace_back(before_images.back());
//        payloads.emplace_back();
//        payloads.back().type = WalPayloadType::FULL_IMAGE;
//        payloads.back().image = stob(pages.back());
//
//        const auto num_rounds = m_random.next_int(1UL, max_rounds);
//        while (payloads.size() - 1 < num_rounds) {
//            WalPayloadWrapper payload;
//            payload.image = stob(after_images.back());
//            payload.type = WalPayloadType::DELTAS;
//            payload.deltas = generate_deltas(payload.image);
//        }
//    }
//
//    [[nodiscard]]
//    auto generate_multiple_pages(Size num_pages, Size max_rounds_per_page) -> WalScenario
//    {
//        std::vector<WalScenario> scenarios(num_pages);
//        std::generate(begin(scenarios), end(scenarios), [this, max_rounds_per_page] {
//            return generate_single_page(max_rounds_per_page);
//        });
//
//    }
//
//private:
//    [[nodiscard]]
//    auto generate_deltas(Bytes image) -> std::vector<PageDelta>
//    {
//        static constexpr Size MAX_WIDTH {30};
//        static constexpr Size MAX_SPREAD {20};
//        std::vector<PageDelta> deltas;
//
//        for (Size offset {m_random.next_int(image.size() / 10)}; offset < image.size(); ) {
//            const auto rest = image.size() - offset;
//            const auto size = m_random.next_int(1UL, std::min(rest, MAX_WIDTH));
//            deltas.emplace_back(PageDelta {offset, size});
//            offset += size + m_random.next_int(1UL, MAX_SPREAD);
//        }
//        for (const auto &[offset, size]: deltas) {
//            const auto replacement = m_random.next_string(size);
//            mem_copy(image.range(offset, size), stob(replacement));
//        }
//        return deltas;
//    }
//
//    Random m_random {123};
//    Size m_page_size {};
//};

class BPlusTree;

namespace tools {

    template<class T>
    auto find_exact(T &t, const std::string &key) -> Cursor
    {
        return t.find_exact(stob(key));
    }

    template<class T>
    auto find(T &t, const std::string &key) -> Cursor
    {
        return t.find(stob(key));
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
        if (!s.is_ok())
            CALICO_EXPECT_TRUE(false && "Error: insert() failed");
    }

    template<class T>
    auto erase(T &t, const std::string &key) -> bool
    {
        auto s = t.erase(find_exact(t, key));
        if (!s.is_ok() && !s.is_not_found())
            CALICO_EXPECT_TRUE(false && "Error: erase() failed");
        return !s.is_not_found();
    }

    template<class T>
    auto erase_one(T &t, const std::string &key) -> bool
    {
        auto was_erased = t.erase(find_exact(t, key));
        CALICO_EXPECT_TRUE(was_erased.has_value());
        if (was_erased.value())
            return true;
        auto cursor = t.find_minimum();
        CALICO_EXPECT_EQ(cursor.error(), std::nullopt);
        if (!cursor.is_valid())
            return false;
        was_erased = t.erase(cursor);
        CALICO_EXPECT_TRUE(was_erased.value());
        return true;
    }

} // tools

template<std::size_t Length = 20> auto make_key(Size key) -> std::string
{
    auto key_string = std::to_string(key);
    return std::string(Length - key_string.size(), '0') + key_string;
}

class TreeValidator {
public:
    explicit TreeValidator(BPlusTree &);
    auto validate() -> void;

private:
    auto validate_sibling_connections() -> void;
    auto validate_parent_child_connections() -> void;
    auto validate_ordering() -> void;
    auto collect_keys() -> std::vector<std::string>;
    auto traverse_inorder(const std::function<void(Node&, Size)>&) -> void;
    auto traverse_inorder_helper(Node, const std::function<void(Node&, Size)>&) -> void;
    auto is_reachable(std::string) -> bool;

    BPlusTree &m_tree;
};

class TreePrinter {
public:
    explicit TreePrinter(BPlusTree &, bool = true);
    auto print(Size = 0) -> void;

private:
    auto add_spaces_to_level(Size, Size) -> void;
    auto add_spaces_to_other_levels(Size, Size) -> void;
    auto print_aux(Node, Size) -> void;
    auto add_key_to_level(BytesView, Size, bool) -> void;
    auto add_key_separator_to_level(Size) -> void;
    auto add_node_start_to_level(Size, Size) -> void;
    auto add_node_end_to_level(Size) -> void;
    auto make_key_token(BytesView) -> std::string;
    static auto make_key_separator_token() -> std::string;
    static auto make_node_start_token(Size) -> std::string;
    static auto make_node_end_token() -> std::string;
    auto ensure_level_exists(Size) -> void;

    std::vector<std::string> m_levels;
    BPlusTree &m_tree;
    bool m_has_integer_keys {};
};

[[maybe_unused]] inline auto hexdump(const Byte *data, Size size, Size indent = 0) -> void
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
    auto generate(Random&, Size) -> std::vector<Record>;

private:
    Parameters m_param;
};

//
//class WALRecordGenerator {
//public:
//    explicit WALRecordGenerator(Size page_size):
//          m_page_size {page_size}
//    {
//        CALICO_EXPECT_TRUE(is_power_of_two(page_size));
//    }
//
//    auto generate_small() -> WALRecord
//    {
//        const auto small_size = m_page_size / 0x10;
//        const auto total_update_size = random.next_int(small_size, small_size * 2);
//        const auto update_count = random.next_int(1UL, 5UL);
//        const auto mean_update_size = total_update_size / update_count;
//        return generate(mean_update_size, update_count);
//    }
//
//    auto generate_large() -> WALRecord
//    {
//        const auto large_size = m_page_size / 3 * 2;
//        const auto total_update_size = random.next_int(large_size, large_size * 2);
//        const auto update_count = random.next_int(1UL, 5UL);
//        const auto mean_update_size = total_update_size / update_count;
//        return generate(mean_update_size, update_count);
//    }
//
//    auto generate(Size mean_update_size, Size update_count) -> WALRecord
//    {
//        CALICO_EXPECT_GT(mean_update_size, 0);
//        constexpr Size page_count = 0x1000;
//        const auto lower_bound = mean_update_size - mean_update_size/3;
//        const auto upper_bound = mean_update_size + mean_update_size/3;
//        const auto page_size = upper_bound;
//        CALICO_EXPECT_LE(page_size, std::numeric_limits<uint16_t>::max());
//
//        m_snapshots_before.emplace_back(random.next_string(page_size));
//        m_snapshots_after.emplace_back(random.next_string(page_size));
//        std::vector<ChangedRegion> update {};
//        auto payload_size = WALPayload::HEADER_SIZE;
//
//        for (Size i {}; i < update_count; ++i) {
//            const auto size = random.next_int(lower_bound, upper_bound);
//            const auto offset = random.next_int(page_size - size);
//
//            update.emplace_back();
//            update.back().offset = offset;
//            update.back().before = stob(m_snapshots_before.back()).range(offset, size);
//            update.back().after = stob(m_snapshots_after.back()).range(offset, size);
//            payload_size += WALPayload::UPDATE_HEADER_SIZE + 2 * size;
//            CALICO_EXPECT_LT(payload_size, 4 * m_page_size);
//        }
//        m_scratches.emplace(m_last_lsn, std::string(4 * m_page_size, '\x00'));
//        WALRecord record {{
//            std::move(update),
//            PageId {static_cast<uint32_t>(random.next_int(page_count))},
//            SeqNum::null(),
//            SeqNum {static_cast<uint32_t>(m_payloads.size() + ROOT_ID_VALUE)},
//        }, stob(m_scratches[m_last_lsn])};
//        m_last_lsn++;
//        m_payloads.push_back(btos(record.payload().data()));
//        return record;
//    }
//
//    auto validate_record(const WALRecord &record, SeqNum target_lsn) const -> void
//    {
//        (void)record;
//        CALICO_EXPECT_EQ(record.lsn(), target_lsn);
//        const auto payload = retrieve_payload(target_lsn);
//        CALICO_EXPECT_EQ(record.type(), WALRecord::Type::FULL);
//        CALICO_EXPECT_TRUE(record.payload().data() == stob(payload));
//        CALICO_EXPECT_TRUE(record.is_consistent());
//    }
//
//    [[nodiscard]] auto retrieve_payload(SeqNum lsn) const -> const std::string&
//    {
//        return m_payloads.at(lsn.as_index());
//    }
//
//    Random random {0};
//
//private:
//    std::vector<std::string> m_payloads;
//    std::vector<std::string> m_snapshots_before;
//    std::vector<std::string> m_snapshots_after;
//    std::unordered_map<SeqNum, std::string, SeqNum::Hash> m_scratches;
//    SeqNum m_last_lsn;
//    Size m_page_size;
//};

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
        out += fmt::format("lsn: {}, ", header.lsn);
        out += fmt::format("crc: {}, ", header.crc);
        out += fmt::format("size: {}, ", header.size);
        out += fmt::format("type: {}, ", header.type);
        out += fmt::format("pad: {}", header.pad);
        return format_to(ctx.out(), "WalRecordHeader {}}}", out);
    }
};

template <>
struct formatter<cco::WalPayloadType> {

    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const cco::WalPayloadType &type, FormatContext &ctx) {
        switch (type) {
            case cco::WalPayloadType::FULL_IMAGE: return format_to(ctx.out(), "FULL_IMAGE");
            case cco::WalPayloadType::DELTAS: return format_to(ctx.out(), "DELTAS");
            case cco::WalPayloadType::COMMIT: return format_to(ctx.out(), "COMMIT");
            default: return format_to(ctx.out(), "<unrecognized>");
        }
    }
};

}  // namespace fmt

#endif // CALICO_TEST_TOOLS_TOOLS_H
