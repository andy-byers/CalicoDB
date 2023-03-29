// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal.h"
#include "encoding.h"
#include "env_posix.h"
#include "logging.h"

namespace calicodb
{

using HashSlot = std::uint16_t;
using PageSlot = std::uint32_t;

struct WalHeader {
    static constexpr std::size_t kSize = 32;
    static constexpr std::uint32_t kMagic = 0x87654321; // TODO: more-or-less arbitrary, but could make it spell something cool?
    static constexpr std::uint32_t kVersion = 0x01'000'000;

    std::uint32_t magic_code = kMagic;
    std::uint32_t version = kVersion;
    std::uint32_t page_size = 0;
    std::uint32_t commit = 0;
    std::uint32_t salt[2] = {};
    std::uint32_t cksm[2] = {};
};

struct CheckpointInfo {
    std::size_t backfill;
    std::size_t backfill_attempts;
};

struct WalFrameHeader {
    static constexpr std::size_t kSize = 16;

    auto read(const char *data) -> void
    {
        page_id.value = get_u32(data);
        data += sizeof(std::uint32_t);

        db_size = get_u32(data);
        data += sizeof(std::uint32_t);

        for (auto *arr : {salt, cksm}) {
            arr[0] = get_u32(data);
            data += sizeof(std::uint32_t);

            arr[1] = get_u32(data);
            data += sizeof(std::uint32_t);
        }
    }

    auto write(char *data) -> void
    {
        put_u32(data, page_id.value);
        data += sizeof(std::uint32_t);

        put_u32(data, db_size);
        data += sizeof(std::uint32_t);

        for (const auto *arr : {salt, cksm}) {
            put_u32(data, arr[0]);
            data += sizeof(std::uint32_t);

            put_u32(data, arr[1]);
            data += sizeof(std::uint32_t);
        }
    }

    Id page_id;

    // DB header page count after a commit (nonzero for commit frames, 0 for
    // all other frames).
    std::uint32_t db_size = 0;

    // Salts, copied from the WAL header. Must match what is in the header for the
    // frame to be considered valid.
    std::uint32_t salt[2] = {};

    std::uint32_t cksm[2] = {};
};

static constexpr std::size_t kHeaderSize = 64;
static constexpr std::size_t kNIndexHashes = 8192;
static constexpr std::size_t kNIndexFrames = 4096;
static constexpr std::size_t kHashPrime = 383;
static constexpr auto kNIndexFrames0 =
    kNIndexFrames - sizeof(WalIndexHeader) / sizeof(PageSlot);
static constexpr auto kTableSize =
    kNIndexFrames * sizeof(PageSlot) +
    kNIndexHashes * sizeof(HashSlot);

static constexpr auto wal_table_number(Id frame_id) -> std::size_t
{
    return (frame_id.as_index() + kNIndexFrames - kNIndexFrames0) / kNIndexFrames;
}

static auto wal_hash(std::size_t pgno) -> std::size_t
{
    return pgno * kHashPrime & (kNIndexHashes - 1);
}

static constexpr auto next_wal_hash(std::size_t hash) -> std::size_t
{
    return (hash + 1) & (kNIndexHashes - 1);
}

static auto too_many_collisions(Id page_id) -> Status
{
    std::string message;
    write_to_string(
        message,
        "too many WAL index collisions for page %u",
        page_id.value);
    return Status::corruption(message);
}

auto num_table_frames(std::size_t table_number) -> std::size_t
{
    return table_number ? kNIndexFrames : kNIndexFrames0;
}

WalIndex::WalIndex(WalIndexHeader &header)
    : m_header(&header)
{
}

WalIndex::~WalIndex()
{
    for (const auto *table : m_tables) {
        delete[] table;
    }
}

auto WalIndex::frame_for_page(Id page_id, Id min_frame_id, Id &out) -> Status
{
    out = Id::null();

    if (m_header->max_frame <= min_frame_id.value) {
        return Status::ok();
    }
    const auto min_table_number = wal_table_number(min_frame_id);

    for (auto n = wal_table_number(Id(m_header->max_frame));; --n) {
        auto table = create_or_open_table(n);
        CALICODB_EXPECT_LE(table.base, m_header->max_frame);
        const auto hash_min = min_frame_id.value >= table.base
                                  ? min_frame_id.value - table.base
                                  : 0;
        const auto hash_max = m_header->max_frame - table.base;
        auto collisions = kNIndexHashes;
        auto key = wal_hash(page_id.value);
        std::size_t h;

        // Find the WAL frame containing the given page. Limit the search to the set of
        // valid frames (in the range "min_frame_id" to "m_header->max_frame", inclusive).
        while ((h = table.hashes[key])) {
            if (collisions-- == 0) {
                return too_many_collisions(page_id);
            }
            const auto found =
                h >= hash_min &&
                h <= hash_max &&
                table.frames[h - 1] == page_id.value;
            if (found) {
                out = Id(table.base + h);
                break;
            }
            key = next_wal_hash(key);
        }
        if (!out.is_null() || n <= min_table_number) {
            break;
        }
    }
    return Status::ok();
}

auto WalIndex::page_for_frame(Id frame_id) -> Id
{
    auto table = create_or_open_table(wal_table_number(frame_id));
    if (table.base) {
        return Id(table.frames[(frame_id.as_index() - kNIndexFrames0) % kNIndexFrames]);
    } else {
        return Id(table.frames[frame_id.as_index()]);
    }
}

auto WalIndex::assign(Id page_id, Id frame_id) -> Status
{
    const auto table_number = wal_table_number(frame_id);
    auto table = create_or_open_table(table_number);
    if (frame_id.as_index() == table.base) {
        std::memset(table.frames, 0, sizeof *table.frames * num_table_frames(table_number));
        std::memset(table.hashes, 0, sizeof *table.hashes * kNIndexHashes);
    }
    CALICODB_EXPECT_LE(table.base, frame_id.value);
    const auto index = frame_id.value - table.base;
    CALICODB_EXPECT_LE(index, num_table_frames(table_number));

    auto key = wal_hash(page_id.value);
    // Use the relative frame index as the number of allowed collisions. This value is always
    // 1 more than the number of entries, so the worst case will succeed. Note that this only
    // works because frames are written in monotonically increasing order.
    auto collisions = index;

    // Find the first unused hash slot. Collisions are handled by incrementing the key to reach
    // the following hash slot, with wrapping once the end is hit. There are always more hash
    // slots than frames, so this search will always terminate.
    for (; table.hashes[key]; key = next_wal_hash(key)) {
        if (collisions-- == 0) {
            return too_many_collisions(page_id);
        }
    }

    table.hashes[key] = static_cast<HashSlot>(index);
    table.frames[index - 1] = page_id.value;
    return Status::ok();
}

auto WalIndex::create_or_open_table(std::size_t table_number) -> WalTable
{
    while (table_number >= m_tables.size()) {
        m_tables.emplace_back();
    }
    if (m_tables[table_number] == nullptr) {
        m_tables[table_number] = new char[kTableSize];
    }
    const auto base =
        table_number == 0 ? 0 : kNIndexFrames0 + (kNIndexFrames * (table_number - 1));
    auto *frames = reinterpret_cast<PageSlot *>(m_tables[table_number]);
    auto *hashes = reinterpret_cast<HashSlot *>(frames + kNIndexFrames);
    return WalTable {frames + kHeaderSize / sizeof *frames * !base, hashes, base};
}

auto WalIndex::cleanup() -> void
{
    if (m_header->max_frame != 0) {
        auto table = create_or_open_table(
            wal_table_number(Id(m_header->max_frame)));
        for (std::size_t i = 0; i < kNIndexHashes; ++i) {
            if (table.hashes[i] > m_header->max_frame) {
                table.hashes[i] = 0;
            }
        }
    }
}

// Merge 2 sorted lists
//
// "left" and "right" contain the indices and "frames" the sort keys.
static auto merge_lists(
    const PageSlot *frames,
    HashSlot *left,
    std::size_t left_size,
    HashSlot *&right,
    std::size_t &right_size,
    HashSlot *scratch)
{
    std::size_t L = 0;
    std::size_t r = 0;
    std::size_t i = 0;

    while (L < left_size || r < right_size) {
        HashSlot hash;
        if (L < left_size && (r >= right_size || frames[left[L]] < frames[right[r]])) {
            hash = left[L++];
        } else {
            hash = right[r++];
        }
        scratch[i++] = hash;
        if (L < left_size && frames[left[L]] == frames[hash]) {
            ++L;
        }
    }

    right = left;
    right_size = i;
    std::memcpy(left, scratch, i * sizeof *scratch);
}

static auto mergesort(
    const PageSlot *frames,
    HashSlot *hashes,
    HashSlot *scratch,
    std::size_t &list_size)
{
    struct SubList {
        HashSlot *ptr;
        std::size_t len;
    };

    SubList sublists[13] = {};
    std::size_t index = 0;
    std::size_t n_merge;
    HashSlot *p_merge;

    for (std::size_t L = 0; L < list_size; ++L) {
        p_merge = hashes + L;
        n_merge = 1;

        for (; L & (1 << index); ++index) {
            auto *sub = &sublists[index];
            merge_lists(frames, sub->ptr, sub->len, p_merge, n_merge, scratch);
        }
        sublists[index].ptr = p_merge;
        sublists[index].len = n_merge;
    }
    for (++index; index < 13; ++index) {
        if (list_size & (1 << index)) {
            auto *sub = &sublists[index];
            merge_lists(frames, sub->ptr, sub->len, p_merge, n_merge, scratch);
        }
    }
    CALICODB_EXPECT_EQ(p_merge, hashes);
    list_size = n_merge;
}

// TODO: This thing will mergesort (by page ID) and deduplicate the WAL frames so the pager
//       can write everything back sequentially (but not necessarily consecutively) during WAL reset.
//       First, allocate a slot for each WAL frame. Then, iterate through each index page, merging it
//       with the temp buffer (initially empty). Then, the temp buffer will contain some number of
//       sequential unique page IDs (less than or equal to the number of allocated slots).
//       .
//       This is what SQLite does to make resetting the WAL much faster. It must be well worth it to
//       sort by page ID first. Also, there are probably many duplicate pages, depending on the number,
//       of commits created by the user so deduplicating may make a big difference.
class WalIterator final
{
    struct Segment {
        std::vector<PageSlot> frames;
        std::vector<HashSlot> hashes;
        std::size_t next;
        std::size_t base;
    };

    std::vector<Segment> m_segments;

public:
    struct Location {
        Id page_id;
        Id frame_id;
    };

    ~WalIterator();

    [[nodiscard]] static auto open(Wal &wal, WalIterator *&out) -> Status;
    [[nodiscard]] auto location() const -> Location;
    [[nodiscard]] auto next() -> Status;
};

auto WalIterator::open(Wal &wal, WalIterator *&out) -> Status
{
    auto *itr = new WalIterator;
    (void)wal;
    out = itr;
    return Status::ok();
}

auto WalIterator::location() const -> Location
{
    return {};
}

auto WalIterator::next() -> Status
{
    return Status::ok();
}

class WalImpl : public Wal
{
public:
    explicit WalImpl(const Parameters &param, File &file);
    ~WalImpl() override;

    [[nodiscard]] auto read(Id page_id, char *page) -> Status override;
    [[nodiscard]] auto write(Id page_id, const Slice &page, std::size_t db_size) -> Status override;
    [[nodiscard]] auto checkpoint(File &db_file) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto commit() -> Status override;

    [[nodiscard]] auto statistics() const -> WalStatistics override
    {
        return m_stats;
    }

private:
    [[nodiscard]] auto frame_offset(Id frame_id) const -> std::size_t;
    [[nodiscard]] auto do_checkpoint(WalIterator &itr, File &db_file) -> Status;

    WalStatistics m_stats;
    WalIndexHeader m_index_header;
    WalIndex m_index;

    std::string m_read_scratch;
    std::string m_write_scratch;

    std::size_t m_page_size = 0;
    std::size_t m_backfill = 0;

    Env *m_env = nullptr;
    File *m_file = nullptr;

    Id m_min_frame;
};

auto Wal::open(const Parameters &param, Wal *&out) -> Status
{
    File *file;
    CALICODB_TRY(param.env->new_file(param.filename, file));
    out = new WalImpl(param, *file);
    return Status::ok();
}

Wal::~Wal() = default;

WalImpl::~WalImpl()
{
    delete m_file;
}

auto WalImpl::read(Id page_id, char *page) -> Status
{
    Id frame_id;
    CALICODB_TRY(m_index.frame_for_page(page_id, m_min_frame, frame_id));

    if (!frame_id.is_null()) {
        const auto frame_size = WalFrameHeader::kSize + m_page_size;
        auto offset = frame_offset(frame_id);

        CALICODB_TRY(m_file->read_exact(
            offset,
            frame_size,
            m_read_scratch.data()));

        //        WalFrameHeader header;
        //        header.read(m_read_scratch.data());

        std::memcpy(page, m_read_scratch.data() + WalFrameHeader::kSize, m_page_size);
        m_stats.bytes_read += WalFrameHeader::kSize + m_page_size;
        return Status::ok();
    }
    return Status::not_found("wal does not contain page");
}

auto WalImpl::write(Id page_id, const Slice &page, std::size_t db_size) -> Status
{
    CALICODB_EXPECT_EQ(page.size(), m_page_size);

    char buffer[WalFrameHeader::kSize];
    WalFrameHeader header;
    header.page_id = page_id;
    header.db_size = static_cast<std::uint32_t>(db_size);
    header.write(buffer);

    Id frame_id;
    CALICODB_TRY(m_index.frame_for_page(page_id, m_min_frame, frame_id));

    std::size_t offset;
    if (frame_id.is_null() || frame_id < m_min_frame) {
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it.
        offset = frame_offset(Id(++m_index_header.max_frame));
    } else {
        // Page has already been written. Overwrite the frame.
        offset = frame_offset(frame_id);
    }

    CALICODB_TRY(m_file->write(offset, Slice(buffer, WalFrameHeader::kSize)));
    CALICODB_TRY(m_file->write(offset + WalFrameHeader::kSize, page));
    m_stats.bytes_written += WalFrameHeader::kSize + m_page_size;
    return Status::ok();
}

auto WalImpl::checkpoint(File &db_file) -> Status
{
    CALICODB_TRY(m_file->sync());

    WalIterator *itr;
    CALICODB_TRY(WalIterator::open(*this, itr));
    auto s = do_checkpoint(*itr, db_file);
    if (s.is_ok()) {
        m_min_frame = Id::root();
        m_index_header.max_frame = 0;
    }
    delete itr;
    return s;
}

auto WalImpl::do_checkpoint(WalIterator &itr, File &db_file) -> Status
{
    for (;;) {
        const auto [page_id, frame_id] = itr.location();

        CALICODB_TRY(m_file->read_exact(
            frame_offset(frame_id),
            WalFrameHeader::kSize + m_page_size,
            m_read_scratch.data()));

        CALICODB_TRY(db_file.write(
            page_id.as_index() * m_page_size,
            Slice(m_read_scratch.data() + WalFrameHeader::kSize, m_page_size)));

        auto s = itr.next();
        if (s.is_not_found()) {
            break;
        } else if (!s.is_ok()) {
            return s;
        }
    }
    return Status::ok();
}

auto WalImpl::sync() -> Status
{
    return m_file->sync();
}

auto WalImpl::commit() -> Status
{
    m_min_frame.value = m_index_header.max_frame + 1;
    return Status::ok();
}

WalImpl::WalImpl(const Parameters &param, File &file)
    : m_index(m_index_header),
      m_read_scratch(WalFrameHeader::kSize + param.page_size, '\0'),
      m_write_scratch(32 * (WalFrameHeader::kSize + param.page_size), '\0'),
      m_page_size(param.page_size),
      m_env(param.env),
      m_file(&file),
      m_min_frame(1)
{
    (void)m_env;
    (void)m_backfill;
}

auto WalImpl::frame_offset(Id frame_id) const -> std::size_t
{
    CALICODB_EXPECT_FALSE(frame_id.is_null());
    return WalHeader::kSize + frame_id.as_index() * (WalFrameHeader::kSize + m_page_size);
}

} // namespace calicodb