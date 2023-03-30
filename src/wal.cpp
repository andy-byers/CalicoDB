// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal.h"
#include "encoding.h"
#include "env_posix.h"
#include "frames.h"
#include "logging.h"

namespace calicodb
{

using HashSlot = U16;
using PageSlot = U32;

struct WalHeader {
    static constexpr std::size_t kSize = 32;
    static constexpr U32 kMagic = 0x87654321; // TODO: more-or-less arbitrary, but could make it spell something cool?
    static constexpr U32 kVersion = 0x01'000'000;

    U32 magic_code = kMagic;
    U32 version = kVersion;
    U32 page_size = 0;
    U32 commit = 0;
    U32 salt[2] = {};
    U32 cksum[2] = {};
};

struct CheckpointInfo {
    // Number of WAL frames that have been written back to the DB.
    std::size_t backfill = 0;

    // The code that handles checkpoints will set this field before attempting to
    // backfill any frames. "backfill_attempts" <= "backfill" is always true, as
    // is "backfill" <= "max_frame", where "max_frame" is the largest frame ID
    // contained in the WAL.
    std::size_t backfill_attempts = 0;
};

struct WalFrameHeader {
    static constexpr std::size_t kSize = 24;

    auto read(const char *data) -> void
    {
        page_id.value = get_u32(data);
        data += sizeof(U32);

        db_size = get_u32(data);
        data += sizeof(U32);

        for (auto *arr : {salt, cksum}) {
            arr[0] = get_u32(data);
            data += sizeof(U32);

            arr[1] = get_u32(data);
            data += sizeof(U32);
        }
    }

    Id page_id;

    // DB header page count after a commit (nonzero for commit frames, 0 for
    // all other frames).
    U32 db_size = 0;

    // Salts, copied from the WAL header. Must match what is in the header for the
    // frame to be considered valid.
    U32 salt[2] = {};

    U32 cksum[2] = {};
};

static constexpr std::size_t kHeaderSize = 64;
static constexpr std::size_t kNIndexHashes = 8192;
static constexpr std::size_t kNIndexFrames = 4096;
static constexpr std::size_t kHashPrime = 383;
static constexpr auto kNIndexFrames0 =
    kNIndexFrames - sizeof(WalIndexHeader) / sizeof(PageSlot);
static constexpr auto kIndexSegmentSize =
    kNIndexFrames * sizeof(PageSlot) +
    kNIndexHashes * sizeof(HashSlot);

static constexpr auto wal_segment_number(Id frame_id) -> std::size_t
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

auto num_segment_frames(std::size_t segment_number) -> std::size_t
{
    return segment_number ? kNIndexFrames : kNIndexFrames0;
}

WalIndex::WalIndex(WalIndexHeader &header)
    : m_header(&header)
{
}

WalIndex::~WalIndex()
{
    for (const auto *segment : m_segments) {
        delete[] segment;
    }
}

auto WalIndex::header() -> WalIndexHeader *
{
    return reinterpret_cast<WalIndexHeader *>(m_segments.front());
}

auto WalIndex::frame_for_page(Id page_id, Id min_frame_id, Id &out) -> Status
{
    out = Id::null();

    if (m_header->max_frame <= min_frame_id.value) {
        return Status::ok();
    }
    const auto min_segment_number = wal_segment_number(min_frame_id);

    for (auto n = wal_segment_number(Id(m_header->max_frame));; --n) {
        auto segment = create_or_open_segment(n);
        CALICODB_EXPECT_LE(segment.base, m_header->max_frame);
        const auto hash_min = min_frame_id.value >= segment.base
                                  ? min_frame_id.value - segment.base
                                  : 0;
        const auto hash_max = m_header->max_frame - segment.base;
        auto collisions = kNIndexHashes;
        auto key = wal_hash(page_id.value);
        std::size_t h;

        // Find the WAL frame containing the given page. Limit the search to the set of
        // valid frames (in the range "min_frame_id" to "m_header->max_frame", inclusive).
        while ((h = segment.hashes[key])) {
            if (collisions-- == 0) {
                return too_many_collisions(page_id);
            }
            const auto found =
                h >= hash_min &&
                h <= hash_max &&
                segment.frames[h - 1] == page_id.value;
            if (found) {
                out = Id(segment.base + h);
                break;
            }
            key = next_wal_hash(key);
        }
        if (!out.is_null() || n <= min_segment_number) {
            break;
        }
    }
    return Status::ok();
}

auto WalIndex::page_for_frame(Id frame_id) -> Id
{
    auto segment = create_or_open_segment(wal_segment_number(frame_id));
    if (segment.base) {
        return Id(segment.frames[(frame_id.as_index() - kNIndexFrames0) % kNIndexFrames]);
    } else {
        return Id(segment.frames[frame_id.as_index()]);
    }
}

auto WalIndex::assign(Id page_id, Id frame_id) -> Status
{
    const auto segment_number = wal_segment_number(frame_id);
    auto segment = create_or_open_segment(segment_number);
    if (frame_id.as_index() == segment.base) {
        std::memset(segment.frames, 0, sizeof *segment.frames * num_segment_frames(segment_number));
        std::memset(segment.hashes, 0, sizeof *segment.hashes * kNIndexHashes);
    }
    CALICODB_EXPECT_LE(segment.base, frame_id.value);
    const auto index = frame_id.value - segment.base;
    CALICODB_EXPECT_LE(index, num_segment_frames(segment_number));

    auto key = wal_hash(page_id.value);
    // Use the relative frame index as the number of allowed collisions. This value is always
    // 1 more than the number of entries, so the worst case will succeed. Note that this only
    // works because frames are written in monotonically increasing order.
    auto collisions = index;

    // Find the first unused hash slot. Collisions are handled by incrementing the key until an
    // unused slot is found, wrapping back to the start if the end is hit. There are always more 
    // hash slots than frames, so this search will always terminate.
    for (; segment.hashes[key]; key = next_wal_hash(key)) {
        if (collisions-- == 0) {
            return too_many_collisions(page_id);
        }
    }

    segment.hashes[key] = static_cast<HashSlot>(index);
    segment.frames[index - 1] = page_id.value;
    return Status::ok();
}

auto WalIndex::create_or_open_segment(std::size_t segment_number) -> Segment
{
    while (segment_number >= m_segments.size()) {
        m_segments.emplace_back();
    }
    if (m_segments[segment_number] == nullptr) {
        m_segments[segment_number] = new char[kIndexSegmentSize];
    }
    const auto base =
        segment_number == 0 ? 0 : kNIndexFrames0 + (kNIndexFrames * (segment_number - 1));
    auto *frames = reinterpret_cast<PageSlot *>(m_segments[segment_number]);
    auto *hashes = reinterpret_cast<HashSlot *>(frames + kNIndexFrames);
    return Segment {frames + kHeaderSize / sizeof *frames * !base, hashes, base};
}

auto WalIndex::cleanup() -> void
{
    if (m_header->max_frame != 0) {
        auto segment = create_or_open_segment(
            wal_segment_number(Id(m_header->max_frame)));
        for (std::size_t i = 0; i < kNIndexHashes; ++i) {
            if (segment.hashes[i] > m_header->max_frame) {
                segment.hashes[i] = 0;
            }
        }
    }
}

//// Merge 2 sorted lists
////
//// "left" and "right" contain the indices and "frames" the sort keys.
//static auto merge_lists(
//    const PageSlot *frames,
//    HashSlot *left,
//    std::size_t left_size,
//    HashSlot *&right,
//    std::size_t &right_size,
//    HashSlot *scratch)
//{
//    std::size_t L = 0;
//    std::size_t r = 0;
//    std::size_t i = 0;
//
//    while (L < left_size || r < right_size) {
//        HashSlot hash;
//        if (L < left_size && (r >= right_size || frames[left[L]] < frames[right[r]])) {
//            hash = left[L++];
//        } else {
//            hash = right[r++];
//        }
//        scratch[i++] = hash;
//        if (L < left_size && frames[left[L]] == frames[hash]) {
//            ++L;
//        }
//    }
//
//    right = left;
//    right_size = i;
//    std::memcpy(left, scratch, i * sizeof *scratch);
//}
//
//static auto mergesort(
//    const PageSlot *frames,
//    HashSlot *hashes,
//    HashSlot *scratch,
//    std::size_t &list_size)
//{
//    struct SubList {
//        HashSlot *ptr;
//        std::size_t len;
//    };
//
//    SubList sublists[13] = {};
//    std::size_t index = 0;
//    std::size_t n_merge;
//    HashSlot *p_merge;
//
//    for (std::size_t L = 0; L < list_size; ++L) {
//        p_merge = hashes + L;
//        n_merge = 1;
//
//        for (; L & (1 << index); ++index) {
//            auto *sub = &sublists[index];
//            merge_lists(frames, sub->ptr, sub->len, p_merge, n_merge, scratch);
//        }
//        sublists[index].ptr = p_merge;
//        sublists[index].len = n_merge;
//    }
//    for (++index; index < 13; ++index) {
//        if (list_size & (1 << index)) {
//            auto *sub = &sublists[index];
//            merge_lists(frames, sub->ptr, sub->len, p_merge, n_merge, scratch);
//        }
//    }
//    CALICODB_EXPECT_EQ(p_merge, hashes);
//    list_size = n_merge;
//}

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
    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t db_size) -> Status override;
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
    [[nodiscard]] auto rewrite_checksums() -> Status;
    [[nodiscard]] auto write_index_header() -> Status;

    WalStatistics m_stats;
    WalIndexHeader m_hdr;
    WalIndex m_index;

    std::string m_scratch;

    std::size_t m_page_size = 0;
    Id m_redo_cksum;
    Id m_backfill;

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

static auto compute_checksum(const Slice &in, const U32 *initial = nullptr)
{
    CALICODB_EXPECT_GE(in.size(), 8);
    CALICODB_EXPECT_EQ(in.size() & 7, 0);
    CALICODB_EXPECT_LE(in.size(), 65'536);

    const auto *data = reinterpret_cast<const U32 *>(in.data());
    const auto *end = data + in.size() / sizeof(U32);

    U32 s1 = 0;
    U32 s2 = 0;
    if (initial != nullptr) {
        s1 = initial[0];
        s2 = initial[2];
    }

    do {
        s1 += *data++ + s2;
        s2 += *data++ + s1;
    } while (data < end);

    return std::make_pair(s1, s2);
}

auto WalImpl::rewrite_checksums() -> Status
{
    compute_checksum(Slice(), nullptr);
    return Status::ok();
}

auto WalImpl::write_index_header() -> Status
{
    return Status::ok();
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
            m_scratch.data()));

        //        WalFrameHeader header;
        //        header.read(m_scratch.data());

        std::memcpy(page, m_scratch.data() + WalFrameHeader::kSize, m_page_size);
        m_stats.bytes_read += WalFrameHeader::kSize + m_page_size;
        return Status::ok();
    }
    return Status::not_found("wal does not contain page");
}

static auto write_frame(File &file, const WalFrameHeader &header, const Slice &page, std::size_t offset)
{
    char buffer[WalFrameHeader::kSize];
    auto *data = buffer;

    put_u32(data, header.page_id.value);
    data += sizeof(U32);

    put_u32(data, header.db_size);
    data += sizeof(U32);

    for (const auto *arr : {header.salt, header.cksum}) {
        put_u32(data, arr[0]);
        data += sizeof(U32);

        put_u32(data, arr[1]);
        data += sizeof(U32);
    }

    CALICODB_TRY(file.write(offset, Slice(buffer, WalFrameHeader::kSize)));
    return file.write(offset + WalFrameHeader::kSize, page);
}

auto WalImpl::write(const CacheEntry *dirty, std::size_t db_size) -> Status
{
    if (m_hdr.max_frame == 0) {
        // This is the first frame written to the WAL. Write the WAL header.
//        char buffer[WalHeader::kSize];
//        auto *data = buffer;

    }

    const auto *live = m_index.header();
    Id first;

    // Check if the WAL's copy of the index header differs from what is in the index. If so,
    // then the WAL has been written since the last commit, with the first record located at
    // frame ID "first".
    if (std::memcmp(&m_hdr, live, sizeof(WalIndexHeader)) != 0) {
        first.value = live->max_frame + 1;
    }

    // Write each dirty page to the WAL.
    Id last(m_hdr.max_frame);
    for (auto *p = dirty; p; p = p->next) {
        // Never overwrite frames on commit. Need the "db_size" field to be set on the
        // final frame.
        if (!first.is_null() && db_size == 0) {
            Id frame_id;
            CALICODB_TRY(m_index.frame_for_page(p->page_id, m_min_frame, frame_id));
            if (first <= frame_id) {
                // Page has already been written. Overwrite the frame.
                if (m_redo_cksum.is_null() || frame_id < m_redo_cksum) {
                    m_redo_cksum = frame_id;
                }
                const auto offset = frame_offset(frame_id) + WalFrameHeader::kSize;
                CALICODB_TRY(m_file->write(offset, p->page));
                continue;
            }
        }
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it.
        WalFrameHeader header;
        header.page_id = p->page_id;
        header.db_size = p->next == nullptr ? static_cast<U32>(db_size) : 0;
        CALICODB_TRY(write_frame(*m_file, header, p->page, frame_offset(last)));
        m_stats.bytes_written += WalFrameHeader::kSize + m_page_size;

        // TODO: SQLite does this after the loop finishes.
        CALICODB_TRY(m_index.assign(p->page_id, last));
        ++last.value;
    }

    m_hdr.max_frame = last.value;
    if (db_size) {
        ++m_hdr.change;
        m_hdr.page_count = static_cast<U32>(db_size);
        return write_index_header();
    }

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
        m_hdr.max_frame = 0;
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
            m_scratch.data()));

        CALICODB_TRY(db_file.write(
            page_id.as_index() * m_page_size,
            Slice(m_scratch.data() + WalFrameHeader::kSize, m_page_size)));

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
    m_min_frame.value = m_hdr.max_frame + 1;
    return Status::ok();
}

WalImpl::WalImpl(const Parameters &param, File &file)
    : m_index(m_hdr),
      m_scratch(WalFrameHeader::kSize + param.page_size, '\0'),
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