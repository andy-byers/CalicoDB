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

using Key = HashIndex::Key;
using Value = HashIndex::Value;
using Hash = U16;

// Header is stored at the start of the first index group. std::memcpy() is used
// on the struct, so it needs to be a POD. Its size should be a multiple of 4 to
// prevent misaligned accesses.
static_assert(std::is_pod_v<HashIndexHeader>);
static_assert((sizeof(HashIndexHeader) & 0b11) == 0);

static constexpr U32 kHashPrime = 383;
static constexpr U32 kNIndexHashes = 8192;
static constexpr U32 kNIndexKeys = 4096;
static constexpr U32 kNIndexKeys0 =
    kNIndexKeys - sizeof(HashIndexHeader) / sizeof(U32);
static constexpr U32 kIndexPageSize =
    kNIndexKeys * sizeof(U32) +
    kNIndexHashes * sizeof(U16);

static constexpr auto index_group_number(Value value) -> U32
{
    return (value - 1 + kNIndexKeys - kNIndexKeys0) / kNIndexKeys;
}

static auto index_hash(Key key) -> Hash
{
    return key * kHashPrime & (kNIndexHashes - 1);
}

static constexpr auto next_index_hash(Hash hash) -> Hash
{
    return (hash + 1) & (kNIndexHashes - 1);
}

static auto too_many_collisions(Key key) -> Status
{
    std::string message;
    write_to_string(message, "too many WAL index collisions for page %u", key);
    return Status::corruption(message);
}

static constexpr auto encode_index_page_size(U32 page_size) -> U16
{
    return static_cast<U16>((page_size & 0xFF00) | page_size >> 16);
}

static constexpr auto decode_index_page_size(U16 page_size) -> U32
{
    return page_size == 1 ? kMaxPageSize : page_size;
}

struct HashGroup {
    explicit HashGroup(U32 group_number, char *data)
    {
        keys = reinterpret_cast<Key *>(data);
        hash = reinterpret_cast<Hash *>(keys + kNIndexKeys);
        if (group_number) {
            base = kNIndexKeys0 + (kNIndexKeys * (group_number - 1));
        } else {
            keys += sizeof(HashIndexHeader) / sizeof *keys;
        }
    }

    Key *keys = nullptr;
    Hash *hash = nullptr;
    U32 base = 0;
};

HashIndex::HashIndex(HashIndexHeader &header)
    : m_hdr(&header)
{
}

HashIndex::~HashIndex()
{
    for (const auto *group : m_groups) {
        delete[] group;
    }
}

auto HashIndex::header() -> HashIndexHeader *
{
    return reinterpret_cast<HashIndexHeader *>(group_data(0));
}

auto HashIndex::lookup(Key key, Value lower, Value &out) -> Status
{
    out = 0;

    const auto upper = m_hdr->max_frame;
    if (upper == 0) {
        return Status::ok();
    }
    if (lower == 0) {
        lower = 1;
    }
    const auto min_group_number = index_group_number(lower);

    for (auto n = index_group_number(m_hdr->max_frame);; --n) {
        HashGroup group(n, group_data(n));
        // The guard above prevents considering groups that haven't been allocated yet.
        // Such groups would start past the current "max_frame".
        CALICODB_EXPECT_LE(group.base, m_hdr->max_frame);
        auto collisions = kNIndexHashes;
        auto key_hash = index_hash(key);
        Hash relative;

        // Find the WAL frame containing the given page. Limit the search to the set of
        // valid frames (in the range "lower" to "m_hdr->max_frame", inclusive).
        while ((relative = group.hash[key_hash])) {
            if (collisions-- == 0) {
                return too_many_collisions(key);
            }
            const auto absolute = relative + group.base;
            const auto found =
                absolute >= lower &&
                absolute <= upper &&
                group.keys[relative - 1] == key;
            if (found) {
                out = absolute;
            }
            key_hash = next_index_hash(key_hash);
        }
        if (out || n <= min_group_number) {
            break;
        }
    }
    return Status::ok();
}

auto HashIndex::fetch(Value value) -> Key
{
    const auto n = index_group_number(value);
    if (n >= m_groups.size()) {
        return 0;
    }
    const HashGroup group(n, group_data(n));
    if (group.base) {
        return group.keys[(value - kNIndexKeys0 - 1) % kNIndexKeys];
    } else {
        return group.keys[value - 1];
    }
}

auto HashIndex::assign(Key key, Value value) -> Status
{
    const auto group_number = index_group_number(value);
    const auto key_capacity = group_number ? kNIndexKeys : kNIndexKeys0;
    HashGroup group(group_number, group_data(group_number));

    CALICODB_EXPECT_LT(group.base, value);
    const auto relative = value - group.base;
    CALICODB_EXPECT_LE(relative, key_capacity);
    if (relative == 1) {
        // Clear the whole group when the first entry is inserted.
        const auto group_size =
            key_capacity * sizeof *group.keys +
            kNIndexHashes * sizeof *group.hash;
        std::memset(group.keys, 0, group_size);
    }

    if (group.keys[relative - 1]) {
        cleanup();
        CALICODB_EXPECT_EQ(group.keys[relative - 1], 0);
    }

    CALICODB_EXPECT_LE(relative, key_capacity);

    auto key_hash = index_hash(key);
    // Use the relative frame index as the number of allowed collisions. This value is always
    // 1 more than the number of entries, so the worst case will succeed. Note that this only
    // works because frames are written in monotonically increasing order.
    auto collisions = relative;

    // Find the first unused hash slot. Collisions are handled by incrementing the key until an
    // unused slot is found, wrapping back to the start if the end is hit. There are always more
    // hash slots than frames, so this search will always terminate.
    for (; group.hash[key_hash]; key_hash = next_index_hash(key_hash)) {
        if (collisions-- == 0) {
            return too_many_collisions(key);
        }
    }

    group.hash[key_hash] = static_cast<Hash>(relative);
    group.keys[relative - 1] = key;
    return Status::ok();
}

auto HashIndex::group_data(std::size_t group_number) -> char *
{
    while (group_number >= m_groups.size()) {
        m_groups.emplace_back();
    }
    if (m_groups[group_number] == nullptr) {
        m_groups[group_number] = new char[kIndexPageSize]();
    }
    return m_groups[group_number];
}

auto HashIndex::cleanup() -> void
{
    if (m_hdr->max_frame != 0) {
        const auto n = index_group_number(m_hdr->max_frame);
        HashGroup group(n, group_data(n));
        const auto max_hash = m_hdr->max_frame - group.base;
        for (std::size_t i = 0; i < kNIndexHashes; ++i) {
            if (group.hash[i] > max_hash) {
                group.hash[i] = 0;
            }
        }
        // Clear the keys that correspond to cleared hash slots.
        const auto rest_size = static_cast<std::uintptr_t>(
            reinterpret_cast<const char *>(group.hash) -
            reinterpret_cast<const char *>(group.keys + max_hash));
        memset(group.keys + max_hash, 0, rest_size);
    }
}

// Merge 2 sorted lists.
static auto merge_lists(
    const Key *keys,
    Hash *left,
    U32 left_size,
    Hash *&right,
    U32 &right_size,
    Hash *scratch)
{
    U32 L = 0;
    U32 r = 0;
    U32 i = 0;

    while (L < left_size || r < right_size) {
        Hash hash;
        if (L < left_size && (r >= right_size || keys[left[L]] < keys[right[r]])) {
            hash = left[L++];
        } else {
            hash = right[r++];
        }
        scratch[i++] = hash;
        if (L < left_size && keys[left[L]] == keys[hash]) {
            ++L;
        }
    }

    right = left;
    right_size = i;
    std::memcpy(left, scratch, i * sizeof *scratch);
}

static auto mergesort(
    const Key *keys,
    Hash *hashes,
    Hash *scratch,
    U32 &size)
{
    struct SubList {
        Hash *ptr;
        U32 len;
    };

    static constexpr std::size_t kMaxBits = 13;

    SubList sublists[kMaxBits] = {};
    U32 sub_index = 0;
    U32 n_merge;
    Hash *p_merge;

    CALICODB_EXPECT_EQ(kNIndexKeys, 1 << (ARRAY_SIZE(sublists) - 1));
    CALICODB_EXPECT_LE(size, kNIndexKeys);
    CALICODB_EXPECT_GT(size, 0);

    for (U32 L = 0; L < size; ++L) {
        p_merge = hashes + L;
        n_merge = 1;

        for (sub_index = 0; L & (1 << sub_index); ++sub_index) {
            auto *sub = &sublists[sub_index];
            CALICODB_EXPECT_LT(sub_index, ARRAY_SIZE(sublists));
            CALICODB_EXPECT_NE(sub->ptr, nullptr);
            CALICODB_EXPECT_LE(sub->len, 1U << sub_index);
            CALICODB_EXPECT_EQ(sub->ptr, hashes + (L & ~U32((2 << sub_index) - 1)));
            merge_lists(keys, sub->ptr, sub->len, p_merge, n_merge, scratch);
        }
        sublists[sub_index].ptr = p_merge;
        sublists[sub_index].len = n_merge;
    }
    for (++sub_index; sub_index < kMaxBits; ++sub_index) {
        if (size & (1 << sub_index)) {
            auto *sub = sublists + sub_index;
            merge_lists(keys, sub->ptr, sub->len, p_merge, n_merge, scratch);
        }
    }
    CALICODB_EXPECT_EQ(p_merge, hashes);
    size = n_merge;

#ifndef NDEBUG
    for (std::size_t i = 1; i < size; ++i) {
        CALICODB_EXPECT_GT(keys[hashes[i]], keys[hashes[i - 1]]);
    }
#endif // NDEBUG
}

HashIterator::HashIterator(HashIndex &source)
    : m_source(&source)
{
    const auto *const hdr = m_source->m_hdr;

    // This method should not be called on an empty index.
    const auto last_value = hdr->max_frame;
    CALICODB_EXPECT_GT(last_value, 0);

    // TODO: Hopefully this makes this struct hack thing OK in C++... I would have tried to
    //       use std::aligned_storage, but there is that buffer of U16s allocated right
    //       after the array of State::Groups.
    static_assert(std::is_pod_v<State>);
    static_assert(std::is_pod_v<State::Group>);
    static_assert(0 == (alignof(State) & (alignof(Hash) - 1)));

    // Allocate internal buffers.
    m_num_groups = index_group_number(last_value) + 1;
    const auto state_size =
        sizeof(State) +                             // Includes storage for 1 group ("groups[1]").
        (m_num_groups - 1) * sizeof(State::Group) + // Additional groups.
        last_value * sizeof(Hash);                  // Indices to sort.
    m_state = reinterpret_cast<State *>(
        operator new(state_size, std::align_val_t {alignof(State)}));
    std::memset(m_state, 0, state_size);

    // Temporary buffer for the mergesort routine. Freed before returning from this routine.
    auto *temp = new Hash[last_value < kNIndexKeys ? last_value : kNIndexKeys]();

    for (U32 i = 0; i < m_num_groups; ++i) {
        HashGroup group(i, m_source->group_data(i));

        U32 group_size = kNIndexKeys;
        if (i + 1 == m_num_groups) {
            group_size = last_value - group.base;
        } else if (i == 0) {
            group_size = kNIndexKeys0;
        }

        // Pointer into the special index buffer located right after the group array.
        auto *index = reinterpret_cast<Hash *>(&m_state->groups[m_num_groups]) + group.base;

        for (std::size_t j = 0; j < group_size; ++j) {
            index[j] = static_cast<Hash>(j);
        }
        mergesort(group.keys, index, temp, group_size);
        m_state->groups[i].base = group.base + 1;
        m_state->groups[i].size = group_size;
        m_state->groups[i].keys = group.keys;
        m_state->groups[i].index = index;
    }
    delete[] temp;
}

HashIterator::~HashIterator()
{
    operator delete(m_state, std::align_val_t {alignof(State)});
}

auto HashIterator::read(Entry &out) -> bool
{
    static constexpr U32 kBadResult = 0xFFFFFFFF;
    CALICODB_EXPECT_LT(m_prior, kBadResult);
    auto result = kBadResult;

    auto *last_group = m_state->groups + m_num_groups - 1;
    for (std::size_t i = 0; i < m_num_groups; ++i) {
        auto *group = last_group - i;
        while (group->next < group->size) {
            const auto key = group->keys[group->index[group->next]];
            if (key > m_prior) {
                if (key < result) {
                    result = key;
                    out.value = group->base + group->index[group->next];
                }
                break;
            }
            ++group->next;
        }
    }
    m_prior = result;
    out.key = m_prior;
    return result != kBadResult;
}

// WAL header layout:
//     Offset  Size  Purpose
//    ---------------------------------------
//     0       4     Magic number (...)
//     4       4     WAL version (...)
//     8       4     DB page size
//     12      4     Checkpoint number
//     16      4     Salt-1
//     20      4     Salt-2
//     24      4     Checksum-1
//     28      4     Checksum-2
//
static constexpr std::size_t kWalHdrSize = 32;
static constexpr U32 kWalMagic = 0x87654321;
static constexpr U32 kWalVersion = 0x01'000'000;

// WAL frame header layout:
//     Offset  Size  Purpose
//    ---------------------------------------
//     0       4     Page number
//     4       4     DB size in pages (0 if not a commit frame)
//     8       4     Salt-1
//     12      4     Salt-2
//     16      4     Checksum-1
//     20      4     Checksum-2
//
struct WalFrameHdr {
    static constexpr std::size_t kSize = 24;

    U32 pgno = 0;

    // DB header page count after a commit (nonzero for commit frames, 0 for
    // all other frames).
    U32 db_size = 0;
};

class WalImpl : public Wal
{
public:
    explicit WalImpl(const Parameters &param, File &file);
    ~WalImpl() override;

    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto read(Id page_id, char *&page) -> Status override;
    [[nodiscard]] auto write(const CacheEntry *dirty, std::size_t db_size) -> Status override;
    [[nodiscard]] auto needs_checkpoint() const -> bool override;
    [[nodiscard]] auto checkpoint(File &db_file, std::size_t *db_size) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto abort() -> Status override;
    [[nodiscard]] auto close() -> Status override;

    [[nodiscard]] auto statistics() const -> WalStatistics override
    {
        return m_stats;
    }

private:
    [[nodiscard]] auto frame_offset(U32 frame) const -> std::size_t;
    [[nodiscard]] auto rewrite_checksums(U32 last) -> Status;
    [[nodiscard]] auto write_index_header() -> Status;
    [[nodiscard]] auto recover_index() -> Status;
    [[nodiscard]] auto restart_header(U32 salt_1) -> Status;
    [[nodiscard]] auto decode_frame(const char *frame, WalFrameHdr &out) -> bool;
    auto encode_frame(const WalFrameHdr &hdr, const char *page, char *out) -> void;

    WalStatistics m_stats;
    HashIndexHeader m_hdr = {};
    HashIndex m_index;

    std::string m_filename;

    // Storage for a single WAL frame.
    std::string m_frame;

    U32 m_page_size = 0;
    U32 m_redo_cksum = 0;
    U32 m_backfill = 0;
    U32 m_ckpt_number = 0;

    Env *m_env = nullptr;
    File *m_file = nullptr;

    U32 m_min_frame = 0;
};

auto Wal::open(const Parameters &param, Wal *&out) -> Status
{
    File *file;
    CALICODB_TRY(param.env->new_file(param.filename, file));
    auto *wal = new WalImpl(param, *file);
    out = wal;
    return wal->open();
}

auto Wal::close(Wal *&wal) -> Status
{
    Status s;
    if (wal != nullptr) {
        s = wal->close();
        delete wal;
        wal = nullptr;
    }
    return s;
}

Wal::~Wal() = default;

WalImpl::WalImpl(const Parameters &param, File &file)
    : m_index(m_hdr),
      m_filename(param.filename),
      m_frame(WalFrameHdr::kSize + param.page_size, '\0'),
      m_page_size(param.page_size),
      m_env(param.env),
      m_file(&file)
{
    (void)m_env;
    (void)m_backfill;
}

WalImpl::~WalImpl()
{
    delete m_file;
}

static auto compute_checksum(const Slice &in, const U32 *initial, U32 *out)
{
    CALICODB_EXPECT_NE(out, nullptr);
    CALICODB_EXPECT_EQ(std::uintptr_t(in.data()) & 3, 0);
    CALICODB_EXPECT_LE(in.size(), 65'536);
    CALICODB_EXPECT_EQ(in.size() & 7, 0);
    CALICODB_EXPECT_GT(in.size(), 0);

    U32 s1 = 0;
    U32 s2 = 0;
    if (initial != nullptr) {
        s1 = initial[0];
        s2 = initial[1];
    }

    const auto *ptr = reinterpret_cast<const U32 *>(in.data());
    const auto *end = ptr + in.size() / sizeof *ptr;

    do {
        s1 += *ptr++ + s2;
        s2 += *ptr++ + s1;
    } while (ptr < end);

    out[0] = s1;
    out[1] = s2;
}

auto WalImpl::open() -> Status
{
    return recover_index();
}

auto WalImpl::rewrite_checksums(U32 end) -> Status
{
    CALICODB_EXPECT_GT(m_redo_cksum, 0);

    // Find the offset of the previous checksum in the WAL file. If we are starting at
    // the first frame, get the previous checksum from the WAL header.
    std::size_t cksum_offset = 24;
    if (m_redo_cksum > 1) {
        cksum_offset = frame_offset(m_redo_cksum - 1) + 16;
    }

    CALICODB_TRY(m_file->read_exact(cksum_offset, 2 * sizeof(U32), m_frame.data()));
    m_hdr.frame_cksum[0] = get_u32(&m_frame[0]);
    m_hdr.frame_cksum[1] = get_u32(&m_frame[sizeof(U32)]);

    auto redo = m_redo_cksum;
    m_redo_cksum = 0;

    for (; redo < end; ++redo) {
        const auto offset = frame_offset(redo);
        CALICODB_TRY(m_file->read_exact(offset, WalFrameHdr::kSize + m_page_size, m_frame.data()));

        WalFrameHdr hdr;
        hdr.pgno = get_u32(&m_frame[0]);
        hdr.db_size = get_u32(&m_frame[4]);
        encode_frame(hdr, &m_frame[WalFrameHdr::kSize], m_frame.data());
        CALICODB_TRY(m_file->write(offset, Slice(m_frame).truncate(WalFrameHdr::kSize)));
    }
    return Status::ok();
}

auto WalImpl::recover_index() -> Status
{
    U32 frame_cksum[2] = {};
    Status s;

    const auto cleanup = [&] {
        return s;
    };

    const auto finish = [&] {
        if (s.is_ok()) {
            m_hdr.frame_cksum[0] = frame_cksum[0];
            m_hdr.frame_cksum[1] = frame_cksum[1];
            CALICODB_TRY(write_index_header());
        }
        return cleanup();
    };

    std::size_t file_size;
    CALICODB_TRY(m_env->file_size(m_filename, file_size));
    if (file_size > kWalHdrSize) {
        char header[kWalHdrSize];
        CALICODB_TRY(m_file->read_exact(0, sizeof(header), header));

        const auto magic = get_u32(&header[0]);
        m_page_size = get_u32(&header[8]);
        const auto is_valid =
            magic == kWalMagic &&
            is_power_of_two(m_page_size) &&
            m_page_size >= kMinPageSize &&
            m_page_size <= kMaxPageSize;
        if (!is_valid) {
            s = Status::corruption("WAL header is corrupted");
            return cleanup();
        }
        m_ckpt_number = get_u32(&header[12]);
        std::memcpy(m_hdr.salt, &header[16], sizeof(m_hdr.salt));

        compute_checksum(
            Slice(header, kWalHdrSize - 8),
            nullptr,
            m_hdr.frame_cksum);
        if (m_hdr.frame_cksum[0] != get_u32(&header[24]) ||
            m_hdr.frame_cksum[1] != get_u32(&header[28])) {
            return finish();
        }
        const auto version = get_u32(&header[4]);
        if (version != kWalVersion) {
            s = Status::corruption("unrecognized WAL version");
            return cleanup();
        }

        const auto last_frame = static_cast<U32>((file_size - kWalHdrSize) / m_frame.size());
        for (U32 n_group = 0; n_group <= index_group_number(last_frame); ++n_group) {
            const auto last = std::min(last_frame, kNIndexKeys0 + n_group * kNIndexKeys);
            const auto first = 1 + (n_group == 0 ? 0 : kNIndexKeys0 + (n_group - 1) * kNIndexKeys);
            for (auto n_frame = first; n_frame <= last; ++n_frame) {
                const auto offset = frame_offset(n_frame);
                CALICODB_TRY(m_file->read_exact(offset, m_frame.size(), m_frame.data()));
                // Decode the frame.
                WalFrameHdr hdr;
                if (!decode_frame(m_frame.data(), hdr)) {
                    break;
                }
                CALICODB_TRY(m_index.assign(hdr.pgno, n_frame));
                if (hdr.db_size) {
                    m_hdr.max_frame = n_frame;
                    m_hdr.page_count = hdr.db_size;
                    m_hdr.page_size = encode_index_page_size(m_page_size);
                    frame_cksum[0] = m_hdr.cksum[0];
                    frame_cksum[1] = m_hdr.cksum[1];
                }
            }
        }
    }

    return finish();
}

auto WalImpl::write_index_header() -> Status
{
    std::memcpy(m_index.header(), &m_hdr, sizeof(m_hdr));
    // TODO: Memory map the first page if it is not already mapped.
    return Status::ok();
}

auto WalImpl::restart_header(U32 salt_1) -> Status
{
    ++m_ckpt_number;
    m_hdr.max_frame = 0;
    auto *salt = reinterpret_cast<char *>(m_hdr.salt);
    put_u32(salt, get_u32(salt) + 1);
    std::memcpy(salt + sizeof(U32), &salt_1, sizeof(salt_1));
    return write_index_header();
}

auto WalImpl::encode_frame(const WalFrameHdr &hdr, const char *page, char *out) -> void
{
    put_u32(&out[0], hdr.pgno);
    put_u32(&out[4], hdr.db_size);

    if (m_redo_cksum) {
        std::memset(&out[8], 0, 16);
    } else {
        auto *cksum = m_hdr.frame_cksum;
        std::memcpy(&out[8], m_hdr.salt, 8);
        compute_checksum(Slice(out, 8), cksum, cksum);
        compute_checksum(Slice(page, m_page_size), cksum, cksum);
        put_u32(&out[16], cksum[0]);
        put_u32(&out[20], cksum[1]);
    }
    std::memcpy(&out[24], page, m_page_size);
}

auto WalImpl::decode_frame(const char *frame, WalFrameHdr &out) -> bool
{
    static constexpr std::size_t kDataFields = sizeof(U32) * 2;
    if (std::memcmp(m_hdr.salt, &frame[kDataFields], 8) != 0) {
        return false;
    }
    const auto pgno = get_u32(&frame[0]);
    if (pgno == 0) {
        return false;
    }
    auto *cksum = m_hdr.frame_cksum;
    compute_checksum(Slice(frame, kDataFields), cksum, cksum);
    compute_checksum(Slice(frame + WalFrameHdr::kSize, m_page_size), cksum, cksum);
    if (cksum[0] != get_u32(&frame[16]) ||
        cksum[1] != get_u32(&frame[20])) {
        return false;
    }
    out.pgno = pgno;
    out.db_size = get_u32(&frame[4]);
    return true;
}

auto WalImpl::read(Id page_id, char *&page) -> Status
{
    U32 frame;
    CALICODB_TRY(m_index.lookup(page_id.value, m_min_frame, frame));

    if (frame) {
        CALICODB_TRY(m_file->read_exact(
            frame_offset(frame),
            WalFrameHdr::kSize + m_page_size,
            m_frame.data()));

        std::memcpy(page, m_frame.data() + WalFrameHdr::kSize, m_page_size);
        m_stats.bytes_read += m_frame.size();
    } else {
        page = nullptr;
    }
    return Status::ok();
}

auto WalImpl::write(const CacheEntry *dirty, std::size_t db_size) -> Status
{
    const auto is_commit = db_size > 0;
    const auto *live = m_index.header();
    auto first_frame = m_min_frame;

    // Check if the WAL's copy of the index header differs from what is on the first index page.
    // If it is different, then the WAL has been written since the last commit, with the first
    // record located at frame number "first_frame".
    if (std::memcmp(&m_hdr, live, sizeof(HashIndexHeader)) != 0) {
        first_frame = live->max_frame + 1;
    }

    if (m_hdr.max_frame == 0) {
        // This is the first frame written to the WAL. Write the WAL header.
        char header[kWalHdrSize];
        U32 cksum[2];

        put_u32(&header[0], kWalMagic);
        put_u32(&header[4], kWalVersion);
        put_u32(&header[8], m_page_size);
        put_u32(&header[12], m_ckpt_number);
        if (m_ckpt_number == 0) {
            m_hdr.salt[0] = static_cast<U32>(rand());
            m_hdr.salt[1] = static_cast<U32>(rand());
        }
        std::memcpy(&header[16], m_hdr.salt, sizeof(m_hdr.salt));
        compute_checksum(Slice(header, sizeof(header) - sizeof(cksum)), nullptr, cksum);
        put_u32(&header[24], cksum[0]);
        put_u32(&header[28], cksum[1]);

        m_hdr.frame_cksum[0] = cksum[0];
        m_hdr.frame_cksum[1] = cksum[1];

        CALICODB_TRY(m_file->write(0, Slice(header, sizeof(header))));
    }

    // Write each dirty page to the WAL.
    auto next_frame = m_hdr.max_frame + 1;
    for (auto *p = dirty; p; p = p->next) {
        U32 frame;

        // Condition ensures that if this set of pages completes a transaction, then
        // the last frame will always be appended, even if another copy of the page
        // exists in the WAL for this transaction. This frame needs to have its
        // "db_size" field set to mark that it is a commit frame.
        if (first_frame && (p->next || !is_commit)) {
            // Check to see if the page has been written to the WAL already by the
            // current transaction. If so, overwrite it and indicate that checksums
            // need to be recomputed from here on commit.
            CALICODB_TRY(m_index.lookup(p->page_id.value, first_frame, frame));
            if (frame) {
                if (m_redo_cksum == 0 || frame < m_redo_cksum) {
                    m_redo_cksum = frame;
                }
                const Slice page(p->page, m_page_size);
                CALICODB_TRY(m_file->write(frame_offset(frame) + WalFrameHdr::kSize, page));
                continue;
            }
        }
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it.
        WalFrameHdr header;
        header.pgno = p->page_id.value;
        header.db_size = p->next == nullptr ? static_cast<U32>(db_size) : 0;
        encode_frame(header, p->page, m_frame.data());
        CALICODB_TRY(m_file->write(frame_offset(next_frame), m_frame));
        m_stats.bytes_written += m_frame.size();

        // TODO: SQLite does this after the loop finishes, in another loop. They pad out remaining frames to make a full sector
        //       if some option is set, to guard against torn writes.
        CALICODB_TRY(m_index.assign(header.pgno, next_frame));
        ++next_frame;
    }

    if (is_commit && m_redo_cksum) {
        CALICODB_TRY(rewrite_checksums(next_frame));
    }

    m_hdr.max_frame = next_frame - 1;
    if (is_commit) {
        // If this is a commit, then at least 1 frame (the commit frame) must be written. The
        // pager has logic to make sure of this (the root page is forcibly written if no pages
        // are dirty).
        CALICODB_EXPECT_TRUE(dirty);
        m_hdr.page_count = static_cast<U32>(db_size);
        ++m_hdr.change;
        return write_index_header();
    }

    return Status::ok();
}

auto WalImpl::needs_checkpoint() const -> bool
{
    return m_hdr.max_frame > 1'000;
}

auto WalImpl::checkpoint(File &db_file, std::size_t *db_size) -> Status
{
    CALICODB_TRY(m_file->sync());
    if (db_size != nullptr) {
        *db_size = m_hdr.page_count;
    }

    // TODO: This should be set to the max frame still needed by a reader.
    auto max_safe_frame = m_hdr.max_frame;
    if (max_safe_frame == 0) {
        return Status::ok();
    }
    HashIterator itr(m_index);
    for (;;) {
        HashIterator::Entry entry;
        if (!itr.read(entry)) {
            break;
        }

        CALICODB_TRY(m_file->read_exact(
            frame_offset(entry.value) + WalFrameHdr::kSize,
            m_page_size,
            m_frame.data()));

        CALICODB_TRY(db_file.write(
            (entry.key - 1) * m_page_size,
            Slice(m_frame.data(), m_page_size)));

        // TODO: Should increase backfill count here.
    }
    ++m_ckpt_number;
    m_min_frame = 0;
    m_hdr.max_frame = 0;
    m_index.cleanup();
    return restart_header(static_cast<U32>(rand()));
}

auto WalImpl::sync() -> Status
{
    return m_file->sync();
}

auto WalImpl::abort() -> Status
{
    if (m_hdr.max_frame) {
        m_hdr = *m_index.header();
        m_index.cleanup();
    }
    return Status::ok();
}

auto WalImpl::close() -> Status
{
    if (m_hdr.max_frame == 0) {
        return m_env->remove_file(m_filename);
    }
    return sync();
}

auto WalImpl::frame_offset(U32 frame) const -> std::size_t
{
    CALICODB_EXPECT_GT(frame, 0);
    return kWalHdrSize + (frame - 1) * (WalFrameHdr::kSize + m_page_size);
}

} // namespace calicodb