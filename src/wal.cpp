// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal.h"
#include "alloc.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "logging.h"
#include "page.h"
#include "ptr.h"
#include "stat.h"

namespace calicodb
{

// Compiler intrinsics for atomic loads and stores
#define ATOMIC_LOAD(p) __atomic_load_n(p, __ATOMIC_RELAXED)
#define ATOMIC_STORE(p, v) __atomic_store_n(p, v, __ATOMIC_RELAXED)

using Key = HashIndex::Key;
using Value = HashIndex::Value;
using Hash = uint16_t;

// Non-volatile pointer types.
using StablePtr = char *;
using ConstStablePtr = const char *;

// Simple routines for working with the hash index header. The volatile-qualified
// parameter must be a pointer to the start of one of the copies of the index header
// in shared memory. The other parameter must be a pointer to a local copy of the
// header.
// Note that these routines are able to copy in chunks of uint64_t, because the
// memory region starting at the first index header is either the address returned
// by mmap()/VirtualAlloc(), or it is a pointer returned by malloc(). Either way,
// it should be suitably aligned. HashIndexHdr has a size that is a multiple of 8,
// so the second copy of the header should be properly aligned as well.
template <class Src, class Dst>
static auto read_hdr(const volatile Src *src, Dst *dst) -> void
{
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(src) & (alignof(uint64_t) - 1), 0);
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(dst) & (alignof(uint64_t) - 1), 0);
    const volatile auto *src64 = reinterpret_cast<const volatile uint64_t *>(src);
    auto *dst64 = reinterpret_cast<uint64_t *>(dst);
    for (size_t i = 0; i < sizeof(HashIndexHdr) / sizeof *src64; ++i) {
        dst64[i] = ATOMIC_LOAD(&src64[i]);
    }
}
template <class Src, class Dst>
static auto write_hdr(const Src *src, volatile Dst *dst) -> void
{
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(src) & (alignof(uint64_t) - 1), 0);
    CALICODB_EXPECT_EQ(reinterpret_cast<std::uintptr_t>(dst) & (alignof(uint64_t) - 1), 0);
    const auto *src64 = reinterpret_cast<const uint64_t *>(src);
    volatile auto *dst64 = reinterpret_cast<volatile uint64_t *>(dst);
    for (size_t i = 0; i < sizeof(HashIndexHdr) / sizeof *src64; ++i) {
        ATOMIC_STORE(&dst64[i], src64[i]);
    }
}
template <class Shared, class Local>
static auto compare_hdr(const volatile Shared *shared, const Local *local) -> int
{
    const volatile auto *shared64 = reinterpret_cast<const volatile uint64_t *>(shared);
    const auto *local64 = reinterpret_cast<const uint64_t *>(local);
    for (size_t i = 0; i < sizeof(HashIndexHdr) / sizeof *shared64; ++i) {
        if (ATOMIC_LOAD(&shared64[i]) != local64[i]) {
            return 1;
        }
    }
    return 0;
}
// Must be true for the above routines to work properly.
static_assert(0 == sizeof(HashIndexHdr) % sizeof(uint64_t));

static constexpr size_t kReadmarkNotUsed = 0xFF'FF'FF'FF;
static constexpr size_t kWriteLock = 0;
static constexpr size_t kNotWriteLock = 1;
static constexpr size_t kCheckpointLock = 1;
static constexpr size_t kRecoveryLock = 2;
static constexpr size_t kReaderCount = File::kShmLockCount - 3;
#define READ_LOCK(i) static_cast<size_t>((i) + 3)

struct CkptInfo {
    // Maximum frame number that has been written back to the database file. Readers
    // act like frames below this number do not exist.
    uint32_t backfill;

    // 5 "readmarks" corresponding to the 5 read locks in the following field. The
    // first readmark always has a value of 0. The remaining readmarks store maximum
    // frame numbers for the various readers attached to the WAL. If a reader is
    // using readmark 0, it will ignore the WAL and read pages directly from the
    // database. This implementation allows 5 versions of the database to be viewed
    // at any given time. More than 1 reader can attach to each readmark.
    uint32_t readmark[kReaderCount];

    // 8 lock bytes (never read or written):
    //    +-------+-------+-------+-------+
    //    | Write | Ckpt  | Rcvr  | Read0 |
    //    +-------+-------+-------+-------+
    //    | Read1 | Read2 | Read3 | Read4 |
    //    +-------+-------+-------+-------+
    uint8_t locks[File::kShmLockCount];

    // Maximum frame number that a checkpointer attempted to write back to the
    // database file. This value is set before the backfill field, so that
    // checkpointer failures can be detected.
    uint32_t backfill_attempted;

    // Reserved for future expansion.
    uint32_t reserved;
};
static_assert(std::is_pod_v<CkptInfo>);

static constexpr size_t kIndexHdrSize = sizeof(HashIndexHdr) * 2 + sizeof(CkptInfo);

// Header is stored at the start of the first index group. std::memcpy() is used
// on the struct, so it needs to be a POD (or at least trivially copiable). Its
// size should be a multiple of 4 to prevent misaligned accesses.
static_assert(std::is_pod_v<HashIndexHdr>);
static_assert((kIndexHdrSize & 0b11) == 0);

static constexpr uint32_t kNIndexHashes = 8192;
static constexpr uint32_t kNIndexKeys = 4096;
static constexpr uint32_t kNIndexKeys0 =
    kNIndexKeys - kIndexHdrSize / sizeof(uint32_t);

static constexpr auto index_group_number(Value value) -> uint32_t
{
    return (value - 1 + kNIndexKeys - kNIndexKeys0) / kNIndexKeys;
}

static auto index_hash(Key key) -> Hash
{
    static constexpr uint32_t kHashPrime = 383;
    return key * kHashPrime & (kNIndexHashes - 1);
}
static constexpr auto next_index_hash(Hash hash) -> Hash
{
    return (hash + 1) & (kNIndexHashes - 1);
}

static auto too_many_collisions() -> Status
{
    return Status::corruption("too many WAL index collisions");
}

struct HashGroup {
    explicit HashGroup(uint32_t group_number, volatile char *data)
        : keys(reinterpret_cast<volatile Key *>(data)),
          hash(reinterpret_cast<volatile Hash *>(keys + kNIndexKeys))
    {
        if (group_number) {
            base = kNIndexKeys0 + (kNIndexKeys * (group_number - 1));
        } else {
            keys += kIndexHdrSize / sizeof *keys;
        }
    }

    volatile Key *keys;
    volatile Hash *hash;
    uint32_t base = 0;
};

HashIndex::HashIndex(HashIndexHdr &header, File *file)
    : m_hdr(&header),
      m_file(file)
{
}

auto HashIndex::header() -> volatile HashIndexHdr *
{
    CALICODB_EXPECT_GT(m_num_groups, 0);
    CALICODB_EXPECT_NE(m_groups[0], nullptr);
    return reinterpret_cast<volatile HashIndexHdr *>(m_groups[0]);
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
    Status s;
    const auto min_group_number = index_group_number(lower);
    for (auto n = index_group_number(m_hdr->max_frame);; --n) {
        s = map_group(n, false);
        if (!s.is_ok()) {
            break;
        }
        CALICODB_EXPECT_TRUE(m_groups[n]);
        HashGroup group(n, m_groups[n]);
        // The guard above prevents considering groups that haven't been allocated yet.
        // Such groups would start past the current "max_frame".
        CALICODB_EXPECT_LE(group.base, m_hdr->max_frame);
        auto collisions = kNIndexHashes;
        auto key_hash = index_hash(key);
        Hash relative;

        // Find the WAL frame containing the given page. Limit the search to the set of
        // valid frames for this connection (in the range `lower` to `m_hdr->max_frame`,
        // inclusive).
        while ((relative = ATOMIC_LOAD(&group.hash[key_hash]))) {
            if (collisions-- == 0) {
                return too_many_collisions();
            }
            const auto absolute = relative + group.base;
            const auto found =
                absolute <= upper &&
                absolute >= lower &&
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
    return s;
}

auto HashIndex::fetch(Value value) -> Key
{
    const auto n = index_group_number(value);
    if (n >= m_num_groups) {
        return 0;
    }
    CALICODB_EXPECT_LT(n, m_num_groups);
    CALICODB_EXPECT_TRUE(m_groups[n]);
    const HashGroup group(n, m_groups[n]);
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

    // REQUIRES: WAL writer lock is held.
    CALICODB_TRY(map_group(group_number, true));
    HashGroup group(group_number, m_groups[group_number]);

    CALICODB_EXPECT_LT(group.base, value);
    const auto relative = value - group.base;
    CALICODB_EXPECT_LE(relative, key_capacity);
    if (relative == 1) {
        // Clear the whole group when the first entry is inserted.
        const auto group_size =
            key_capacity * sizeof *group.keys +
            kNIndexHashes * sizeof *group.hash;
        std::memset(StablePtr(group.keys), 0, group_size);
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

    // Find the first unused hash slot. Collisions are handled by incrementing the hash until an
    // unused slot is found, wrapping back to the start if the end is hit. There are always more
    // hash slots than frames, so this search will always terminate.
    for (; ATOMIC_LOAD(&group.hash[key_hash]); key_hash = next_index_hash(key_hash)) {
        if (collisions-- == 0) {
            return too_many_collisions();
        }
    }
    group.keys[relative - 1] = key;
    ATOMIC_STORE(&group.hash[key_hash], static_cast<Hash>(relative));
    return Status::ok();
}

auto HashIndex::map_group(size_t group_number, bool extend) -> Status
{
    if (m_num_groups <= group_number) {
        static constexpr size_t kPtrWidth = sizeof(volatile char *);
        const auto needed_len = group_number + 1;
        if (auto **groups = static_cast<volatile char **>(Alloc::realloc(
                m_groups, needed_len * kPtrWidth))) {
            std::memset(groups + m_num_groups, 0,
                        (needed_len - m_num_groups) * kPtrWidth);
            m_groups = groups;
            m_num_groups = needed_len;
        } else {
            return Status::no_memory();
        }
    }

    Status s;
    if (m_groups[group_number] == nullptr) {
        volatile void *ptr;
        if (m_file) {
            s = m_file->shm_map(group_number, extend, ptr);
        } else {
            auto *buf = Alloc::malloc(File::kShmRegionSize);
            if (buf == nullptr) {
                s = Status::no_memory();
            } else if (group_number == 0) {
                std::memset(buf, 0, kIndexHdrSize);
            }
            ptr = buf;
        }
        m_groups[group_number] = reinterpret_cast<volatile char *>(ptr);
    }
    return s;
}

auto HashIndex::groups() const -> volatile char **
{
    return m_groups;
}

auto HashIndex::cleanup() -> void
{
    if (m_hdr->max_frame) {
        const auto n = index_group_number(m_hdr->max_frame);
        CALICODB_EXPECT_TRUE(m_groups[n]); // Must already be mapped
        HashGroup group(n, m_groups[n]);
        // Clear obsolete hash slots. No other connections should be using these slots,
        // since this connection must be the only writer, and other readers are excluded
        // from this range by their "max frame" values.
        const auto max_hash = m_hdr->max_frame - group.base;
        for (size_t i = 0; i < kNIndexHashes; ++i) {
            if (group.hash[i] > max_hash) {
                group.hash[i] = 0;
            }
        }
        // Clear the keys that correspond to cleared hash slots.
        const auto rest_size = static_cast<std::uintptr_t>(
            ConstStablePtr(group.hash) -
            ConstStablePtr(group.keys + max_hash));
        std::memset(StablePtr(group.keys + max_hash), 0, rest_size);
    }
}

auto HashIndex::close() -> void
{
    if (m_file) {
        m_file->shm_unmap(true);
        m_file = nullptr;
    } else {
        for (size_t i = 0; i < m_num_groups; ++i) {
            Alloc::free(const_cast<char *>(m_groups[i]));
        }
    }
    Alloc::free(m_groups);
    m_groups = nullptr;
}

// Merge 2 sorted lists.
static auto merge_lists(
    const Key *keys,
    Hash *left,
    uint32_t left_size,
    Hash *&right,
    uint32_t &right_size,
    Hash *scratch)
{
    uint32_t L = 0;
    uint32_t r = 0;
    uint32_t i = 0;

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
    uint32_t &size)
{
    struct SubList {
        Hash *ptr;
        uint32_t len;
    };

    static constexpr size_t kMaxBits = 13;

    SubList sublists[kMaxBits] = {};
    uint32_t sub_index = 0;
    uint32_t n_merge;
    Hash *p_merge;

    CALICODB_EXPECT_EQ(kNIndexKeys, 1 << (ARRAY_SIZE(sublists) - 1));
    CALICODB_EXPECT_LE(size, kNIndexKeys);
    CALICODB_EXPECT_GT(size, 0);

    for (uint32_t L = 0; L < size; ++L) {
        p_merge = hashes + L;
        n_merge = 1;

        for (sub_index = 0; L & (1 << sub_index); ++sub_index) {
            auto *sub = &sublists[sub_index];
            CALICODB_EXPECT_LT(sub_index, ARRAY_SIZE(sublists));
            CALICODB_EXPECT_NE(sub->ptr, nullptr);
            CALICODB_EXPECT_LE(sub->len, 1U << sub_index);
            CALICODB_EXPECT_EQ(sub->ptr, hashes + (L & ~uint32_t((2 << sub_index) - 1)));
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
    for (size_t i = 1; i < size; ++i) {
        CALICODB_EXPECT_GT(keys[hashes[i]], keys[hashes[i - 1]]);
    }
#endif // NDEBUG
}

HashIterator::HashIterator(HashIndex &source)
    : m_source(&source)
{
}

HashIterator::~HashIterator()
{
    Alloc::free(m_state);
}

auto HashIterator::init(uint32_t backfill) -> Status
{
    const auto *const hdr = m_source->m_hdr;

    // This method should not be called on an empty index.
    const auto last_value = hdr->max_frame;
    CALICODB_EXPECT_GT(last_value, 0);

    static_assert(std::is_pod_v<State>);
    static_assert(std::is_pod_v<State::Group>);
    static_assert(alignof(State) == sizeof(void *));
    static_assert(alignof(State) == alignof(State::Group));

    // Allocate internal buffers.
    static constexpr size_t kAlignmentMask = alignof(State) - 1;
    m_num_groups = index_group_number(last_value) + 1;
    const auto state_size =
        sizeof(State) +                             // Includes storage for 1 group ("groups[1]" member).
        (m_num_groups - 1) * sizeof(State::Group) + // Additional groups.
        last_value * sizeof(Hash);                  // Indices to sort.
    m_state = static_cast<State *>(Alloc::malloc(state_size));
    if (m_state == nullptr) {
        return Status::no_memory();
    }
    std::memset(m_state, 0, state_size);

    // Temporary buffer for mergesort. Freed before returning from this routine. Possibly a bit
    // larger than necessary due to platform alignment requirements (see alloc.h).
    UniqueBuffer<Hash> temp;
    if (temp.realloc(last_value < kNIndexKeys ? last_value : kNIndexKeys)) {
        // m_state will be freed in the destructor.
        return Status::no_memory();
    }
    std::memset(temp.ptr(), 0, temp.len() * sizeof(Hash));

    Status s;
    for (uint32_t i = index_group_number(backfill + 1); i < m_num_groups; ++i) {
        s = m_source->map_group(i, true);
        if (!s.is_ok()) {
            break;
        }
        HashGroup group(i, m_source->m_groups[i]);

        uint32_t group_size = kNIndexKeys;
        if (i + 1 == m_num_groups) {
            group_size = last_value - group.base;
        } else if (i == 0) {
            group_size = kNIndexKeys0;
        }

        // Pointer into the special index buffer located right after the group array.
        auto *index_buf = group.base + reinterpret_cast<Hash *>(
                                           m_state->groups + m_num_groups);

        for (size_t j = 0; j < group_size; ++j) {
            index_buf[j] = static_cast<Hash>(j);
        }

        auto *keys = const_cast<Key *>(group.keys);
        mergesort(keys, index_buf, temp.ptr(), group_size);
        m_state->groups[i] = {
            keys,
            index_buf,
            group_size,
            0,
            group.base + 1,
        };
    }
    return s;
}

auto HashIterator::read(Entry &out) -> bool
{
    static constexpr uint32_t kBadResult = 0xFF'FF'FF'FF;
    CALICODB_EXPECT_LT(m_prior, kBadResult);
    auto result = kBadResult;

    auto *last_group = m_state->groups + m_num_groups - 1;
    for (size_t i = 0; i < m_num_groups; ++i) {
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
//     0       4     Magic number (1,559,861,749)
//     4       4     WAL version (1)
//     8       4     DB page size
//     12      4     Checkpoint number
//     16      4     Salt-1
//     20      4     Salt-2
//     24      4     Checksum-1
//     28      4     Checksum-2
//
static constexpr size_t kWalHdrSize = 32;
static constexpr uint32_t kWalMagic = 1'559'861'749;
static constexpr uint32_t kWalVersion = 1;

// WAL frame header layout:
//     Offset  Size  Purpose
//    ---------------------------------------
//     0       4     Page number
//     4       4     DB size in pages (> 0 if commit frame)
//     8       4     Salt-1
//     12      4     Salt-2
//     16      4     Checksum-1
//     20      4     Checksum-2
//
struct WalFrameHdr {
    static constexpr size_t kSize = 24;

    uint32_t pgno = 0;

    // DB header page count after a commit (nonzero for commit frames, 0 for
    // all other frames).
    uint32_t db_size = 0;
};

static auto compute_checksum(const Slice &in, const uint32_t *initial, uint32_t *out)
{
    CALICODB_EXPECT_NE(out, nullptr);
    CALICODB_EXPECT_EQ(std::uintptr_t(in.data()) & 3, 0);
    CALICODB_EXPECT_LE(in.size(), 65'536);
    CALICODB_EXPECT_EQ(in.size() & 7, 0);
    CALICODB_EXPECT_GT(in.size(), 0);

    uint32_t s1 = 0;
    uint32_t s2 = 0;
    if (initial) {
        s1 = initial[0];
        s2 = initial[1];
    }

    const auto *ptr = reinterpret_cast<const uint32_t *>(in.data());
    const auto *end = ptr + in.size() / sizeof *ptr;

    do {
        s1 += *ptr++ + s2;
        s2 += *ptr++ + s1;
    } while (ptr < end);

    out[0] = s1;
    out[1] = s2;
}

class WalImpl : public Wal
{
public:
    explicit WalImpl(const Parameters &param, UserPtr<File> wal_file);
    ~WalImpl() override;

    auto read(Id page_id, char *&page) -> Status override;
    auto write(PageRef *dirty, size_t db_size) -> Status override;
    auto checkpoint(bool reset) -> Status override;

    auto rollback(const Undo &undo, void *object) -> void override
    {
        CALICODB_EXPECT_TRUE(m_writer_lock);
        const auto max_frame = m_hdr.max_frame;
        // Cast away volatile qualifier.
        m_hdr = *const_cast<const HashIndexHdr *>(m_index.header());
        for (auto frame = m_hdr.max_frame + 1; frame <= max_frame; ++frame) {
            undo(object, Id(m_index.fetch(frame)));
        }
        if (max_frame != m_hdr.max_frame) {
            m_index.cleanup();
        }
    }

    auto close() -> Status override
    {
        // This will not block. This connection has an exclusive lock on the database file,
        // so no other connections are active right now.
        auto s = checkpoint(true);
        if (s.is_ok()) {
            s = m_env->remove_file(m_wal_name);
            if (!s.is_ok()) {
                log(m_log, R"(failed to unlink WAL at "%s": %s (%s))",
                    m_wal_name, s.type_name(), s.message());
            }
        }
        return s;
    }

    auto start_reader(bool &changed) -> Status override
    {
        CALICODB_EXPECT_FALSE(m_ckpt_lock);

        Status s;
        unsigned tries = 0;
        do {
            // Actions taken by other connections that cause readers to wait will mostly
            // complete in a bounded amount of time. The exception is when a checkpointer
            // thread wants to restart the WAL, and will block until all readers are done.
            // Otherwise, try_reader() should indicate that we can retry.
            s = try_reader(false, tries++, changed);
        } while (s.is_retry());
        return s;
    }
    auto finish_reader() -> void override
    {
        finish_writer();
        if (m_reader_lock >= 0) {
            unlock_shared(READ_LOCK(m_reader_lock));
            m_reader_lock = -1;
        }
    }

    auto start_writer() -> Status override
    {
        if (m_writer_lock) {
            return Status::ok();
        }
        CALICODB_EXPECT_GE(m_reader_lock, 0);
        CALICODB_EXPECT_EQ(m_redo_cksum, 0);

        CALICODB_TRY(lock_exclusive(kWriteLock, 1));
        m_writer_lock = true;

        // We have the writer lock, so no other connection will write the index header while
        // std::memcmp() is running here.
        if (0 != std::memcmp(&m_hdr, ConstStablePtr(m_index.header()), sizeof(HashIndexHdr))) {
            // Another connection has written since the read transaction was started. This
            // is not allowed (this connection now has an outdated snapshot, meaning the
            // local "max frame" value is no longer correct).
            unlock_exclusive(kWriteLock, 1);
            m_writer_lock = false;
            return Status::busy("stale snapshot");
        }
        return Status::ok();
    }
    auto finish_writer() -> void override
    {
        if (m_writer_lock) {
            unlock_exclusive(kWriteLock, 1);
            m_writer_lock = false;
            m_redo_cksum = 0;
        }
    }

    [[nodiscard]] auto last_frame_count() const -> size_t override
    {
        // NOTE: This value is used to determine if an automatic checkpoint should be run. It doesn't need to
        //       be totally up-to-date. It must, however, be less than or equal to the actual maximum frame.
        return m_hdr.max_frame;
    }

    [[nodiscard]] auto db_size() const -> uint32_t override
    {
        CALICODB_EXPECT_GE(m_reader_lock, 0);
        return m_hdr.page_count;
    }

private:
    auto lock_shared(size_t r) -> Status
    {
        if (m_lock_mode == Options::kLockExclusive) {
            return Status::ok();
        }
        return m_db->shm_lock(r, 1, kShmLock | kShmReader);
    }
    auto unlock_shared(size_t r) -> void
    {
        if (m_lock_mode != Options::kLockExclusive) {
            (void)m_db->shm_lock(r, 1, kShmUnlock | kShmReader);
        }
    }

    auto lock_exclusive(size_t r, size_t n) -> Status
    {
        if (m_lock_mode == Options::kLockExclusive) {
            return Status::ok();
        }
        return m_db->shm_lock(r, n, kShmLock | kShmWriter);
    }
    auto unlock_exclusive(size_t r, size_t n) -> void
    {
        if (m_lock_mode != Options::kLockExclusive) {
            (void)m_db->shm_lock(r, n, kShmUnlock | kShmWriter);
        }
    }

    [[nodiscard]] auto get_ckpt_info() -> volatile CkptInfo *
    {
        CALICODB_EXPECT_NE(m_index.groups(), nullptr);
        CALICODB_EXPECT_NE(m_index.groups()[0], nullptr);
        return reinterpret_cast<volatile CkptInfo *>(
            &m_index.groups()[0][sizeof(HashIndexHdr) * 2]);
    }
    [[nodiscard]] auto try_index_header(bool &changed) -> bool
    {
        HashIndexHdr h1 = {};
        HashIndexHdr h2 = {};
        changed = false;

        const volatile auto *hdr = m_index.header();
        read_hdr(&hdr[0], &h1);
        m_db->shm_barrier();
        read_hdr(&hdr[1], &h2);

        if (0 != std::memcmp(&h1, &h2, sizeof(h1))) {
            return false;
        }
        if (!h1.is_init) {
            return false;
        }
        uint32_t cksum[2];
        const Slice target(ConstStablePtr(&h1), sizeof(h1) - sizeof(h1.cksum));
        compute_checksum(target, nullptr, cksum);

        if (cksum[0] != h1.cksum[0] ||
            cksum[1] != h1.cksum[1]) {
            return false;
        }
        if (0 != std::memcmp(&m_hdr, &h1, sizeof(m_hdr))) {
            changed = true;
            m_hdr = h1;
        }
        return true;
    }
    auto read_index_header(bool &changed) -> Status
    {
        auto s = m_index.map_group(0, m_writer_lock);
        if (!s.is_ok()) {
            return s;
        }
        CALICODB_EXPECT_GT(m_index.m_num_groups, 0);
        CALICODB_EXPECT_TRUE(m_index.m_groups[0] || !m_writer_lock);

        auto success = false;
        if (m_index.m_groups[0]) {
            success = try_index_header(changed);
        }
        if (!success) {
            const auto prev_lock = m_writer_lock;
            if (prev_lock || (s = lock_exclusive(kWriteLock, 1)).is_ok()) {
                m_writer_lock = true;
                if ((s = m_index.map_group(0, true)).is_ok()) {
                    success = try_index_header(changed);
                    if (!success) {
                        s = recover_index();
                        changed = true;
                    }
                }
                if (prev_lock == 0) {
                    unlock_exclusive(kWriteLock, 1);
                    m_writer_lock = false;
                }
            }
        }
        if (success && m_hdr.version != kWalVersion) {
            return Status::not_supported("WAL version mismatch");
        }
        return s;
    }
    auto write_index_header() -> void
    {
        m_hdr.is_init = true;
        m_hdr.version = kWalVersion;

        const Slice target(StablePtr(&m_hdr), offsetof(HashIndexHdr, cksum));
        compute_checksum(target, nullptr, m_hdr.cksum);

        volatile auto *hdr = m_index.header();
        write_hdr(&m_hdr, &hdr[1]);
        m_db->shm_barrier();
        write_hdr(&m_hdr, &hdr[0]);
    }

    auto restart_header(uint32_t salt_1) -> void
    {
        ++m_ckpt_number;
        m_hdr.max_frame = 0;
        auto *salt = StablePtr(m_hdr.salt);
        put_u32(salt, get_u32(salt) + 1);
        std::memcpy(salt + sizeof(uint32_t), &salt_1, sizeof(salt_1));
        write_index_header();

        volatile auto *info = get_ckpt_info();
        CALICODB_EXPECT_EQ(info->readmark[0], 0);
        ATOMIC_STORE(&info->backfill, 0);
        ATOMIC_STORE(&info->backfill_attempted, 0);
        ATOMIC_STORE(&info->readmark[1], 0);
        for (size_t i = 2; i < kReaderCount; ++i) {
            ATOMIC_STORE(&info->readmark[i], kReadmarkNotUsed);
        }
    }
    auto restart_log() -> Status
    {
        Status s;
        if (m_reader_lock == 0) {
            // If this connection has READ_LOCK(0) locked, then no other connection is
            // currently in the process of running a checkpoint (`backfill` field will
            // not change).
            const volatile auto *info = get_ckpt_info();
            CALICODB_EXPECT_EQ(info->backfill, m_hdr.max_frame);
            if (info->backfill) {
                const auto salt_1 = m_env->rand();
                s = lock_exclusive(READ_LOCK(1), kReaderCount - 1);
                if (s.is_ok()) {
                    restart_header(salt_1);
                    unlock_exclusive(READ_LOCK(1), kReaderCount - 1);
                } else if (!s.is_busy()) {
                    return s;
                }
            }
            unlock_shared(READ_LOCK(0));
            m_reader_lock = -1;
            unsigned tries = 0;
            do {
                bool unused;
                s = try_reader(true, tries, unused);
            } while (s.is_retry());
        }
        return s;
    }

    auto try_reader(bool use_wal, unsigned tries, bool &changed) -> Status
    {
        CALICODB_EXPECT_LT(m_reader_lock, 0);
        if (tries > 5) {
            if (tries > 100) {
                m_lock_error = true;
                return Status::io_error("protocol error");
            }
            unsigned delay = 1;
            if (tries >= 10) {
                delay = (tries - 9) * (tries - 9) * 39;
            }
            m_env->sleep(delay);
        }

        Status s;
        if (!use_wal) {
            s = read_index_header(changed);
            if (s.is_busy()) {
                if (m_index.m_groups[0] == nullptr) {
                    // First shm region has not been mapped.
                    s = Status::retry();
                } else if ((s = lock_shared(kRecoveryLock)).is_ok()) {
                    unlock_shared(kRecoveryLock);
                    s = Status::retry();
                }
            }
            if (!s.is_ok()) {
                return s;
            }
        }

        CALICODB_EXPECT_NE(m_index.groups(), nullptr);
        CALICODB_EXPECT_NE(m_index.groups()[0], nullptr);

        volatile auto *info = get_ckpt_info();
        if (!use_wal && ATOMIC_LOAD(&info->backfill) == m_hdr.max_frame) {
            // The whole WAL has been written back to the database file, or the WAL is just empty.
            // Take info->readmark[0], which always has a value of 0 (the reader will see the WAL
            // as empty, causing it to read from the database file instead).
            s = lock_shared(READ_LOCK(0));
            m_db->shm_barrier();
            if (s.is_ok()) {
                if (compare_hdr(m_index.header(), &m_hdr)) {
                    // The WAL has been written since the index header was last read. Indicate
                    // that the user should try again.
                    unlock_shared(READ_LOCK(0));
                    return Status::retry();
                }
                m_reader_lock = 0;
                return Status::ok();
            } else if (!s.is_busy()) {
                return s;
            }
        }

        size_t max_readmark = 0;
        size_t max_index = 0;
        uint32_t max_frame = m_hdr.max_frame;

        // Attempt to find a readmark that this reader can use to read the most-recently-committed WAL
        // frames.
        for (size_t i = 1; i < kReaderCount; i++) {
            const auto mark = ATOMIC_LOAD(&info->readmark[i]);
            if (max_readmark <= mark && mark <= max_frame) {
                CALICODB_EXPECT_NE(mark, kReadmarkNotUsed);
                max_readmark = mark;
                max_index = i;
            }
        }
        if (max_readmark < max_frame || max_index == 0) {
            // Attempt to increase a readmark to include the most-recent commit.
            for (size_t i = 1; i < kReaderCount; ++i) {
                s = lock_exclusive(READ_LOCK(i), 1);
                if (s.is_ok()) {
                    ATOMIC_STORE(&info->readmark[i], max_frame);
                    max_readmark = max_frame;
                    max_index = i;
                    unlock_exclusive(READ_LOCK(i), 1);
                    break;
                } else if (!s.is_busy()) {
                    return s;
                }
            }
        }
        if (max_index == 0) {
            return Status::retry();
        }
        // Will return a busy status if another connection is resetting the WAL. After this call,
        // this connection will have a lock on a nonzero readmark. This connection will read pages
        // from the WAL between the current backfill count and integer stored in readmark number
        // `max_index`, inclusive.
        s = lock_shared(READ_LOCK(max_index));
        if (!s.is_ok()) {
            return s.is_busy() ? Status::retry() : s;
        }
        m_min_frame = ATOMIC_LOAD(&info->backfill) + 1;
        m_db->shm_barrier();

        // Make sure there wasn't a writer between the readmark increase and when we took the
        // shared lock.
        const auto changed_unexpectedly =
            ATOMIC_LOAD(&info->readmark[max_index]) != max_readmark ||
            compare_hdr(m_index.header(), &m_hdr);
        if (changed_unexpectedly) {
            unlock_shared(READ_LOCK(max_index));
            return Status::retry();
        } else {
            // It's possible that this connection wasn't able to find (or increase a readmark
            // to equal) the `max_frame` value read from the index header. This is fine, it
            // just may restrict how many frames can be backfilled while this reader is live.
            CALICODB_EXPECT_LE(max_readmark, m_hdr.max_frame);
            m_reader_lock = static_cast<int>(max_index);
        }
        return s;
    }

    [[nodiscard]] static auto frame_offset(uint32_t frame) -> size_t
    {
        CALICODB_EXPECT_GT(frame, 0);
        return kWalHdrSize + (frame - 1) * kFrameSize;
    }

    auto transfer_contents(bool reset) -> Status;
    auto rewrite_checksums(uint32_t end) -> Status;
    auto recover_index() -> Status;
    [[nodiscard]] auto decode_frame(const char *frame, WalFrameHdr &out) -> bool;
    auto encode_frame(const WalFrameHdr &hdr, const char *page, char *out) -> void;

    HashIndexHdr m_hdr = {};
    HashIndex m_index;

    const char *const m_wal_name;
    const Options::SyncMode m_sync_mode;
    const Options::LockMode m_lock_mode;

    // Storage for a single WAL frame.
    static constexpr size_t kFrameSize = WalFrameHdr::kSize + kPageSize;
    char m_frame[kFrameSize];

    uint32_t m_redo_cksum = 0;
    uint32_t m_ckpt_number = 0;

    UserPtr<File> m_wal;

    Env *const m_env = nullptr;
    File *const m_db = nullptr;
    Logger *const m_log = nullptr;
    Stat *const m_stat = nullptr;
    BusyHandler *const m_busy = nullptr;

    uint32_t m_min_frame = 0;

    int m_reader_lock = -1;
    bool m_writer_lock = false;
    bool m_ckpt_lock = false;
    bool m_lock_error = false;

    //    bool m_readonly = false; // TODO: readonly connections
    //    bool m_shm_unreliable = false; // TODO: specific situation where this connection is readonly
    //                                            and no writer is connected that can keep the shm file up-to-date
};

auto Wal::open(const Parameters &param, Wal *&wal_out) -> Status
{
    UserPtr<File> wal_file;
    auto s = param.env->new_file(param.filename, Env::kCreate, wal_file.ref());
    if (s.is_ok()) {
        wal_out = Alloc::new_object<WalImpl>(param, std::move(wal_file));
        if (wal_out == nullptr) {
            s = Status::no_memory();
        }
    }
    return s;
}

Wal::~Wal() = default;

WalImpl::WalImpl(const Parameters &param, UserPtr<File> wal_file)
    : m_index(m_hdr, param.lock_mode == Options::kLockNormal ? param.db_file : nullptr),
      m_wal_name(param.filename),
      m_sync_mode(param.sync_mode),
      m_lock_mode(param.lock_mode),
      m_wal(std::move(wal_file)),
      m_env(param.env),
      m_db(param.db_file),
      m_log(param.info_log),
      m_stat(param.stat),
      m_busy(param.busy)
{
}

WalImpl::~WalImpl()
{
    m_index.close();
}

auto WalImpl::rewrite_checksums(uint32_t end) -> Status
{
    CALICODB_EXPECT_GT(m_redo_cksum, 0);

    // Find the offset of the previous checksum in the WAL file. If we are starting at
    // the first frame, get the previous checksum from the WAL header.
    size_t cksum_offset = 24;
    if (m_redo_cksum > 1) {
        cksum_offset = frame_offset(m_redo_cksum - 1) + 16;
    }

    char cksum_buffer[2 * sizeof(uint32_t)];
    CALICODB_TRY(m_wal->read_exact(
        cksum_offset,
        sizeof(cksum_buffer),
        cksum_buffer));
    m_stat->counters[Stat::kReadWal] += kFrameSize;

    m_hdr.frame_cksum[0] = get_u32(&m_frame[0]);
    m_hdr.frame_cksum[1] = get_u32(&m_frame[sizeof(uint32_t)]);

    auto redo = m_redo_cksum;
    m_redo_cksum = 0;

    for (; redo < end; ++redo) {
        const auto offset = frame_offset(redo);
        CALICODB_TRY(m_wal->read_exact(offset, kFrameSize, m_frame));
        m_stat->counters[Stat::kReadWal] += kFrameSize;

        WalFrameHdr hdr;
        hdr.pgno = get_u32(&m_frame[0]);
        hdr.db_size = get_u32(&m_frame[4]);
        encode_frame(hdr, &m_frame[WalFrameHdr::kSize], m_frame);

        CALICODB_TRY(m_wal->write(offset, Slice(m_frame, WalFrameHdr::kSize)));
        m_stat->counters[Stat::kWriteWal] += WalFrameHdr::kSize;
    }
    return Status::ok();
}

auto WalImpl::recover_index() -> Status
{
    CALICODB_EXPECT_EQ(kNotWriteLock, kWriteLock + 1);
    CALICODB_EXPECT_EQ(kCheckpointLock, kNotWriteLock);
    CALICODB_EXPECT_TRUE(m_writer_lock);
    m_hdr = {};

    // TODO: This code is not being called from checkpoint anymore. It would be necessary if
    //       we wanted to support a "truncate" checkpoint mode like SQLite. If not, this code
    //       can be simplified to just taking the recovery lock.
    // Lock the recover "Rcvr" lock. Lock the checkpoint ("Ckpt") lock as well, if this
    // code isn't being called from the checkpoint routine. In that case, the checkpoint
    // lock is already held.
    const auto lock = kNotWriteLock + m_ckpt_lock;
    CALICODB_TRY(lock_exclusive(lock, READ_LOCK(0) - lock));

    uint32_t frame_cksum[2] = {};

    const auto cleanup = [&frame_cksum, lock, this](auto s) {
        if (s.is_ok()) {
            m_hdr.frame_cksum[0] = frame_cksum[0];
            m_hdr.frame_cksum[1] = frame_cksum[1];
            write_index_header();
            // NOTE: This code can run while readers are trying to connect (`start_reader()`).
            //
            volatile auto *info = get_ckpt_info();
            // TODO: It seems that this store races with connections that are attempting to
            //       start reading. Making it atomic for now. When readers read the backfill
            //       count to see if they can get read lock 0, they are not under any lock...
            // info->backfill = 0;
            ATOMIC_STORE(&info->backfill, 0);
            info->backfill_attempted = m_hdr.max_frame;
            info->readmark[0] = 0;
            for (size_t i = 1; i < kReaderCount; ++i) {
                s = lock_exclusive(READ_LOCK(i), 1);
                if (s.is_ok()) {
                    if (i == 1 && m_hdr.max_frame) {
                        info->readmark[i] = m_hdr.max_frame;
                    } else {
                        info->readmark[i] = kReadmarkNotUsed;
                    }
                    unlock_exclusive(READ_LOCK(i), 1);
                } else if (!s.is_busy()) {
                    break;
                }
            }
            log(m_log, "recovered %u WAL frames", m_hdr.max_frame);
        }
        unlock_exclusive(lock, READ_LOCK(0) - lock);
        return s;
    };

    size_t file_size;
    CALICODB_TRY(m_env->file_size(m_wal_name, file_size));
    if (file_size > kWalHdrSize) {
        char header[kWalHdrSize];
        CALICODB_TRY(m_wal->read_exact(0, sizeof(header), header));
        m_stat->counters[Stat::kReadWal] += sizeof(header);

        const auto magic = get_u32(&header[0]);
        if (magic != kWalMagic) {
            return cleanup(Status::corruption("WAL header is corrupted"));
        }
        m_ckpt_number = get_u32(&header[12]);
        std::memcpy(m_hdr.salt, &header[16], sizeof(m_hdr.salt));

        compute_checksum(
            Slice(header, kWalHdrSize - 8),
            nullptr,
            m_hdr.frame_cksum);
        if (m_hdr.frame_cksum[0] == get_u32(&header[24]) &&
            m_hdr.frame_cksum[1] == get_u32(&header[28])) {
            // Checksums match, meaning there is valid WAL data. Reconstruct the hash index from it
            // if the version number is understood.
            const auto version = get_u32(&header[4]);
            if (version != kWalVersion) {
                //                std::string message;
                //                append_fmt_string(message, "found WAL version %u but expected %u", version, kWalVersion);
                return cleanup(Status::invalid_argument("message"));
            }

            const auto last_frame = static_cast<uint32_t>((file_size - kWalHdrSize) / kFrameSize);
            for (uint32_t n_group = 0; n_group <= index_group_number(last_frame); ++n_group) {
                const auto last = std::min(last_frame, kNIndexKeys0 + n_group * kNIndexKeys);
                const auto first = 1 + (n_group == 0 ? 0 : kNIndexKeys0 + (n_group - 1) * kNIndexKeys);
                for (auto n_frame = first; n_frame <= last; ++n_frame) {
                    const auto offset = frame_offset(n_frame);
                    CALICODB_TRY(m_wal->read_exact(offset, kFrameSize, m_frame));
                    m_stat->counters[Stat::kReadWal] += kFrameSize;

                    WalFrameHdr hdr;
                    if (!decode_frame(m_frame, hdr)) {
                        break;
                    }
                    CALICODB_TRY(m_index.assign(hdr.pgno, n_frame));
                    if (hdr.db_size) {
                        // Found a commit frame.
                        m_hdr.max_frame = n_frame;
                        m_hdr.page_count = hdr.db_size;
                        frame_cksum[0] = m_hdr.cksum[0];
                        frame_cksum[1] = m_hdr.cksum[1];
                    }
                }
            }
        }
    }
    return cleanup(Status::ok());
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
        compute_checksum(Slice(page, kPageSize), cksum, cksum);
        put_u32(&out[16], cksum[0]);
        put_u32(&out[20], cksum[1]);
    }
    std::memcpy(&out[24], page, kPageSize);
}

auto WalImpl::decode_frame(const char *frame, WalFrameHdr &out) -> bool
{
    static constexpr size_t kDataFields = sizeof(uint32_t) * 2;
    if (0 != std::memcmp(m_hdr.salt, &frame[kDataFields], 8)) {
        return false;
    }
    const auto pgno = get_u32(&frame[0]);
    if (pgno == 0) {
        return false;
    }
    auto *cksum = m_hdr.frame_cksum;
    compute_checksum(Slice(frame, kDataFields), cksum, cksum);
    compute_checksum(Slice(frame + WalFrameHdr::kSize, kPageSize), cksum, cksum);
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
    CALICODB_EXPECT_GE(m_reader_lock, 0);
    auto *ptr = page;
    page = nullptr;
    if (m_reader_lock && m_hdr.max_frame) {
        uint32_t frame;
        CALICODB_TRY(m_index.lookup(page_id.value, m_min_frame, frame));

        if (frame) {
            CALICODB_TRY(m_wal->read_exact(
                frame_offset(frame) + WalFrameHdr::kSize,
                kPageSize,
                m_frame));
            m_stat->counters[Stat::kReadWal] += kPageSize;

            std::memcpy(ptr, m_frame, kPageSize);
            page = ptr;
        }
    }
    return Status::ok();
}

auto WalImpl::write(PageRef *first_ref, size_t db_size) -> Status
{
    CALICODB_EXPECT_TRUE(m_writer_lock);
    CALICODB_EXPECT_NE(first_ref, nullptr);

    const auto is_commit = db_size > 0;
    volatile auto *live = m_index.header();
    auto *dirty = &first_ref->dirty_hdr;
    uint32_t first_frame = 0;

    // Check if the WAL's copy of the index header differs from what is on the first index page.
    // If it is different, then the WAL has been written since the last commit, with the first
    // record located at frame number "first_frame".
    if (0 != std::memcmp(&m_hdr, ConstStablePtr(live), sizeof(m_hdr))) {
        first_frame = live->max_frame + 1;
    }
    CALICODB_TRY(restart_log());

    if (m_hdr.max_frame == 0) {
        // This is the first frame written to the WAL. Write the WAL header.
        char header[kWalHdrSize];
        uint32_t cksum[2];

        put_u32(&header[0], kWalMagic);
        put_u32(&header[4], kWalVersion);
        put_u32(&header[8], 0); // Reserved field
        put_u32(&header[12], m_ckpt_number);
        if (m_ckpt_number == 0) {
            m_hdr.salt[0] = m_env->rand();
            m_hdr.salt[1] = m_env->rand();
        }
        std::memcpy(&header[16], m_hdr.salt, sizeof(m_hdr.salt));
        // Compute the checksum of the header bytes up to the checksum field.
        const Slice target(header, sizeof(header) - sizeof(cksum));
        compute_checksum(target, nullptr, cksum);
        put_u32(&header[24], cksum[0]);
        put_u32(&header[28], cksum[1]);

        m_hdr.frame_cksum[0] = cksum[0];
        m_hdr.frame_cksum[1] = cksum[1];

        CALICODB_TRY(m_wal->write(0, Slice(header, sizeof(header))));
        m_stat->counters[Stat::kWriteWal] += sizeof(header);

        if (m_sync_mode != Options::kSyncOff) {
            ++m_stat->counters[Stat::kSyncWal];
            CALICODB_TRY(m_wal->sync());
        }
    }

    // Write each dirty page to the WAL.
    auto next_frame = m_hdr.max_frame + 1;
    auto offset = frame_offset(next_frame);
    for (auto *p = dirty; p; p = p->dirty) {
        auto *ref = p->get_page_ref();
        uint32_t frame;

        // Condition ensures that if this set of pages completes a transaction, then
        // the last frame will always be appended, even if another copy of the page
        // exists in the WAL for this transaction. This frame needs to have its
        // "db_size" field set to mark that it is a commit frame.
        if (first_frame && (p->dirty || !is_commit)) {
            // Check to see if the page has been written to the WAL already by the
            // current transaction. If so, overwrite it and indicate that checksums
            // need to be recomputed from here on commit.
            CALICODB_TRY(m_index.lookup(ref->page_id.value, first_frame, frame));
            if (frame) {
                if (m_redo_cksum == 0 || frame < m_redo_cksum) {
                    m_redo_cksum = frame;
                }
                CALICODB_TRY(m_wal->write(
                    frame_offset(frame) + WalFrameHdr::kSize,
                    Slice(ref->data, kPageSize)));
                m_stat->counters[Stat::kWriteWal] += kPageSize;
                continue;
            }
        }
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it. Note that we don't clear the dirty flag on this path.
        // It will be cleared below when the page ID-to-frame number mapping is
        // created for the new frame.
        WalFrameHdr header;
        header.pgno = ref->page_id.value;
        header.db_size = p->dirty == nullptr ? static_cast<uint32_t>(db_size) : 0;
        encode_frame(header, ref->data, m_frame);
        CALICODB_TRY(m_wal->write(offset, Slice(m_frame, kFrameSize)));
        m_stat->counters[Stat::kWriteWal] += kFrameSize;
        ref->set_flag(PageRef::kExtra);

        CALICODB_EXPECT_EQ(offset, frame_offset(next_frame));
        offset += kFrameSize;
        ++next_frame;
    }

    if (is_commit && m_redo_cksum) {
        CALICODB_TRY(rewrite_checksums(next_frame));
    }

    Status s;
    next_frame = m_hdr.max_frame + 1;
    for (auto *p = dirty; s.is_ok() && p; p = p->dirty) {
        auto *ref = p->get_page_ref();
        if (ref->get_flag(PageRef::kExtra)) {
            s = m_index.assign(ref->page_id.value, next_frame++);
            ref->clear_flag(PageRef::kExtra);
        }
    }
    if (s.is_ok()) {
        m_hdr.max_frame = next_frame - 1;
        if (is_commit) {
            // If this is a commit, then at least 1 frame (the commit frame) must be written. The
            // pager has logic to make sure of this (the root page is forcibly written if no pages
            // are dirty).
            CALICODB_EXPECT_TRUE(dirty);
            if (m_sync_mode == Options::kSyncFull) {
                ++m_stat->counters[Stat::kSyncWal];
                CALICODB_TRY(m_wal->sync());
            }
            m_hdr.page_count = static_cast<uint32_t>(db_size);
            ++m_hdr.change;
            write_index_header();
        }
    }
    return s;
}

auto WalImpl::checkpoint(bool reset) -> Status
{
    CALICODB_EXPECT_FALSE(m_ckpt_lock);
    CALICODB_EXPECT_FALSE(m_writer_lock);

    // Exclude other connections from running a checkpoint. If the `reset` flag is set,
    // also exclude writers.
    auto s = lock_exclusive(kCheckpointLock, 1);
    if (s.is_ok()) {
        m_ckpt_lock = true;
        if (reset) {
            s = busy_wait(m_busy, [this] {
                return lock_exclusive(kWriteLock, 1);
            });
            m_writer_lock = s.is_ok();
        }
    }

    auto changed = false;
    if (s.is_ok()) {
        s = read_index_header(changed);
    }
    if (s.is_ok() && m_hdr.max_frame) {
        s = transfer_contents(reset);
    }
    if (changed) {
        m_hdr = {};
    }
    finish_writer();
    if (m_ckpt_lock) {
        unlock_exclusive(kCheckpointLock, 1);
        m_ckpt_lock = false;
    }
    return s;
}

// Write as much of the WAL back to the database file as possible
// This method is run under an exclusive checkpoint lock, and possibly an exclusive writer
// lock. Writes to the "backfill count" variable stored in the checkpoint header must be
// atomic here, but reads need not be. This is the only connection allowed to change the
// backfill count right now. Note that the backfill count is also set during both WAL reset
// and index recovery, however, connections performing either of these actions are excluded
// by shm locks (other checkpointers by the checkpoint lock, and connections seeking to
// restart the log by the writer lock).
auto WalImpl::transfer_contents(bool reset) -> Status
{
    CALICODB_EXPECT_TRUE(m_ckpt_lock);
    CALICODB_EXPECT_TRUE(!reset || m_writer_lock);
    const auto sync_on_ckpt = m_sync_mode != Options::kSyncOff;

    Status s;
    uint32_t start_frame = 0; // Original "backfill" for logging
    volatile auto *info = get_ckpt_info();
    auto max_safe_frame = m_hdr.max_frame;
    auto max_pgno = m_hdr.page_count;
    // This code can run while another connection is resetting the WAL. That code will set
    // the backfill count to 0, however, it will also hold exclusive locks on read locks
    // 1 to N until it has reset the readmarks. This connection will block when trying to
    // lock the read locks below, and will see that it shouldn't write anything back.
    if (ATOMIC_LOAD(&info->backfill) < max_safe_frame) {
        // Determine the range of frames that are available to write back to the database
        // file. This range starts at the frame after the last frame backfilled by another
        // checkpointer and ends at either the last WAL frame this connection knows about,
        // or the most-recent frame still needed by a reader, whichever is smaller.
        for (size_t i = 1; i < kReaderCount; ++i) {
            const auto y = ATOMIC_LOAD(&info->readmark[i]);
            if (y < max_safe_frame) {
                CALICODB_EXPECT_LE(y, m_hdr.max_frame);
                s = busy_wait(m_busy, [this, i] {
                    return lock_exclusive(READ_LOCK(i), 1);
                });
                if (s.is_ok()) {
                    const uint32_t mark = i == 1 ? max_safe_frame : kReadmarkNotUsed;
                    ATOMIC_STORE(&info->readmark[i], mark);
                    unlock_exclusive(READ_LOCK(i), 1);
                } else if (s.is_busy()) {
                    max_safe_frame = y;
                } else {
                    return s;
                }
            }
        }

        if (ATOMIC_LOAD(&info->backfill) < max_safe_frame) {
            HashIterator itr(m_index);
            s = itr.init(info->backfill);
            if (s.is_ok()) {
                s = busy_wait(m_busy, [this] {
                    // Lock reader lock 0. This prevents other connections from ignoring the WAL and
                    // reading all pages from the database file. New readers should find a readmark,
                    // so they know which pages to get from the WAL, since this connection is about
                    // to overwrite some pages in the database file (readers would otherwise risk
                    // reading pages that are in the process of being written).
                    return lock_exclusive(READ_LOCK(0), 1);
                });
            }
            if (!s.is_ok()) {
                return s;
            }

            start_frame = info->backfill;
            info->backfill_attempted = max_safe_frame;
            if (sync_on_ckpt) {
                ++m_stat->counters[Stat::kSyncWal];
                s = m_wal->sync();
            }

            while (s.is_ok()) {
                HashIterator::Entry entry;
                if (!itr.read(entry)) {
                    break;
                }
                if (entry.value <= start_frame ||
                    entry.value > max_safe_frame ||
                    entry.key > max_pgno) {
                    continue;
                }
                m_stat->counters[Stat::kReadWal] += kPageSize;
                s = m_wal->read_exact(
                    frame_offset(entry.value) + WalFrameHdr::kSize,
                    kPageSize,
                    m_frame);
                if (s.is_ok()) {
                    m_stat->counters[Stat::kWriteDB] += kPageSize;
                    s = m_db->write(
                        (entry.key - 1) * kPageSize,
                        Slice(m_frame, kPageSize));
                }
            }
            if (s.is_ok()) {
                if (max_safe_frame == m_hdr.max_frame) {
                    s = m_db->resize(m_hdr.page_count * kPageSize);
                    if (s.is_ok() && sync_on_ckpt) {
                        ++m_stat->counters[Stat::kSyncDB];
                        s = m_db->sync();
                    }
                }
                if (s.is_ok()) {
                    ATOMIC_STORE(&info->backfill, max_safe_frame);
                }
            }
            unlock_exclusive(READ_LOCK(0), 1);
        }
    }
    if (s.is_ok() && reset) {
        CALICODB_EXPECT_TRUE(m_writer_lock);
        if (info->backfill < m_hdr.max_frame) {
            // Some other connection got in the way.
            s = Status::retry();
        } else {
            const auto salt_1 = m_env->rand();
            // Wait on other connections that are still reading from the WAL. This is
            // what SQLite does for `SQLITE_CHECKPOINT_RESTART`. New connections will
            // take readmark 0 and read directly from the database file, and the next
            // writer will reset the log.
            s = busy_wait(m_busy, [this] {
                return lock_exclusive(READ_LOCK(1), kReaderCount - 1);
            });
            if (s.is_ok()) {
                restart_header(salt_1);
                unlock_exclusive(READ_LOCK(1), kReaderCount - 1);
            }
        }
    }
    log(m_log, "checkpointed WAL frames [%u, %u] out of %u",
        start_frame, ATOMIC_LOAD(&info->backfill), m_hdr.max_frame);
    return s;
}

} // namespace calicodb