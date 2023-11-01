// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/wal.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "encoding.h"
#include "logging.h"
#include "mem.h"
#include "page.h"
#include "status_internal.h"
#include "unique_ptr.h"
#include "wal_internal.h"

namespace calicodb
{

namespace
{

// Compiler intrinsics for atomic loads and stores
#define ATOMIC_LOAD(p) __atomic_load_n(p, __ATOMIC_RELAXED)
#define ATOMIC_STORE(p, v) __atomic_store_n(p, v, __ATOMIC_RELAXED)

#if defined(__clang__) && !defined(NO_TSAN)
#define NO_TSAN __attribute((no_sanitize_thread))
#else
#define NO_TSAN
#endif

struct HashIndexHdr {
    uint32_t version;
    uint32_t unused;
    uint32_t change;
    uint16_t is_init;
    uint16_t page_size;
    uint32_t max_frame;
    uint32_t page_count;
    uint32_t frame_cksum[2];
    uint32_t salt[2];
    uint32_t cksum[2];
};

class HashIndex final
{
public:
    friend class HashIterator;

    using Key = uint32_t;
    using Value = uint32_t;

    explicit HashIndex(HashIndexHdr &header, File *file);
    [[nodiscard]] auto fetch(Value value) -> Key;
    auto lookup(Key key, Value lower, Value &out) -> Status;
    auto assign(Key key, Value value) -> Status;
    [[nodiscard]] auto header() -> volatile HashIndexHdr *;
    [[nodiscard]] auto groups() const -> volatile char **;
    void cleanup();
    void close();

private:
    friend class WalImpl;

    auto map_group(size_t group_number, bool extend) -> Status;

    // Storage for hash table groups.
    volatile char **m_groups = nullptr;
    size_t m_num_groups = 0;

    // Address of the hash table header kept in memory. This version of the header corresponds
    // to the current transaction. The one stored in the first table group corresponds to the
    // most-recently-committed transaction.
    HashIndexHdr *const m_hdr;

    File *m_file;
};

// Construct for iterating through the hash index.
class HashIterator final
{
public:
    using Key = HashIndex::Key;
    using Value = HashIndex::Value;

    struct Entry {
        Key key = 0;
        Value value = 0;
    };

    ~HashIterator();

    // Create an iterator over the contents of the provided hash index.
    explicit HashIterator(HashIndex &source);
    auto init(uint32_t backfill = 0) -> Status;

    // Return the next hash entry.
    //
    // This method should return a key that is greater than the last key returned by this
    // method, along with the most-recently-set value.
    [[nodiscard]] auto read(Entry &out) -> bool;

private:
    struct State {
        struct Group {
            Key *keys;
            uint16_t *index;
            uint32_t size;
            uint32_t next;
            uint32_t base;
        } groups[1];
    };

    HashIndex *const m_source;
    State *m_state = nullptr;
    size_t m_num_groups = 0;
    Key m_prior = 0;
};

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
template <class Src, class Dst>
void read_hdr(const volatile Src *src, Dst *dst)
{
    std::memcpy(dst, ConstStablePtr(src), sizeof(HashIndexHdr));
}

template <class Src, class Dst>
void write_hdr(const Src *src, volatile Dst *dst)
{
    std::memcpy(StablePtr(dst), src, sizeof(HashIndexHdr));
}

template <class Shared, class Local>
auto compare_hdr(const volatile Shared *shared, const Local *local) -> int
{
    return std::memcmp(ConstStablePtr(shared), local, sizeof(HashIndexHdr));
}

constexpr uint32_t kReadmarkNotUsed = 0xFF'FF'FF'FF;
constexpr uint32_t kWriteLock = 0;
constexpr uint32_t kNotWriteLock = 1;
constexpr uint32_t kCheckpointLock = 1;
constexpr uint32_t kRecoveryLock = 2;
constexpr uint32_t kReaderCount = File::kShmLockCount - 3;
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

    // Reserved for future expansion. reserved1 corresponds to the "backfill
    // attempted" field in SQLite's checkpoint info structure, which seems to only
    // be required for implementing SQLITE_ENABLE_SNAPSHOT functionality.
    uint32_t reserved1;
    uint32_t reserved2;
};
static_assert(std::is_pod_v<CkptInfo>);

constexpr size_t kIndexHdrSize = sizeof(HashIndexHdr) * 2 + sizeof(CkptInfo);

// Header is stored at the start of the first index group. std::memcpy() is used
// on the struct, so it needs to be a POD (or at least trivially copiable). Its
// size should be a multiple of 4 to prevent misaligned accesses.
static_assert(std::is_pod_v<HashIndexHdr>);
static_assert((kIndexHdrSize & 0b11) == 0);

constexpr uint32_t kNIndexHashes = 8'192;
constexpr uint32_t kNIndexKeys = 4'096;
constexpr uint32_t kNIndexKeys0 =
    kNIndexKeys - kIndexHdrSize / sizeof(uint32_t);

constexpr auto index_group_number(Value value) -> uint32_t
{
    return (value - 1 + kNIndexKeys - kNIndexKeys0) / kNIndexKeys;
}

auto index_hash(Key key) -> Hash
{
    static constexpr uint32_t kHashPrime = 383;
    return key * kHashPrime & (kNIndexHashes - 1);
}
constexpr auto next_index_hash(Hash hash) -> Hash
{
    return (hash + 1) & (kNIndexHashes - 1);
}

auto too_many_collisions() -> Status
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
    auto s = map_group(group_number, true);
    if (!s.is_ok()) {
        return s;
    }
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
        if (auto **groups = static_cast<volatile char **>(Mem::reallocate(
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
            auto *buf = Mem::allocate(File::kShmRegionSize);
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

void HashIndex::cleanup()
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
        const auto rest_size = static_cast<uintptr_t>(
            ConstStablePtr(group.hash) -
            ConstStablePtr(group.keys + max_hash));
        std::memset(StablePtr(group.keys + max_hash), 0, rest_size);
    }
}

void HashIndex::close()
{
    if (m_file) {
        m_file->shm_unmap(true);
        m_file = nullptr;
    } else {
        for (size_t i = 0; i < m_num_groups; ++i) {
            Mem::deallocate(const_cast<char *>(m_groups[i]));
        }
    }
    Mem::deallocate(m_groups);
    m_groups = nullptr;
}

// Merge 2 sorted lists.
auto merge_lists(
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

auto mergesort(
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
    Mem::deallocate(m_state);
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
    m_num_groups = index_group_number(last_value) + 1;
    const auto state_size =
        sizeof(State) +                             // Includes storage for 1 group ("groups[1]" member).
        (m_num_groups - 1) * sizeof(State::Group) + // Additional groups.
        last_value * sizeof(Hash);                  // Indices to sort.
    m_state = static_cast<State *>(Mem::allocate(state_size));
    if (m_state == nullptr) {
        return Status::no_memory();
    }
    std::memset(m_state, 0, state_size);

    // Temporary buffer for mergesort. Freed before returning from this routine. Possibly a bit
    // larger than necessary due to platform alignment requirements (see alloc.h).
    Buffer<Hash> temp;
    if (temp.resize(last_value < kNIndexKeys ? last_value : kNIndexKeys)) {
        // m_state will be freed in the destructor.
        return Status::no_memory();
    }
    std::memset(temp.data(), 0, temp.size() * sizeof(Hash));

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
        mergesort(keys, index_buf, temp.data(), group_size);
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
constexpr size_t kWalHdrSize = 32;
constexpr uint32_t kWalMagic = 1'559'861'749;
constexpr uint32_t kWalVersion = 1;

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

auto compute_checksum(const Slice &in, const uint32_t *initial, uint32_t *out)
{
    CALICODB_EXPECT_NE(out, nullptr);
    CALICODB_EXPECT_EQ(uintptr_t(in.data()) & 3, 0);
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

//  Operation        | Write | Checkpoint | Recovery | ReadN |
// ------------------|-------|------------|----------|-------|
//  Read frames      |       |            |          | 1     |
//  Write frames     | X     |            |
//  Checkpoint       |       | X          |
//  Checkpoint reset | X     | X          |
//  Restart log      | X     |
//  Recover index    | X     |
class WalImpl : public Wal
{
public:
    explicit WalImpl(const WalOptionsExtra &options, const char *filename);
    ~WalImpl() override;

    auto open(const WalOptions &options, const char *filename) -> Status override;

    auto read(uint32_t page_id, uint32_t page_size, char *&page) -> Status override;
    auto write(Pages &writer, uint32_t page_size, size_t db_size) -> Status override;
    auto checkpoint(CheckpointMode mode,
                    char *scratch,
                    uint32_t scratch_size,
                    BusyHandler *busy,
                    CheckpointInfo *info_out) -> Status override;

    void rollback(const Rollback &hook, void *object) override
    {
        CALICODB_EXPECT_TRUE(m_writer_lock);
        const auto max_frame = m_hdr.max_frame;
        // Cast away volatile qualifier.
        m_hdr = *const_cast<const HashIndexHdr *>(m_index.header());
        for (auto frame = m_hdr.max_frame + 1; frame <= max_frame; ++frame) {
            hook(object, m_index.fetch(frame));
        }
        if (max_frame != m_hdr.max_frame) {
            m_index.cleanup();
        }
    }

    auto close(char *scratch, uint32_t page_size) -> Status override
    {
        // This will not block. This connection has an exclusive lock on the database file,
        // so no other connections are active right now.
        auto s = checkpoint(kCheckpointPassive, scratch, page_size, nullptr, nullptr);
        if (s.is_ok()) {
            s = m_env->remove_file(m_wal_name);
            if (!s.is_ok()) {
                log(m_log, R"(failed to unlink WAL file "%s": %s)",
                    m_wal_name, s.message());
            }
        }
        return s;
    }

    auto start_read(bool &changed) -> Status override
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

    void finish_read() override
    {
        finish_write();
        if (m_reader_lock >= 0) {
            unlock_shared(READ_LOCK(m_reader_lock));
            m_reader_lock = -1;
        }
    }

    auto start_write() -> Status override
    {
        if (m_writer_lock) {
            return Status::ok();
        }
        CALICODB_EXPECT_GE(m_reader_lock, 0);
        CALICODB_EXPECT_EQ(m_redo_cksum, 0);

        auto s = lock_exclusive(kWriteLock, 1);
        if (!s.is_ok()) {
            return s;
        }
        m_writer_lock = true;

        // We have the writer lock, so no other connection will write the index header while
        // std::memcmp() is running here.
        if (0 != std::memcmp(&m_hdr, ConstStablePtr(m_index.header()), sizeof(HashIndexHdr))) {
            // Another connection has written since the read transaction was started. This
            // is not allowed (this connection now has an outdated snapshot, meaning the
            // local "max frame" value is no longer correct).
            unlock_exclusive(kWriteLock, 1);
            m_writer_lock = false;
            return Status::busy();
        }
        return Status::ok();
    }

    void finish_write() override
    {
        if (m_writer_lock) {
            unlock_exclusive(kWriteLock, 1);
            m_writer_lock = false;
            m_redo_cksum = 0;
        }
    }

    [[nodiscard]] auto callback() -> uint32_t override
    {
        return exchange(m_callback_arg, 0U);
    }

    [[nodiscard]] auto db_size() const -> uint32_t override
    {
        if (m_reader_lock >= 0) {
            return m_hdr.page_count;
        }
        return 0;
    }

private:
    auto lock_shared(size_t r) -> Status
    {
        if (m_lock_mode == Options::kLockExclusive) {
            return Status::ok();
        }
        return m_db->shm_lock(r, 1, kShmLock | kShmReader);
    }

    void unlock_shared(size_t r)
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

    void unlock_exclusive(size_t r, size_t n)
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
        HashIndexHdr h1;
        HashIndexHdr h2;

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
        m_page_size = m_hdr.page_size;
        return true;
    }

    NO_TSAN auto read_index_header(bool &changed) -> Status
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

    NO_TSAN void write_index_header()
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

    // Restart the WAL index header such that the next writer writes to the start
    // of the log file
    // Readers are attached to readmark 0 and reading exclusively from the database
    // file. This connection holds kWriteLock and the whole WAL has been checkpointed.
    void restart_header(uint32_t salt1)
    {
        CALICODB_EXPECT_TRUE(m_writer_lock);
        volatile auto *info = get_ckpt_info();

        ++m_ckpt_number;
        m_hdr.max_frame = 0;
        auto *salt = StablePtr(m_hdr.salt);
        put_u32(salt, get_u32(salt) + 1);
        std::memcpy(salt + sizeof(uint32_t), &salt1, sizeof(salt1));
        write_index_header();

        // These writes need to be atomic, otherwise TSan complains (see the end of
        // recover_index() for an explanation of a similar situation).
        CALICODB_DEBUG_DELAY(*m_env);
        ATOMIC_STORE(&info->backfill, 0);
        ATOMIC_STORE(info->readmark + 1, 0);
        for (size_t i = 2; i < kReaderCount; ++i) {
            ATOMIC_STORE(info->readmark + i, kReadmarkNotUsed);
        }
        CALICODB_EXPECT_EQ(info->readmark[0], 0);
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
                const auto salt1 = m_env->rand();
                s = lock_exclusive(READ_LOCK(1), kReaderCount - 1);
                if (s.is_ok()) {
                    restart_header(salt1);
                    unlock_exclusive(READ_LOCK(1), kReaderCount - 1);
                } else if (!s.is_busy()) {
                    return s;
                }
            }
            unlock_shared(READ_LOCK(0));
            m_reader_lock = -1;
            unsigned tries = 0;
            do {
                bool _;
                s = try_reader(true, tries, _);
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
            // Attempt to read the index header. This will fail if another connection is
            // writing the header concurrently.
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
        auto max_frame = m_hdr.max_frame;

        // Attempt to find a readmark that this reader can use to read the most-recently-committed WAL
        // frames.
        for (size_t i = 1; i < kReaderCount; i++) {
            const auto mark = ATOMIC_LOAD(info->readmark + i);
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
                    ATOMIC_STORE(info->readmark + i, max_frame);
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

    [[nodiscard]] static auto frame_offset(uint32_t frame, uint32_t page_size) -> uint64_t
    {
        CALICODB_EXPECT_GT(frame, 0);
        CALICODB_EXPECT_GT(page_size, 0);
        return kWalHdrSize + (frame - 1) * static_cast<uint64_t>(WalFrameHdr::kSize + page_size);
    }

    auto transfer_contents(CheckpointMode mode, char *scratch, BusyHandler *busy) -> Status;
    auto rewrite_checksums(uint32_t end) -> Status;
    auto recover_index() -> Status;
    [[nodiscard]] auto decode_frame(const char *frame, WalFrameHdr &out) -> int;
    void encode_frame(const WalFrameHdr &hdr, const char *page, char *out);
    auto write_frame(const WalFrameHdr &hdr, const char *page, size_t offset) -> Status;

    HashIndexHdr m_hdr = {};
    HashIndex m_index;

    const char *const m_wal_name;
    const Options::SyncMode m_sync_mode;
    const Options::LockMode m_lock_mode;

    uint32_t m_redo_cksum = 0;
    uint32_t m_ckpt_number = 0;

    UserPtr<File> m_wal;

    Env *const m_env;
    File *const m_db;
    Logger *const m_log;
    Stats *const m_stat;

    uint32_t m_min_frame = 0;

    // Size of a database page in bytes. The value is not known until it is provided in
    // write(), read from the index header, or read from the WAL header. A copy is kept in
    // the index header for performance reasons. This way we don't have to read the WAL
    // file header each time a transaction is started.
    uint32_t m_page_size = 0;

    uint32_t m_callback_arg = 0;

    int m_reader_lock = -1;
    bool m_writer_lock = false;
    bool m_ckpt_lock = false;
    bool m_lock_error = false;
};

auto WalImpl::open(const WalOptions &, const char *) -> Status
{
    CALICODB_EXPECT_FALSE(m_wal);
    UserPtr<File> file;
    auto s = m_env->new_file(m_wal_name, Env::kCreate, file.ref());
    if (s.is_ok()) {
        m_wal = move(file);
    }
    return s;
}

WalImpl::WalImpl(const WalOptionsExtra &options, const char *filename)
    : m_index(m_hdr, options.lock_mode == Options::kLockNormal ? options.db : nullptr),
      m_wal_name(filename),
      m_sync_mode(options.sync_mode),
      m_lock_mode(options.lock_mode),
      m_env(options.env),
      m_db(options.db),
      m_log(options.info_log),
      m_stat(options.stat)
{
}

WalImpl::~WalImpl()
{
    CALICODB_EXPECT_FALSE(m_writer_lock);
    CALICODB_EXPECT_FALSE(m_ckpt_lock);
    CALICODB_EXPECT_EQ(m_reader_lock, -1);
    m_index.close();
}

auto WalImpl::rewrite_checksums(uint32_t end) -> Status
{
    CALICODB_EXPECT_GT(m_redo_cksum, 0);
    const auto frame_size = WalFrameHdr::kSize + m_page_size;

    Buffer<char> frame;
    if (frame.resize(frame_size)) {
        return Status::no_memory();
    }

    // Find the offset of the previous checksum in the WAL file. If we are starting at
    // the first frame, get the previous checksum from the WAL header.
    size_t cksum_offset = 24;
    if (m_redo_cksum > 1) {
        cksum_offset = frame_offset(m_redo_cksum - 1, m_page_size) + 16;
    }

    char cksum_buffer[2 * sizeof(uint32_t)];
    auto s = m_wal->read_exact(
        cksum_offset,
        sizeof(cksum_buffer),
        cksum_buffer);
    if (!s.is_ok()) {
        return s;
    }
    m_stat->read_wal += frame_size;

    m_hdr.frame_cksum[0] = get_u32(frame.data());
    m_hdr.frame_cksum[1] = get_u32(frame.data() + sizeof(uint32_t));

    auto redo = m_redo_cksum;
    m_redo_cksum = 0;

    for (; redo < end; ++redo) {
        const auto offset = frame_offset(redo, m_page_size);
        s = m_wal->read_exact(offset, frame_size, frame.data());
        if (!s.is_ok()) {
            break;
        }
        m_stat->read_wal += frame_size;

        WalFrameHdr hdr;
        hdr.pgno = get_u32(frame.data());
        hdr.db_size = get_u32(frame.data() + sizeof(uint32_t));

        char buffer[WalFrameHdr::kSize];
        encode_frame(hdr, frame.data() + WalFrameHdr::kSize, buffer);
        s = m_wal->write(offset, Slice(buffer, sizeof(buffer)));
        if (!s.is_ok()) {
            break;
        }
        m_stat->write_wal += sizeof(buffer);
    }
    return s;
}

auto WalImpl::recover_index() -> Status
{
    CALICODB_EXPECT_EQ(kNotWriteLock, kWriteLock + 1);
    CALICODB_EXPECT_EQ(kCheckpointLock, kNotWriteLock);
    CALICODB_EXPECT_TRUE(m_writer_lock);

    // Lock the recover "Rcvr" lock. Lock the checkpoint ("Ckpt") lock as well, if this
    // code isn't being called from the checkpoint routine. In that case, the checkpoint
    // lock is already held.
    const auto lock = kNotWriteLock + m_ckpt_lock;
    auto s = lock_exclusive(lock, READ_LOCK(0) - lock);
    if (!s.is_ok()) {
        return s;
    }

    uint32_t frame_cksum[2] = {};
    m_hdr = {};

    uint64_t file_size;
    s = m_wal->get_size(file_size);
    if (!s.is_ok()) {
        return s;
    }

    if (file_size > kWalHdrSize) {
        char header[kWalHdrSize];
        s = m_wal->read_exact(0, sizeof(header), header);
        if (!s.is_ok()) {
            goto cleanup;
        }
        m_stat->read_wal += sizeof(header);

        const auto magic = get_u32(&header[0]);
        if (magic != kWalMagic) {
            s = Status::corruption("WAL header is corrupted");
            goto cleanup;
        }
        m_page_size = get_u32(&header[8]);
        m_ckpt_number = get_u32(&header[12]);
        std::memcpy(m_hdr.salt, &header[16], sizeof(m_hdr.salt));

        Buffer<char> frame;
        if (frame.resize(WalFrameHdr::kSize + m_page_size)) {
            s = Status::no_memory();
            goto cleanup;
        }
        compute_checksum(Slice(header, kWalHdrSize - 8),
                         nullptr, m_hdr.frame_cksum);
        if (m_hdr.frame_cksum[0] == get_u32(&header[24]) &&
            m_hdr.frame_cksum[1] == get_u32(&header[28])) {
            // Checksums match, meaning there is valid WAL data. Reconstruct the hash index from it
            // if the version number is understood.
            const auto version = get_u32(&header[4]);
            if (version != kWalVersion) {
                s = StatusBuilder::invalid_argument("found WAL version %u but expected %u",
                                                    version, kWalVersion);
                goto cleanup;
            }

            const auto frame_size = WalFrameHdr::kSize + m_page_size;
            const auto last_frame = static_cast<uint32_t>((file_size - kWalHdrSize) / frame_size);
            for (uint32_t n_group = 0; n_group <= index_group_number(last_frame); ++n_group) {
                const auto last = minval(last_frame, kNIndexKeys0 + n_group * kNIndexKeys);
                const auto first = 1 + (n_group == 0 ? 0 : kNIndexKeys0 + (n_group - 1) * kNIndexKeys);
                for (auto n_frame = first; n_frame <= last; ++n_frame) {
                    const auto offset = frame_offset(n_frame, m_page_size);
                    s = m_wal->read_exact(offset, frame_size, frame.data());
                    if (!s.is_ok()) {
                        goto cleanup;
                    }
                    m_stat->read_wal += frame_size;

                    WalFrameHdr hdr;
                    // Stop at the first invalid frame. This is not an error, it just indicates that there
                    // are no more valid frames in this WAL. The WAL implementation may start to overwrite
                    // frames from the start of the file, once the file has reached some threshold size.
                    // Obsolete frames will fail decoding.
                    if (decode_frame(frame.data(), hdr)) {
                        goto cleanup;
                    }
                    s = m_index.assign(hdr.pgno, n_frame);
                    if (!s.is_ok()) {
                        goto cleanup;
                    }
                    if (hdr.db_size) {
                        // Found a commit frame.
                        m_hdr.max_frame = n_frame;
                        m_hdr.page_count = hdr.db_size;
                        // Page size was read from the WAL header above.
                        m_hdr.page_size = static_cast<uint16_t>(m_page_size);
                        frame_cksum[0] = m_hdr.cksum[0];
                        frame_cksum[1] = m_hdr.cksum[1];
                    }
                }
            }
        }
    }

cleanup:
    if (s.is_ok()) {
        volatile auto *info = get_ckpt_info();

        m_hdr.page_size = static_cast<uint16_t>(m_page_size);
        m_hdr.frame_cksum[0] = frame_cksum[0];
        m_hdr.frame_cksum[1] = frame_cksum[1];
        // Make the recovered frames visible to other connections.
        write_index_header();

        // NOTE: This code might run while a reader is trying to connect (see try_reader()).
        //       Added calls to ATOMIC_*() to make TSan happy.
        CALICODB_DEBUG_DELAY(*m_env);
        ATOMIC_STORE(&info->backfill, 0);
        ATOMIC_STORE(&info->readmark[0], 0);
        for (size_t i = 1; i < kReaderCount; ++i) {
            s = lock_exclusive(READ_LOCK(i), 1);
            if (s.is_ok()) {
                const auto readmark = i == 1 && m_hdr.max_frame
                                          ? m_hdr.max_frame
                                          : kReadmarkNotUsed;
                ATOMIC_STORE(&info->readmark[i], readmark);
                unlock_exclusive(READ_LOCK(i), 1);
            } else if (!s.is_busy()) {
                break;
            }
        }
        log(m_log, "recovered %u WAL frames", m_hdr.max_frame);
    }
    unlock_exclusive(lock, READ_LOCK(0) - lock);
    return s;
}

auto WalImpl::write_frame(const WalFrameHdr &hdr, const char *page, size_t offset) -> Status
{
    char frame[WalFrameHdr::kSize];
    encode_frame(hdr, page, frame);
    auto s = m_wal->write(offset, Slice(frame, sizeof(frame)));
    if (s.is_ok()) {
        s = m_wal->write(offset + sizeof(frame), Slice(page, m_page_size));
    }
    return s;
}

void WalImpl::encode_frame(const WalFrameHdr &hdr, const char *page, char *out)
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
}

auto WalImpl::decode_frame(const char *frame, WalFrameHdr &out) -> int
{
    static constexpr size_t kDataFields = sizeof(uint32_t) * 2;
    if (0 != std::memcmp(m_hdr.salt, &frame[kDataFields], 8)) {
        return -1;
    }
    const auto pgno = get_u32(&frame[0]);
    if (pgno == 0) {
        return -1;
    }
    auto *cksum = m_hdr.frame_cksum;
    compute_checksum(Slice(frame, kDataFields), cksum, cksum);
    compute_checksum(Slice(frame + WalFrameHdr::kSize, m_page_size), cksum, cksum);
    if (cksum[0] != get_u32(&frame[16]) ||
        cksum[1] != get_u32(&frame[20])) {
        return -1;
    }
    out.pgno = pgno;
    out.db_size = get_u32(&frame[4]);
    return 0;
}

auto WalImpl::read(uint32_t page_id, uint32_t page_size, char *&page) -> Status
{
    CALICODB_EXPECT_GE(m_reader_lock, 0);
    auto *ptr = exchange(page, nullptr);
    if (m_reader_lock && m_hdr.max_frame) {
        uint32_t frame;
        auto s = m_index.lookup(page_id, m_min_frame, frame);
        if (!s.is_ok() || frame == 0) {
            // Either there was a low-level I/O error, or the page is not in the WAL.
            return s;
        }
        s = m_wal->read_exact(
            frame_offset(frame, page_size) + WalFrameHdr::kSize,
            minval(page_size, m_page_size),
            ptr);
        if (s.is_ok()) {
            m_stat->read_wal += m_page_size;
            page = ptr;
        }
        return s;
    }
    return Status::ok();
}

auto WalImpl::write(Pages &writer, uint32_t page_size, size_t db_size) -> Status
{
    CALICODB_EXPECT_TRUE(m_writer_lock);
    CALICODB_EXPECT_NE(writer.value(), nullptr);

    const auto frame_size = WalFrameHdr::kSize + page_size;
    const auto is_commit = db_size > 0;
    volatile auto *live = m_index.header();
    uint32_t first_frame = 0;

    // Check if the WAL's copy of the index header differs from what is on the first index page.
    // If it is different, then the WAL has been written since the last commit, with the first
    // record located at frame number "first_frame".
    if (0 != std::memcmp(&m_hdr, ConstStablePtr(live), sizeof(m_hdr))) {
        first_frame = live->max_frame + 1;
    }
    auto s = restart_log();
    if (!s.is_ok()) {
        return s;
    }

    if (m_hdr.max_frame == 0) {
        // This is the first frame written to the WAL. Write the WAL header.
        char header[kWalHdrSize];
        uint32_t cksum[2];

        put_u32(&header[0], kWalMagic);
        put_u32(&header[4], kWalVersion);
        put_u32(&header[8], page_size);
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
        m_page_size = page_size;

        s = m_wal->write(0, Slice(header, sizeof(header)));
        if (s.is_ok()) {
            m_stat->write_wal += sizeof(header);

            if (m_sync_mode != Options::kSyncOff) {
                ++m_stat->sync_wal;
                s = m_wal->sync();
            }
        }
    }
    if (!s.is_ok()) {
        // I/O error while writing the WAL header.
        return s;
    }
    CALICODB_EXPECT_EQ(m_page_size, page_size);

    // Write each dirty page to the WAL.
    auto next_frame = m_hdr.max_frame + 1;
    auto offset = frame_offset(next_frame, m_page_size);
    while (writer.value()) {
        auto ref = *writer.value();
        // After this call, writer.value() will contain the next page reference that
        // needs to be written, or nullptr if ref is the last page.
        writer.next();

        // Condition ensures that if this set of pages completes a transaction, then
        // the last frame will always be appended, even if another copy of the page
        // exists in the WAL for this transaction. This frame needs to have its
        // "db_size" field set to mark that it is a commit frame.
        if (first_frame && (writer.value() || !is_commit)) {
            uint32_t frame;
            // Check to see if the page has been written to the WAL already by the
            // current transaction. If so, overwrite it and indicate that checksums
            // need to be recomputed from here on commit. If not, fall through and
            // append a new frame containing the page.
            s = m_index.lookup(ref.page_id, first_frame, frame);
            if (s.is_ok() && frame) {
                if (m_redo_cksum == 0 || frame < m_redo_cksum) {
                    m_redo_cksum = frame;
                }
                s = m_wal->write(frame_offset(frame, m_page_size) + WalFrameHdr::kSize,
                                 Slice(ref.data, m_page_size));
                if (s.is_ok()) {
                    // Overwrite was successful, skip the code that appends a new frame.
                    m_stat->write_wal += m_page_size;
                    continue;
                }
            }
            if (!s.is_ok()) {
                // I/O error during lookup or overwrite.
                return s;
            }
        }
        // Page has not been written during the current transaction. Create a new
        // WAL frame for it. Note that we don't clear the dirty flag on this path.
        // It will be cleared below when the page ID-to-frame number mapping is
        // created for the new frame.
        WalFrameHdr header;
        header.pgno = ref.page_id;
        header.db_size = writer.value() ? 0 : static_cast<uint32_t>(db_size);

        CALICODB_EXPECT_EQ(offset, frame_offset(next_frame, m_page_size));
        s = write_frame(header, ref.data, offset);

        m_stat->write_wal += frame_size;
        *ref.flag |= PageRef::kAppend;
        offset += frame_size;
        ++next_frame;
    }

    if (is_commit && m_redo_cksum) {
        s = rewrite_checksums(next_frame);
        if (!s.is_ok()) {
            return s;
        }
    }
    if (is_commit && m_sync_mode == Options::kSyncFull) {
        ++m_stat->sync_wal;
        s = m_wal->sync();
    }

    next_frame = m_hdr.max_frame;
    for (writer.reset(); s.is_ok() && writer.value(); writer.next()) {
        auto ref = *writer.value();
        if (*ref.flag & PageRef::kAppend) {
            ++next_frame;
            s = m_index.assign(ref.page_id, next_frame);
            if (s.is_ok()) {
                *ref.flag &= ~static_cast<uint16_t>(PageRef::kAppend);
            }
        }
    }
    if (s.is_ok()) {
        m_hdr.page_size = static_cast<uint16_t>(m_page_size);
        m_hdr.max_frame = next_frame;
        if (is_commit) {
            m_hdr.page_count = static_cast<uint32_t>(db_size);
            ++m_hdr.change;
            write_index_header();
            m_callback_arg = next_frame;
        }
    }
    return s;
}

auto WalImpl::checkpoint(CheckpointMode mode,
                         char *scratch,
                         uint32_t scratch_size,
                         BusyHandler *busy,
                         CheckpointInfo *info_out) -> Status
{
    CALICODB_EXPECT_FALSE(m_ckpt_lock);
    CALICODB_EXPECT_FALSE(m_writer_lock);
    const auto mode0 = mode;

    // Exclude other connections from running a checkpoint. If the `reset` flag is set,
    // also exclude writers.
    auto s = lock_exclusive(kCheckpointLock, 1);
    if (s.is_ok()) {
        m_ckpt_lock = true;
        if (mode != kCheckpointPassive) {
            s = busy_wait(busy, [this] {
                return lock_exclusive(kWriteLock, 1);
            });
            if (s.is_ok()) {
                m_writer_lock = true;
            } else if (s.is_busy()) {
                mode = kCheckpointPassive;
                busy = nullptr;
                s = Status::ok();
            }
        }
    }

    auto changed = false;
    if (s.is_ok()) {
        s = read_index_header(changed);
    }
    if (s.is_ok()) {
        if (m_hdr.max_frame && m_page_size != scratch_size) {
            s = Status::corruption();
        } else {
            s = transfer_contents(mode, scratch, busy);
        }
    }
    if (info_out && (s.is_ok() || s.is_busy())) {
        info_out->backfill = get_ckpt_info()->backfill;
        info_out->wal_size = m_hdr.max_frame;
    }
    if (changed) {
        m_hdr = {};
    }
    finish_write();
    if (m_ckpt_lock) {
        unlock_exclusive(kCheckpointLock, 1);
        m_ckpt_lock = false;
    }
    return s.is_ok() && mode != mode0 ? Status::busy() : s;
}

// Write as much of the WAL back to the database file as possible
// This method is run under an exclusive checkpoint lock, and possibly an exclusive writer
// lock. Writes to the "backfill count" variable stored in the checkpoint header must be
// atomic here, but reads need not be. This is the only connection allowed to change the
// backfill count right now. Note that the backfill count is also set during both WAL restart
// and index recovery, however, connections performing either of these actions are excluded
// by shm locks. Checkpointers are serialized using the checkpoint lock, and connections
// seeking to restart the log are excluded by the writer lock (but only if this checkpoint
// is not a kCheckpointPassive), or reader lock 0.
auto WalImpl::transfer_contents(CheckpointMode mode, char *scratch, BusyHandler *busy) -> Status
{
    CALICODB_EXPECT_TRUE(m_ckpt_lock);
    CALICODB_EXPECT_TRUE(mode == kCheckpointPassive || m_writer_lock);
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
            const auto y = ATOMIC_LOAD(info->readmark + i);
            if (y < max_safe_frame) {
                CALICODB_EXPECT_LE(y, m_hdr.max_frame);
                s = busy_wait(busy, [this, i] {
                    return lock_exclusive(READ_LOCK(i), 1);
                });
                if (s.is_ok()) {
                    const uint32_t mark = i == 1 ? max_safe_frame : kReadmarkNotUsed;
                    ATOMIC_STORE(info->readmark + i, mark);
                    unlock_exclusive(READ_LOCK(i), 1);
                } else if (s.is_busy()) {
                    max_safe_frame = y;
                } else {
                    return s;
                }
            }
        }

        if (info->backfill < max_safe_frame) {
            HashIterator itr(m_index);
            s = itr.init(info->backfill);
            if (s.is_ok()) {
                s = busy_wait(busy, [this] {
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
            if (sync_on_ckpt) {
                ++m_stat->sync_wal;
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
                m_stat->read_wal += m_page_size;
                s = m_wal->read_exact(frame_offset(entry.value, m_page_size) + WalFrameHdr::kSize,
                                      m_page_size, scratch);
                if (s.is_ok()) {
                    m_stat->write_db += m_page_size;
                    s = m_db->write(static_cast<uint64_t>(entry.key - 1) * m_page_size,
                                    Slice(scratch, m_page_size));
                }
            }
            if (s.is_ok()) {
                if (max_safe_frame == m_hdr.max_frame) {
                    s = m_db->resize(m_hdr.page_count * static_cast<uint64_t>(m_page_size));
                    if (s.is_ok() && sync_on_ckpt) {
                        ++m_stat->sync_db;
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
    if (s.is_ok() && mode != kCheckpointPassive) {
        CALICODB_EXPECT_TRUE(m_writer_lock);
        if (info->backfill < m_hdr.max_frame) {
            // Some other connection got in the way.
            s = Status::busy();
        } else if (mode >= kCheckpointRestart) {
            const auto salt1 = m_env->rand();
            // Wait on other connections that are still reading from the WAL. This is
            // what SQLite does for `SQLITE_CHECKPOINT_RESTART`. New connections will
            // take readmark 0 and read directly from the database file, and the next
            // writer will reset the log.
            s = busy_wait(busy, [this] {
                return lock_exclusive(READ_LOCK(1), kReaderCount - 1);
            });
            if (s.is_ok()) {
                restart_header(salt1);
                unlock_exclusive(READ_LOCK(1), kReaderCount - 1);
            }
        }
    }
    log(m_log, "checkpointed WAL frames [%u, %u] out of %u",
        start_frame, ATOMIC_LOAD(&info->backfill), m_hdr.max_frame);
    return s;
}

} // namespace

auto new_default_wal(const WalOptionsExtra &options, const char *filename) -> Wal *
{
    return Mem::new_object<WalImpl>(options, filename);
}

Wal::Pages::Pages() = default;

Wal::Pages::~Pages() = default;

Wal::Wal() = default;

Wal::~Wal() = default;

WalPagesImpl::WalPagesImpl(PageRef &first)
    : m_first(&first),
      m_itr(m_first)
{
}

WalPagesImpl::~WalPagesImpl() = default;

auto WalPagesImpl::value() const -> Data *
{
    if (m_itr) {
        m_data = {
            m_itr->data,
            reinterpret_cast<uint16_t *>(&m_itr->flag),
            m_itr->page_id.value,
        };
        return &m_data;
    }
    return nullptr;
}

void WalPagesImpl::next()
{
    if (m_itr) {
        // Move to the next dirty page in the transient dirty list.
        auto *next = m_itr->dirty_hdr.dirty;
        m_itr = next ? next->get_page_ref() : nullptr;
    }
}

void WalPagesImpl::reset()
{
    m_itr = m_first;
}

} // namespace calicodb