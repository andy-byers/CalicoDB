// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "hash_index.h"
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

static constexpr std::size_t kNIndexValues = 8192;
static constexpr std::size_t kNIndexKeys = 4096;
static constexpr std::size_t kHashPrime = 383;
static constexpr auto kNIndexKeys0 =
    kNIndexKeys - sizeof(HashIndexHeader) / sizeof(Key);
static constexpr auto kIndexGroupSize =
    kNIndexKeys * sizeof(Key) +
    kNIndexValues * sizeof(Value);

static constexpr auto index_group_number(Value value) -> std::size_t
{
    return (value - 1 + kNIndexKeys - kNIndexKeys0) / kNIndexKeys;
}

static auto index_hash(Key key) -> Hash
{
    return key * kHashPrime & (kNIndexValues - 1);
}

static constexpr auto next_index_hash(Hash hash) -> Hash
{
    return (hash + 1) & (kNIndexValues - 1);
}

static auto too_many_collisions(Key key) -> Status
{
    std::string message;
    write_to_string(message, "too many WAL index collisions for page %u", key);
    return Status::corruption(message);
}

struct HashGroup {
    explicit HashGroup(std::size_t group_number, char *data)
    {
        keys = reinterpret_cast<Key *>(data);
        hash = reinterpret_cast<Hash *>(keys + kNIndexKeys);
        if (group_number) {
            base = static_cast<U32>(kNIndexKeys0 + (kNIndexKeys * (group_number - 1)));
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
    const auto min_group_number = index_group_number(lower);

    for (auto n = index_group_number(m_hdr->max_frame);; --n) {
        if (n >= m_groups.size()) {
            continue;
        }
        HashGroup group(n, group_data(n));
        // The guard above prevents considering groups that haven't been allocated yet.
        // Such groups would start past the current "max_frame".
        CALICODB_EXPECT_LE(group.base, m_hdr->max_frame);
        auto collisions = kNIndexValues;
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
            kNIndexValues * sizeof *group.hash;
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
        m_groups[group_number] = new char[kIndexGroupSize]();
    }
    return m_groups[group_number];
}

auto HashIndex::cleanup() -> void
{
    if (m_hdr->max_frame != 0) {
        const auto n = index_group_number(m_hdr->max_frame);
        HashGroup group(n, group_data(n));
        for (std::size_t i = 0; i < kNIndexValues; ++i) {
            if (group.hash[i] > m_hdr->max_frame) {
                group.hash[i] = 0;
            }
        }
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
        operator new (state_size, std::align_val_t {alignof(State)}));
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
    operator delete (m_state, std::align_val_t {alignof(State)});
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

} // namespace calicodb