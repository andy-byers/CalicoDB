// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_DEBUG_H
#define CALICODB_UTILS_DEBUG_H

#include "common.h"
#include "config_internal.h"
#include "logging.h"
#include "pager.h"
#include "pointer_map.h"
#include <iostream>

namespace calicodb
{

auto print_database_overview(std::ostream &os, Pager &pager) -> void
{
#define SEP "|-----------|-----------|----------------|---------------------------------|\n"

    if (pager.page_count() == 0) {
        os << "DB is empty\n";
        return;
    }
    auto &root = pager.get_root();
    auto s = FileHdr::check_db_support(root.data);
    if (s.is_ok()) {
        os << "File Header:\n";
        os << "    | Field           | Value\n";
        os << "    |-----------------|-------\n";
        os << "    | page_count      | " << FileHdr::get_page_count(root.data) << '\n';
        os << "    | freelist_head   | " << FileHdr::get_freelist_head(root.data).value << '\n';
        os << "    | freelist_length | " << FileHdr::get_freelist_length(root.data) << '\n';
        os << "    | largest_root    | " << FileHdr::get_largest_root(root.data).value << '\n';
        os << "    | page_size       | " << FileHdr::get_page_size(root.data) << '\n';
        os << "Root Header:\n";
        os << "    | Field           | Value\n";
        os << "    |-----------------|-------\n";
        os << "    | type       | " << NodeHdr::get_type(root.data + FileHdr::kSize) << '\n';
        os << "    | cell_count | " << NodeHdr::get_cell_count(root.data + FileHdr::kSize) << '\n';
        os << "    | cell_start | " << NodeHdr::get_cell_start(root.data + FileHdr::kSize) << '\n';
        os << "    | free_start | " << NodeHdr::get_free_start(root.data + FileHdr::kSize) << '\n';
        os << "    | frag_count | " << NodeHdr::get_frag_count(root.data + FileHdr::kSize) << '\n';
        os << "    | next_id    | " << NodeHdr::get_next_id(root.data + FileHdr::kSize).value << '\n';
    }
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        if (page_id.as_index() % 32 == 0) {
            os << SEP "|    PageID |  ParentID | PageType       | Info                            |\n" SEP;
        }
        Id parent_id;
        String info, type;
        if (PointerMap::is_map(page_id, pager.page_size())) {
            const auto first = page_id.value + 1;
            (void)append_format_string(info, "Range=[%u,%u]", first, first + pager.page_size() / 5 - 1);
            (void)append_strings(type, "<PtrMap>");
        } else {
            PointerMap::Entry entry;
            if (page_id.is_root()) {
                entry.type = PointerMap::kTreeRoot;
            } else {
                s = PointerMap::read_entry(pager, page_id, entry);
                if (!s.is_ok()) {
                    os << "error: " << s.message() << '\n';
                    return;
                }
                parent_id = entry.back_ptr;
            }
            PageRef *page;
            s = pager.acquire(page_id, page);
            if (!s.is_ok()) {
                os << "error: " << s.message() << '\n';
                return;
            }

            switch (entry.type) {
                case PointerMap::kTreeRoot:
                    (void)append_strings(type, "TreeRoot");
                    [[fallthrough]];
                case PointerMap::kTreeNode: {
                    auto n = NodeHdr::get_cell_count(
                        page->data + page_id.is_root() * FileHdr::kSize);
                    if (NodeHdr::get_type(page->data) == NodeHdr::kExternal) {
                        (void)append_format_string(info, "Ex,N=%u", n);
                    } else {
                        (void)append_strings(type, "In,N=", std::to_string(n).c_str());
                        ++n;
                    }
                    if (type.is_empty()) {
                        (void)append_strings(type, "TreeNode");
                    }
                    break;
                }
                case PointerMap::kFreelistPage:
                    (void)append_strings(type, "Freelist");
                    break;
                case PointerMap::kOverflowHead:
                    (void)append_format_string(info, "Next=%u", get_u32(page->data));
                    (void)append_strings(type, "OvflHead");
                    break;
                case PointerMap::kOverflowLink:
                    (void)append_format_string(info, "Next=%u", get_u32(page->data));
                    (void)append_strings(type, "OvflLink");
                    break;
                default:
                    (void)append_strings(type, "<BadType>");
            }
            pager.release(page);
        }
        String line;
        (void)append_format_string(
            line,
            "|%10u |%10u | %-15s| %-32s|\n",
            page_id.value,
            parent_id.value,
            type.c_str(),
            info.c_str());
        os << line.c_str();
    }
    os << SEP;
#undef SEP
}

namespace
{

constexpr auto kMaxLimit = SIZE_MAX - kMaxAllocation;
using DebugHeader = uint64_t;

struct {
    DebugAllocator::Hook hook = nullptr;
    void *hook_arg = nullptr;
    size_t limit = kMaxLimit;
    size_t bytes_used = 0;
} s_debug;

} // namespace

#define ALLOCATION_HOOK                                       \
    do {                                                      \
        if (s_debug.hook && s_debug.hook(s_debug.hook_arg)) { \
            return nullptr;                                   \
        }                                                     \
    } while (0)

auto debug_malloc(size_t size) -> void *
{
    CALICODB_EXPECT_NE(size, 0);
    const auto alloc_size = sizeof(DebugHeader) + size;
    if (s_debug.bytes_used + alloc_size > s_debug.limit) {
        return nullptr;
    }

    ALLOCATION_HOOK;

    auto *ptr = static_cast<DebugHeader *>(
        CALICODB_DEFAULT_MALLOC(alloc_size));
    if (ptr) {
        s_debug.bytes_used += alloc_size;
        *ptr++ = alloc_size;
    }
    return ptr;
}

auto debug_free(void *ptr) -> void
{
    CALICODB_EXPECT_NE(ptr, nullptr);
    const auto alloc_size = DebugAllocator::size_of(ptr);
    CALICODB_EXPECT_GT(alloc_size, sizeof(DebugHeader));
    CALICODB_EXPECT_LE(alloc_size, s_debug.bytes_used);

    // Fill the memory region with junk data. SQLite uses random bytes, which is probably more ideal,
    // but this should be good enough for now. This is intended to cause use-after-free bugs to be more
    // likely to result in crashes, rather than data corruption.
    std::memset(ptr, 0xFF, alloc_size - sizeof(DebugHeader));
    CALICODB_DEFAULT_FREE(static_cast<DebugHeader *>(ptr) - 1);
    s_debug.bytes_used -= alloc_size;
}

auto debug_realloc(void *old_ptr, size_t new_size) -> void *
{
    CALICODB_EXPECT_NE(new_size, 0);
    CALICODB_EXPECT_NE(old_ptr, nullptr);

    const auto new_alloc_size = sizeof(DebugHeader) + new_size;
    const auto old_alloc_size = DebugAllocator::size_of(old_ptr);
    CALICODB_EXPECT_GE(old_alloc_size, sizeof(DebugHeader));
    CALICODB_EXPECT_GE(s_debug.bytes_used, old_alloc_size);
    const auto grow = new_alloc_size > old_alloc_size
                          ? new_alloc_size - old_alloc_size
                          : 0;
    if (s_debug.bytes_used + grow > s_debug.limit) {
        return nullptr;
    }

    ALLOCATION_HOOK;

    // Call malloc() to get a new address. realloc() might resize the allocation inplace, but it
    // is undefined behavior to access the memory through the old pointer. If any code is doing
    // that, this makes it more likely to crash early rather than continue and  produce unexpected
    // results.
    auto *new_ptr = static_cast<DebugHeader *>(
        CALICODB_DEFAULT_MALLOC(new_alloc_size));
    if (new_ptr) {
        *new_ptr++ = new_alloc_size;

        // Copy the data over to the new allocation. Free the old allocation.
        const auto data_size = minval(old_alloc_size, new_alloc_size) - sizeof(DebugHeader);
        std::memcpy(new_ptr, old_ptr, data_size);
        debug_free(old_ptr);

        s_debug.bytes_used += new_alloc_size;
    }
    return new_ptr;
}

auto DebugAllocator::config() -> AllocatorConfig
{
    return {
        debug_malloc,
        debug_realloc,
        debug_free,
    };
}

auto DebugAllocator::set_limit(size_t limit) -> size_t
{
    limit = limit ? limit : kMaxLimit;
    if (s_debug.bytes_used <= limit) {
        return exchange(s_debug.limit, limit);
    }
    return 0;
}

auto DebugAllocator::set_hook(Hook hook, void *arg) -> void
{
    s_debug.hook = hook;
    s_debug.hook_arg = arg;
}

auto DebugAllocator::bytes_used() -> size_t
{
    return s_debug.bytes_used;
}

auto DebugAllocator::size_of(void *ptr) -> size_t
{
    return static_cast<DebugHeader *>(ptr)[-1];
}

} // namespace calicodb

#endif // CALICODB_UTILS_DEBUG_H
